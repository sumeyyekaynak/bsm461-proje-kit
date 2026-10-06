# -*- coding: utf-8 -*-
"""TOPLU HAT İSKELETİ — ham JSON (S3) → Parquet → Delta tablosu → sorgu.

Çalıştırma (önce toplu veri üretin):
  docker compose exec araclar python araclar/uretici.py --grup <kodunuz> --sablon log --toplu 500000
  docker compose exec spark spark-submit is/toplu_is.py --sablon log

Bu dosya ÇALIŞIR hâlde gelir; ama projeniz değildir. "TODO" ile işaretli yerleri kendi şablonunuza
göre değiştirmeniz ve her kararınızı raporda gerekçelendirmeniz beklenir.
Bağlantılı haftalar: H2 (biçim) · H4 (nesne deposu) · H6–H7 (Spark, explain, çarpıklık) · H9 (tablo biçimi)
"""
import argparse, time
from pyspark.sql import SparkSession, functions as F

ap = argparse.ArgumentParser()
ap.add_argument("--sablon", required=True, choices=["log", "satis", "iot", "haber"])
a = ap.parse_args()

spark = SparkSession.builder.appName(f"toplu-{a.sablon}").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

HAM = f"s3a://ham/{a.sablon}/"
PARQUET = f"s3a://islenmis/{a.sablon}/"
TABLO = f"s3a://tablolar/{a.sablon}"

# ── 1. Ham veriyi oku ──────────────────────────────────────────────────────
t0 = time.time()
ham = spark.read.json(HAM)            # TODO (H2): şemayı elle verin — çıkarım tüm veriyi bir kez fazladan okur
ham = ham.withColumn("olay_zamani", F.to_timestamp("olay_zamani")) \
         .withColumn("tarih", F.to_date("olay_zamani"))
print(f"ham satır: {ham.count():,}   ({time.time()-t0:.1f} s)")

# ── 2. Temizle ─────────────────────────────────────────────────────────────
# TODO (Z10, H4–H7): kirli kayıtlar var (eksik alan, boş değer, tekrar eden olay). Ne yapacaksınız —
#             atmak mı, düzeltmek mi, ayrı bir tabloya almak mı? Kaç kayıt etkilendi? Raporda yazın.
temiz = ham.dropna(subset=["olay_zamani"]).dropDuplicates()

# ── 3. Parquet olarak yaz ──────────────────────────────────────────────────
# TODO (H2, H9): hangi sütuna göre bölümlüyorsunuz, neden? Kaç dosya oluştu (küçük dosya problemi, H4)?
t0 = time.time()
temiz.write.mode("overwrite").partitionBy("tarih").parquet(PARQUET)
print(f"Parquet yazıldı: {PARQUET}   ({time.time()-t0:.1f} s)")

# ── 4. Delta tablosu ───────────────────────────────────────────────────────
# TODO (H9): tabloya ikinci bir yazma (ör. düzeltme ya da yeni gün) yapın ve zaman yolculuğuyla
#            eski sürümü okuyun. Atomik commit'in sizi hangi hatadan koruduğunu raporda anlatın.
temiz.write.format("delta").mode("overwrite").partitionBy("tarih").save(TABLO)
print("Delta sürümleri:")
spark.sql(f"DESCRIBE HISTORY delta.`{TABLO}`").select("version", "timestamp", "operation").show(truncate=False)

# ── 5. Bir analiz sorgusu ve fiziksel planı ────────────────────────────────
tablo = spark.read.format("delta").load(TABLO)
ANAHTAR = {"log": "ip", "satis": "urun_id", "iot": "sensor_id", "haber": "kaynak"}[a.sablon]

# TODO (H6–H7): bu sorguyu kendi sorunuzla değiştirin. explain() çıktısını raporda satır satır
#               yorumlayın: hangi aşamalar var, shuffle nerede, hangi filtre aşağı itildi (pushdown)?
sorgu = (tablo.filter(F.col("tarih") >= F.date_sub(F.current_date(), 3))
              .groupBy(ANAHTAR).count()
              .orderBy(F.desc("count")))
sorgu.explain(mode="formatted")
t0 = time.time()
sorgu.show(10, truncate=False)
print(f"sorgu süresi: {time.time()-t0:.2f} s")

# ── 6. Çarpıklık kontrolü (H3, H7) ─────────────────────────────────────────
# En büyük anahtar toplamın yüzde kaçı? Bölümlere eşit dağılıyor mu? (en dolu ÷ ortalama)
toplam = tablo.count()
en_buyuk = sorgu.first()["count"]
print(f"en büyük anahtarın payı: %{100*en_buyuk/toplam:.1f}   (ideal dağılımda her anahtar eşit pay alırdı)")
# TODO (H7): bu anahtara göre join ya da groupBy yapan bir işte ne olur? Spark UI'da (localhost:4040)
#            görev sürelerine bakın; gerekiyorsa tuzlama uygulayıp ÖNCE/SONRA süresini ölçün.

spark.stop()
