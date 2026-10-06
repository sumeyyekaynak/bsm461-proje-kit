# -*- coding: utf-8 -*-
"""AKIŞ HATTI İSKELETİ — Kafka → Spark Structured Streaming → pencereli sayım → çıktı.

Çalıştırma (iki ayrı terminalde):
  1) docker compose exec araclar python araclar/uretici.py --grup <kodunuz> --sablon iot --hiz 50
  2) docker compose exec spark spark-submit is/akis_iskelet.py --sablon iot

Varsayılan hâliyle her 1 dakikalık pencerede anahtar başına olay sayısını konsola yazar.
"TODO" yerlerini doldurmanız ve kararlarınızı raporda gerekçelendirmeniz beklenir.
Bağlantılı haftalar: H3 (idempotentlik) · H10 (Kafka, ofset, teslim) · H11 (pencere, watermark, garanti)
"""
import argparse
from pyspark.sql import SparkSession, functions as F, types as T

ap = argparse.ArgumentParser()
ap.add_argument("--sablon", required=True, choices=["log", "satis", "iot", "haber"])
ap.add_argument("--cikti", default="konsol", choices=["konsol", "s3"],
                help="konsol: ekrana yaz · s3: Parquet olarak s3a://islenmis/akis/<sablon>/")
a = ap.parse_args()

KONU = {"log": "web-loglari", "satis": "satislar", "iot": "sensorler", "haber": "haberler"}[a.sablon]
ANAHTAR = {"log": "ip", "satis": "urun_id", "iot": "sensor_id", "haber": "kaynak"}[a.sablon]

# Şablonların alanları — TODO: kullanacağınız alanları ekleyin/çıkarın
SEMA = {
    "log":   "olay_zamani STRING, ip STRING, yontem STRING, yol STRING, durum INT, bayt LONG, sure_ms DOUBLE, ajan STRING",
    "satis": "olay_zamani STRING, siparis_id STRING, magaza_id INT, urun_id INT, adet INT, birim_fiyat DOUBLE, tur STRING",
    "iot":   "olay_zamani STRING, sensor_id STRING, bolge STRING, sicaklik DOUBLE, nem DOUBLE, titresim DOUBLE",
    "haber": "olay_zamani STRING, haber_id STRING, kaynak STRING, kategori STRING, baslik STRING, metin STRING",
}[a.sablon]

spark = SparkSession.builder.appName(f"akis-{a.sablon}").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# ── 1. Kafka'dan oku ───────────────────────────────────────────────────────
# TODO (H10): startingOffsets "latest" mi "earliest" mi? İş yeniden başlarsa nereden devam eder?
ham = (spark.readStream.format("kafka")
       .option("kafka.bootstrap.servers", "kafka:29092")
       .option("subscribe", KONU)
       .option("startingOffsets", "latest")
       .load())

olaylar = (ham.select(F.from_json(F.col("value").cast("string"), SEMA).alias("o"),
                      F.col("timestamp").alias("islem_zamani"))     # Kafka'ya ulaştığı an
              .select("o.*", "islem_zamani")
              .withColumn("olay_zamani", F.to_timestamp("olay_zamani")))   # olayın gerçekte olduğu an

# ── 2. Pencere ve watermark ────────────────────────────────────────────────
# TODO (H11): watermark süresini seçin ve gerekçelendirin. Üretici olayların ~%5'ini 1–10 dakika
#             GEÇ gönderiyor. 2 dakikalık watermark bunların ne kadarını kaçırır? Ölçün.
# TODO (H11): pencere türü (sabit / kayan / oturum) ve boyutu sizin sorunuza uygun mu?
# TODO (H10–H11): üretici bazı olayları İKİ KEZ gönderiyor. Sayımınız bundan etkileniyor mu?
#                 Nasıl önlersiniz (dropDuplicates + watermark, idempotent yazma)?
pencereli = (olaylar
             .withWatermark("olay_zamani", "2 minutes")
             .groupBy(F.window("olay_zamani", "1 minute"), F.col(ANAHTAR))
             .agg(F.count("*").alias("olay_sayisi")))

# ── 3. Çıktı ───────────────────────────────────────────────────────────────
# TODO (H11): "konsol" yalnızca geliştirme içindir. Gerçek çıktınız nereye gidiyor (S3/Delta, ES,
#             servis deposu)? Teslim garantiniz ne: en az bir kez mi, tam bir kez mi? Neden?
if a.cikti == "konsol":
    sorgu = (pencereli.writeStream.outputMode("update").format("console")
             .option("truncate", False).option("numRows", 20)
             .trigger(processingTime="10 seconds").start())
else:
    sorgu = (pencereli.writeStream.outputMode("append").format("parquet")
             .option("path", f"s3a://islenmis/akis/{a.sablon}/")
             .option("checkpointLocation", f"s3a://islenmis/_checkpoint/{a.sablon}/")   # kalınan yer
             .trigger(processingTime="30 seconds").start())

sorgu.awaitTermination()
