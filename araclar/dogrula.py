# -*- coding: utf-8 -*-
"""Kitin parçalarını tek tek dener ve sonucu ✅ / ❌ olarak yazar.
dogrula.sh bunu araclar kapsayıcısının içinde çalıştırır."""
import sys, time, uuid
sys.path.insert(0, "/kit/araclar")
from ortak import KAFKA, ES_ADRES, KOVALAR, s3

sonuc = []

def dene(ad, f, zorunlu=True):
    try:
        ayrinti = f()
        sonuc.append((True, ad, ayrinti, zorunlu))
    except Exception as hata:
        metin = str(hata) if isinstance(hata, RuntimeError) else f"{type(hata).__name__}: {hata}"
        sonuc.append((False, ad, metin[:160], zorunlu))


def kafka_gidis_donus():
    from confluent_kafka import Producer, Consumer
    from confluent_kafka.admin import AdminClient, NewTopic
    konu = "kit-dogrulama"
    yonetici = AdminClient({"bootstrap.servers": KAFKA})
    if konu not in yonetici.list_topics(timeout=10).topics:
        yonetici.create_topics([NewTopic(konu, num_partitions=1, replication_factor=1)])[konu].result(10)
    isaret = uuid.uuid4().hex
    p = Producer({"bootstrap.servers": KAFKA})
    p.produce(konu, value=isaret.encode()); p.flush(10)
    c = Consumer({"bootstrap.servers": KAFKA, "group.id": "dogrula-" + isaret,
                  "auto.offset.reset": "earliest"})
    c.subscribe([konu])
    bitis = time.time() + 20
    while time.time() < bitis:
        m = c.poll(1.0)
        if m is not None and not m.error() and m.value().decode() == isaret:
            c.close()
            return "yazılan mesaj geri okundu"
    c.close()
    raise RuntimeError("mesaj 20 saniyede geri okunamadı")


def s3_kovalar():
    istemci = s3()
    var = {k["Name"] for k in istemci.list_buckets()["Buckets"]}
    eksik = [k for k in KOVALAR if k not in var]
    if eksik:
        raise RuntimeError("eksik kova: " + ", ".join(eksik) + " (docker compose up s3-kur)")
    anahtar = "_dogrulama/" + uuid.uuid4().hex
    istemci.put_object(Bucket="ham", Key=anahtar, Body=b"bsm461")
    geri = istemci.get_object(Bucket="ham", Key=anahtar)["Body"].read()
    istemci.delete_object(Bucket="ham", Key=anahtar)
    if geri != b"bsm461":
        raise RuntimeError("yazılan nesne geri okunamadı")
    return "kovalar: " + ", ".join(KOVALAR) + " · yaz/oku çalışıyor"


def duckdb_s3():
    import duckdb
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("SET s3_endpoint='s3:8333'; SET s3_use_ssl=false; SET s3_url_style='path';"
                "SET s3_access_key_id='bsm461'; SET s3_secret_access_key='bsm461-gizli'; SET s3_region='us-east-1';")
    con.execute("COPY (SELECT 1 AS a) TO 's3://islenmis/_dogrulama/duckdb.parquet' (FORMAT parquet)")
    n = con.execute("SELECT count(*) FROM 's3://islenmis/_dogrulama/duckdb.parquet'").fetchone()[0]
    return f"DuckDB s3:// üzerinden Parquet yazdı ve okudu ({n} satır)"


def mongodb():
    from pymongo import MongoClient
    from ortak import MONGO_ADRES
    try:
        surum = MongoClient(MONGO_ADRES, serverSelectionTimeoutMS=3000).server_info()["version"]
    except Exception:
        raise RuntimeError("kapalı — açmak için: docker compose --profile servis up -d")
    return f"MongoDB {surum}"


def elasticsearch():
    from elasticsearch import Elasticsearch
    es = Elasticsearch(ES_ADRES, request_timeout=5)
    try:
        durum = es.cluster.health()["status"]
    except Exception:
        raise RuntimeError("kapalı — açmak için: docker compose --profile arama up -d")
    return f"küme durumu: {durum}"


dene("Kafka  · yaz → oku", kafka_gidis_donus)
dene("S3     · kovalar ve yaz → oku", s3_kovalar)
dene("DuckDB · s3:// üzerinden Parquet", duckdb_s3)
dene("Elasticsearch (yalnızca --profile arama ile)", elasticsearch, zorunlu=False)
dene("MongoDB (yalnızca --profile servis ile)", mongodb, zorunlu=False)

print()
for basari, ad, ayrinti, zorunlu in sonuc:
    isaret = "✅" if basari else ("❌" if zorunlu else "➖")
    print(f"  {isaret}  {ad:<46} {ayrinti}")
hata = [s for s in sonuc if not s[0] and s[3]]
print()
print("  🔴 Eksik var — README.md 'Sık karşılaşılan sorunlar' bölümüne bakın." if hata
      else "  🎉 Çekirdek kit çalışıyor.")
sys.exit(1 if hata else 0)
