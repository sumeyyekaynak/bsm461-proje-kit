# -*- coding: utf-8 -*-
"""Kit içindeki bütün Python betiklerinin ortak ayarları.

Betikler `araclar` kapsayıcısının içinde çalışır; adresler kapsayıcılar arası adlardır
(kafka, s3, elasticsearch). Bilgisayarınızdan bağlanacaksanız: localhost:9092, localhost:8333,
localhost:9200.
"""
import os

KAFKA = os.environ.get("KAFKA", "kafka:29092")
S3_ADRES = os.environ.get("S3_ADRES", "http://s3:8333")
S3_ANAHTAR = "bsm461"
S3_GIZLI = "bsm461-gizli"
ES_ADRES = os.environ.get("ES_ADRES", "http://elasticsearch:9200")
MONGO_ADRES = os.environ.get("MONGO_ADRES", "mongodb://mongo:27017")

KOVALAR = ["ham", "islenmis", "tablolar"]          # ham veri · Parquet · Delta tabloları

SABLONLAR = {
    "log":   {"konu": "web-loglari",  "anahtar": "ip",        "ad": "Web sunucu log analizi ve pano"},
    "satis": {"konu": "satislar",     "anahtar": "urun_id",   "ad": "Gerçek zamanlı satış/stok takibi"},
    "iot":   {"konu": "sensorler",    "anahtar": "sensor_id", "ad": "IoT sensör telemetrisi ve anomali"},
    "haber": {"konu": "haberler",     "anahtar": "kaynak",    "ad": "Metin/haber akışı analitiği"},
}


def s3():
    import boto3
    return boto3.client("s3", endpoint_url=S3_ADRES,
                        aws_access_key_id=S3_ANAHTAR, aws_secret_access_key=S3_GIZLI,
                        region_name="us-east-1")
