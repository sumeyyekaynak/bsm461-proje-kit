# -*- coding: utf-8 -*-
"""S3 kovalarını oluşturur (zaten varsa dokunmaz). `docker compose up` sırasında bir kez çalışır."""
import sys, time
sys.path.insert(0, "/kit/araclar")
from ortak import s3, KOVALAR

for deneme in range(30):
    try:
        istemci = s3()
        var = {k["Name"] for k in istemci.list_buckets().get("Buckets", [])}
        for kova in KOVALAR:
            if kova not in var:
                istemci.create_bucket(Bucket=kova)
                print("kova oluşturuldu:", kova)
            else:
                print("kova zaten var:  ", kova)
        sys.exit(0)
    except Exception as hata:              # S3 henüz hazır değilse bekle
        print("S3 bekleniyor…", type(hata).__name__)
        time.sleep(2)
print("S3'e ulaşılamadı"); sys.exit(1)
