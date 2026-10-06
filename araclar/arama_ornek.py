# -*- coding: utf-8 -*-
"""ARAMA ÖRNEĞİ — Elasticsearch'e Türkçe analizörlü bir indeks kurar, haber üretip indeksler, arar.
Şablon 1 (log) ve 4 (haber) için başlangıç noktası.

  docker compose --profile arama up -d
  docker compose exec araclar python araclar/arama_ornek.py
  docker compose exec araclar python araclar/arama_ornek.py --sorgu "ihracatımızdan"

Bağlantılı hafta: H8 (ters indeks, analiz zinciri, Türkçe kök bulma, BM25)
"""
import argparse, random, sys
sys.path.insert(0, "/kit/araclar")
from elasticsearch import Elasticsearch, helpers
from ortak import ES_ADRES
from uretici import uretec, zaman_damgasi
from datetime import datetime, timezone

ap = argparse.ArgumentParser()
ap.add_argument("--adet", type=int, default=2000)
ap.add_argument("--sorgu", default="enflasyonlarından")
a = ap.parse_args()

es = Elasticsearch(ES_ADRES, request_timeout=30)
INDEKS = "haberler"

# TODO (H8): bu ayar ve eşleme (mapping) sizin kararınız. Türkçe kök bulma neyi kazandırıyor,
#             neyi bozuyor? "standard" analizörle karşılaştırın. Dinamik eşlemeyi neden kapattık?
AYAR = {
    "settings": {"number_of_shards": 1, "number_of_replicas": 0,
                 "analysis": {"analyzer": {"turkce": {
                     "type": "custom", "tokenizer": "standard",
                     "filter": ["apostrophe", "turkish_lowercase", "turkish_stop", "turkish_stemmer"]}},
                     "filter": {"turkish_lowercase": {"type": "lowercase", "language": "turkish"},
                                "turkish_stop": {"type": "stop", "stopwords": "_turkish_"},
                                "turkish_stemmer": {"type": "stemmer", "language": "turkish"}}}},
    "mappings": {"dynamic": "strict", "properties": {
        "olay_zamani": {"type": "date"}, "haber_id": {"type": "keyword"},
        "kaynak": {"type": "keyword"}, "kategori": {"type": "keyword"},
        "baslik": {"type": "text", "analyzer": "turkce"},
        "metin": {"type": "text", "analyzer": "turkce"}}},
}

if es.indices.exists(index=INDEKS):
    es.indices.delete(index=INDEKS)
es.indices.create(index=INDEKS, **AYAR)

olay = uretec("haber", random.Random(461))
belgeler = ({"_index": INDEKS, "_id": h["haber_id"],
             "_source": {"olay_zamani": zaman_damgasi(datetime.now(timezone.utc)), **h}}
            for h in (olay(datetime.now(timezone.utc)) for _ in range(a.adet)))
ok, _ = helpers.bulk(es, belgeler)
es.indices.refresh(index=INDEKS)
print(f"✓ {ok} haber indekslendi")

# Analizör bir kelimeyi nasıl parçalıyor?
belirtecler = es.indices.analyze(index=INDEKS, analyzer="turkce", text=a.sorgu)["tokens"]
print(f"'{a.sorgu}' → analizör çıktısı: {[b['token'] for b in belirtecler]}")

sonuc = es.search(index=INDEKS, query={"match": {"metin": a.sorgu}}, size=3)
print(f"'{a.sorgu}' için {sonuc['hits']['total']['value']} sonuç; ilk üçü (BM25 skoru):")
for h in sonuc["hits"]["hits"]:
    print(f"  {h['_score']:.2f}  {h['_source']['baslik']}")

top = es.search(index=INDEKS, size=0, aggs={"kaynaklar": {"terms": {"field": "kaynak"}}})
print("kaynak başına haber (toplama):", {b["key"]: b["doc_count"] for b in top["aggregations"]["kaynaklar"]["buckets"]})
