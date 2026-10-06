# -*- coding: utf-8 -*-
"""SENARYO KARTI — grubunuzun gerçek sistem koşullarını yazdırır.

  docker compose exec araclar python araclar/senaryo.py --grup B231210001 --sablon log

Grup kodu = grup üyelerinin öğrenci numaralarından en küçüğü. Çıktıyı kanit/senaryo.txt olarak saklayın.
"""
import argparse, sys
sys.path.insert(0, "/kit/araclar")
from grup import senaryo_karti

ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
ap.add_argument("--grup", required=True, help="grup kodu: üyelerin en küçük öğrenci numarası")
ap.add_argument("--sablon", required=True, choices=["log", "satis", "iot", "haber"])
a = ap.parse_args()
print(senaryo_karti(a.grup, a.sablon))
