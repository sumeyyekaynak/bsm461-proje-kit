# -*- coding: utf-8 -*-
"""GRUP KODU — her grubun verisini ve senaryosunu kendine özgü yapar.

Grup kodu = grup üyelerinin öğrenci numaralarından EN KÜÇÜĞÜ (ör. B231210001).
Aynı kod her zaman aynı veriyi ve aynı senaryoyu verir; farklı grupların verisi ve senaryosu farklıdır.

  python araclar/senaryo.py --grup B231210001 --sablon log     # senaryo kartınız
  python araclar/uretici.py --grup B231210001 --sablon log ... # verinizi üretir

Yalnızca standart kütüphane kullanır.
"""
import random, re, zlib

ORNEK_KOD = "ORNEK"


def normalle(kod):
    kod = (kod or "").strip().upper()
    if not kod:
        return ORNEK_KOD
    if not re.fullmatch(r"[A-Z0-9]{4,20}", kod):
        raise SystemExit(f"❌ geçersiz grup kodu: {kod!r} — öğrenci numarası gibi yalnızca harf ve rakam olmalı")
    return kod


def _r(kod, ek):
    return random.Random(zlib.crc32(f"{normalle(kod)}|{ek}".encode()))


def tohum(kod):
    """Grup kodundan rastgele sayı tohumu: aynı grup, aynı veri."""
    return zlib.crc32(normalle(kod).encode())


# ─────────────────────────────────────────────────────────── veri ayarları (üretici kullanır)
def veri_ayarlari(kod, sablon):
    r = _r(kod, "veri|" + sablon)
    g = {"tuz": normalle(kod)}
    if sablon == "log":
        adaylar = ["/odeme", "/api/arama", "/sepet", "/giris", "/hesabim", "/urunler"]
        y1, y2 = r.sample(adaylar, 2)
        g.update(zipf_us=round(r.uniform(1.0, 1.35), 2),
                 yavas_yol={y1: round(r.uniform(3.0, 5.0), 1), y2: round(r.uniform(2.0, 3.0), 1)},
                 patlama_dakika=r.choice([10, 15, 20]),
                 saat_kayma=r.choice([-1, 0, 1, 2]))
    elif sablon == "satis":
        magazalar = list(range(1, 21))
        buyuk = r.sample(magazalar, 2)
        orta = r.sample([m for m in magazalar if m not in buyuk], 4)
        g.update(zipf_us=round(r.uniform(1.05, 1.3), 2),
                 magaza_agirlik={m: 3 if m in buyuk else (2 if m in orta else 1) for m in magazalar},
                 gun_agirlik=r.choice([[1.0, 1.0, 1.0, 1.0, 1.2, 1.6, 1.5],     # Pzt…Paz
                                       [1.0, 1.0, 1.0, 1.1, 1.3, 1.4, 1.8],
                                       [0.9, 1.0, 1.0, 1.0, 1.7, 1.5, 1.1]]),
                 stok_taban=r.choice([30, 40, 50]), stok_adim=r.choice([37, 41, 43, 53]))
    elif sablon == "iot":
        sus = r.choice([19, 23, 25, 29])
        g.update(ariza_bas=r.randint(1, 191),
                 sus_mod=sus, kayip_mod=r.choice([m for m in (13, 17, 31) if m != sus]),
                 kayip_oran=round(r.uniform(0.2, 0.4), 2),
                 bolge_kayma={b: round(r.uniform(-2, 2), 1) for b in
                              ["Marmara", "Ege", "Akdeniz", "İç Anadolu", "Karadeniz", "Doğu Anadolu", "Güneydoğu"]})
    elif sablon == "haber":
        uygun = {"ajans-kuzey": ["dünya", "gündem"], "ajans-guney": ["dünya", "gündem", "ekonomi"],
                 "gazete-merkez": ["gündem", "ekonomi", "kültür"], "portal-hizli": ["spor", "teknoloji", "kültür"],
                 "yerel-haber": ["gündem", "eğitim", "sağlık"], "ekonomi-gunluk": ["ekonomi"]}
        tercihli = r.sample(sorted(uygun), 3)
        g.update(zipf_us=round(r.uniform(1.1, 1.5), 2),
                 kaynak_kategori={k: r.choice(uygun[k]) for k in tercihli},
                 gundem_blok_saat=r.choice([3, 4, 6]))
    return g


# ─────────────────────────────────────────────────────────── senaryo (gerçek sistemin koşulları)
_HIZ = {"log":   [300, 1_500, 6_000, 25_000, 80_000],
        "satis": [40, 200, 900, 3_500, 12_000],
        "iot":   [200, 2_000, 15_000, 60_000, 250_000],
        "haber": [3, 20, 120, 600, 2_500]}
_IS = {"log":   ("e-ticaret sitesi", "istek"),
       "satis": ("mağaza zinciri", "satış/iade/stok olayı"),
       "iot":   ("sensör ağı", "ölçüm"),
       "haber": ("haber toplayıcı", "haber")}


def senaryo(kod, sablon):
    """Grubun gerçek sistem koşulları: kararlarınızı bunlara göre verirsiniz."""
    r = _r(kod, "senaryo|" + sablon)
    return {
        "hiz":            r.choice(_HIZ[sablon]),                      # ortalama olay/s
        "tepe_carpan":    r.choice([2, 4, 8]),                         # en yoğun saat ÷ ortalama
        "uyari_suresi_s": r.choice([5, 30, 120, 600]),                 # akış sorusunun cevabı en geç kaç s'de
        "gec_orani":      r.choice([0.01, 0.05, 0.15]),                # olayların ne kadarı geç gelir
        "gec_en_fazla_dk": r.choice([2, 10, 30]),                      # geç olay en fazla kaç dk gecikir
        "kayip_siniri":   r.choice([0.0, 0.001, 0.01]),                # sayıma girmeyebilecek olay oranı
        "saklama_gun":    r.choice([30, 365, 3 * 365]),                # ham veri ne kadar saklanır
        "servis_ms":      r.choice([50, 200, 2000]),                   # servis sorgusu p99 sınırı
    }


def senaryo_karti(kod, sablon):
    s = senaryo(kod, sablon)
    ne, birim = _IS[sablon]
    kayip = "hiçbir olay kaybolmamalı" if s["kayip_siniri"] == 0 else f"olayların en fazla %{s['kayip_siniri']*100:g}'i"
    satirlar = [
        f"SENARYO KARTI · grup {normalle(kod)} · şablon {sablon}",
        "─" * 64,
        f"Sistem             bir {ne}",
        f"Ortalama hız       saniyede {s['hiz']:,} {birim}".replace(",", "."),
        f"Tepe yük           en yoğun saatte ortalamanın {s['tepe_carpan']} katı",
        f"Uyarı süresi       akış sorusunun cevabı en geç {s['uyari_suresi_s']} saniyede çıkmalı",
        f"Geç veri           olayların %{s['gec_orani']*100:g}'i en fazla {s['gec_en_fazla_dk']} dakika geç gelir",
        f"Kayıp sınırı       sayıma girmeyen (geç kalıp atılan): {kayip}",
        f"Saklama            ham veri {s['saklama_gun']} gün saklanır",
        f"Servis sorgusu     p99 en fazla {s['servis_ms']} ms",
        "─" * 64,
        "Bu kart sizin sisteminizin GERÇEK koşullarıdır. Ölçek (Z1), Kafka bölüm sayısı ve watermark (Z8),",
        "servis deposu (Z6) kararlarınızı bu sayılara dayandırın. Kitteki veri bu sistemin bilgisayarınıza",
        "sığacak kadar küçültülmüş bir örneğidir: hızı --hiz ile siz seçersiniz; geç veri oranı ve gecikmesi",
        "bu karttaki gibidir. Koşullar birbiriyle çelişebilir — hangisinden ödün verdiğinizi gerekçelendirin.",
    ]
    return "\n".join(satirlar)
