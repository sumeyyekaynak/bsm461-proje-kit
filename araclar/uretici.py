# -*- coding: utf-8 -*-
"""SAHTE VERİ ÜRETİCİ — dört proje şablonu için gerçekçi olaylar üretir.

İki kip:
  Akış  → Kafka'ya sürekli olay basar          (akış hattı için)
  Toplu → S3'e geçmiş günlerin verisini yazar  (toplu hat için)

Örnekler (araclar kapsayıcısının içinde):
  python araclar/uretici.py --grup B231210001 --sablon log     # saniyede 20 olay, Ctrl+C ile dur
  python araclar/uretici.py --grup B231210001 --sablon iot --hiz 200 --sure 60   # 60 saniye, saniyede 200 olay
  python araclar/uretici.py --grup B231210001 --sablon satis --toplu 2000000   # S3'e 2 milyon geçmiş olay
  python araclar/uretici.py --grup B231210001 --sablon haber --gec-orani 0.2     # olayların %20'si geç gelsin

Grup kodu (üyelerin en küçük öğrenci numarası) verinizi ve senaryonuzu belirler — bkz. grup.py, senaryo.py.

Bilerek konan "gerçek hayat kusurları" (raporunuzda ele almanız beklenir):
  • Çarpık anahtar: birkaç anahtar olayların çoğunu üretir (H3 sıcak nokta, H7 çarpıklık)
  • Geç olay:      bazı olayların olay_zamani, gönderildiği andan dakikalar öncedir (H11 watermark)
  • Kirli kayıt:   az sayıda olayda alan eksik ya da değer boştur (H4–H7 temizleme)
  • Tekrar:        az sayıda olay iki kez gönderilir (H10/H11 teslim garantisi, idempotentlik)
"""
import argparse, json, math, random, sys, time, uuid, zlib
from datetime import datetime, timedelta, timezone

sys.path.insert(0, "/kit/araclar")
from ortak import KAFKA, SABLONLAR, s3
from grup import ORNEK_KOD, normalle, senaryo, tohum, veri_ayarlari


# ─────────────────────────────────────────────────────────── yardımcılar
def zipf_secici(n, us=1.2, r=random):
    """1..n arasından Zipf dağılımlı seçim: küçük numaralar çok daha sık gelir."""
    agirlik = [1 / (i ** us) for i in range(1, n + 1)]
    toplam = sum(agirlik)
    birikimli, t = [], 0.0
    for a in agirlik:
        t += a / toplam
        birikimli.append(t)
    import bisect
    return lambda: bisect.bisect_left(birikimli, r.random()) + 1


def zaman_damgasi(an):
    return an.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


# ─────────────────────────────────────────────────────────── şablonlar
YOLLAR = ["/", "/urunler", "/urun/{}", "/sepet", "/odeme", "/giris", "/api/arama", "/api/stok/{}",
          "/kampanya", "/yardim", "/hesabim", "/static/app.js", "/static/logo.png"]
DURUMLAR = [(200, 82), (304, 6), (301, 2), (404, 6), (500, 3), (503, 1)]
AJANLAR = ["Mozilla/5.0 (Windows NT 10.0)", "Mozilla/5.0 (Macintosh)", "Mozilla/5.0 (iPhone)",
           "Mozilla/5.0 (Android 14)", "curl/8.4", "Googlebot/2.1"]
BOLGELER = ["Marmara", "Ege", "Akdeniz", "İç Anadolu", "Karadeniz", "Doğu Anadolu", "Güneydoğu"]
KAYNAKLAR = ["ajans-kuzey", "ajans-guney", "gazete-merkez", "portal-hizli", "yerel-haber", "ekonomi-gunluk"]
KATEGORILER = ["ekonomi", "spor", "teknoloji", "sağlık", "eğitim", "kültür", "dünya", "gündem"]
KONULAR = ["enflasyon", "faiz", "deprem", "seçim", "yapay zekâ", "transfer", "aşı", "sınav", "ihracat",
           "turizm", "konser", "iklim", "borsa", "maç", "üniversite", "hastane"]
FIILLER = ["açıklandı", "artıyor", "düştü", "tartışılıyor", "rekor kırdı", "ertelendi", "başladı", "sona erdi"]


# Türkçe ekler — ünlü uyumu ve sert ünsüz kuralıyla (arama analizörünü denemek için gerçek ekli sözcükler)
def _son_unlu(w):
    for h in reversed(w.lower()):
        if h in "aıoueiöüâîû":
            return h
    return "e"

def _kalin(w):
    return _son_unlu(w) in "aıouâû"

def _dortlu(w):
    return {"a": "ı", "ı": "ı", "â": "ı", "o": "u", "u": "u", "û": "u",
            "e": "i", "i": "i", "î": "i", "ö": "ü", "ü": "ü"}[_son_unlu(w)]

def ekle(w, ek):
    """ek: '' · 'lar' · 'ların' · 'lardan' · 'ımızdan' · 'daki' — kalın/ince ve sert/yumuşak biçim seçilir."""
    k = _kalin(w)
    if ek == "":        return w
    if ek == "lar":     return w + ("lar" if k else "ler")
    if ek == "ların":   return w + ("ların" if k else "lerin")
    if ek == "lardan":  return w + ("lardan" if k else "lerden")
    if ek == "daki":    return w + ("t" if w[-1] in "fstkçşhp" else "d") + ("aki" if k else "eki")
    if ek == "ımızdan":
        v = _dortlu(w)
        govde = w + ("m" + v + "z" if w[-1] in "aıoueiöüâîû" else v + "m" + v + "z")
        return govde + ("dan" if v in "ıu" else "den")
    raise ValueError(ek)

EKLER = ["", "lar", "ların", "lardan", "ımızdan", "daki"]


def buyuk(w):
    """Türkçe baş harf büyütme: i → İ, ı → I (str.capitalize bunu bilmez)."""
    ilk = {"i": "İ", "ı": "I"}.get(w[:1], w[:1].upper())
    return ilk + w[1:]


# ─────────────────────────────────────────────────────────── olay davranışı
# Olayların zamana, kaynağa ve bölgeye bağlı davranışı. Ayrıntılar grup koduna göre değişir (grup.py).

def _gun_tohumu(an, ek=""):
    """Tarihe (ve bir eke) bağlı, her çalıştırmada aynı çıkan sayı."""
    return zlib.crc32((an.strftime("%Y-%m-%d") + ek).encode())

# Saat başına göreli yoğunluk (Türkiye saati, UTC+3) — toplu üretimde olayların saatini belirler
SAAT_AGIRLIK = {
    "log":   [3, 2, 1, 1, 1, 2, 3, 5, 7, 8, 8, 8, 9, 8, 8, 8, 8, 9, 10, 12, 13, 13, 10, 6],
    "satis": [0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 6, 8, 10, 9, 8, 8, 9, 10, 12, 13, 12, 9, 5, 0],
    "iot":   [1] * 24,                                   # sensörler saatten bağımsız ölçer
    "haber": [2, 1, 1, 1, 1, 2, 4, 7, 9, 10, 10, 9, 9, 9, 9, 9, 9, 9, 8, 8, 8, 7, 5, 3],
}
BOLGE_SICAKLIK = {"Marmara": 20, "Ege": 24, "Akdeniz": 28, "İç Anadolu": 17,
                  "Karadeniz": 16, "Doğu Anadolu": 12, "Güneydoğu": 30}


def _tr_saat(an):
    return (an.hour + 3) % 24


def saat_agirlik(sablon, g):
    ag = SAAT_AGIRLIK[sablon]
    k = g.get("saat_kayma", 0)
    return ag[-k:] + ag[:-k] if k else ag


def uretec(sablon, r, kod=None):
    """Şablona göre olay üreten fonksiyon döndürür: olay(an) → dict, ya da None (o an olay yok)."""
    g = veri_ayarlari(kod, sablon)
    tuz = g["tuz"]
    if sablon == "log":
        ip_sec = zipf_secici(2000, g["zipf_us"], r)
        ipler = [f"10.{r.randint(0,255)}.{r.randint(0,255)}.{r.randint(1,254)}" for _ in range(2000)]
        kodlar, agirliklar = zip(*DURUMLAR)
        b = random.Random(tohum(tuz) + 1)                  # boyut tablosu olay akışını etkilemesin
        ip_satir = ["ip,ulke,kurum_turu"]
        for i, ip in enumerate(ipler):                     # en çok istek atan IP'ler çoğunlukla veri merkezi
            kurum = "veri-merkezi" if i < 15 else b.choices(["ev", "kurumsal", "mobil"], [6, 2, 3])[0]
            ulke = "TR" if kurum != "veri-merkezi" and b.random() < 0.9 else b.choice(["TR", "DE", "NL", "US"])
            ip_satir.append(f"{ip},{ulke},{kurum}")
        def olay(an):
            yol = r.choice(YOLLAR)
            yol_ad = yol.format(r.randint(1, 500)) if "{}" in yol else yol
            sure = r.lognormvariate(3.5, 0.8) * g["yavas_yol"].get(yol, 1.0)
            # günde bir kez hata patlaması + her 10 dakikanın ilk dakikasında küçük patlama
            patlama_saati = _gun_tohumu(an, "log" + tuz) % 24
            buyuk_patlama = _tr_saat(an) == patlama_saati and an.minute < g["patlama_dakika"]
            kucuk_patlama = an.minute % 10 == 0
            if buyuk_patlama or (kucuk_patlama and r.random() < 0.25):
                durum = r.choices([500, 503, 200], [55, 25, 20])[0]
                sure *= 3
            else:
                durum = r.choices(kodlar, agirliklar)[0]
            ajan = r.choice(AJANLAR)
            if ajan.startswith(("Googlebot", "curl")):            # botlar ürün sayfalarını tarar
                yol_ad = f"/urun/{r.randint(1, 500)}"
            return {"ip": ipler[ip_sec() - 1], "yontem": r.choices(["GET", "POST"], [9, 1])[0],
                    "yol": yol_ad, "durum": durum, "bayt": int(r.lognormvariate(8, 1.2)),
                    "sure_ms": round(sure, 1), "ajan": ajan}
        olay.boyutlar = {"ip_bilgisi.csv": ip_satir}
        return olay

    if sablon == "satis":
        urun_sec = zipf_secici(500, g["zipf_us"], r)
        fiyat = {u: round(r.uniform(15, 2500), 2) for u in range(1, 501)}
        b = random.Random(tohum(tuz) + 2)
        kategoriler = ["elektronik", "giyim", "gıda", "kozmetik", "ev", "kırtasiye", "oyuncak", "spor"]
        urun_satir = ["urun_id,kategori,marka,birim_maliyet"]
        for u in range(1, 501):
            urun_satir.append(f"{u},{b.choice(kategoriler)},marka-{b.randint(1, 40):02d},{fiyat[u] * b.uniform(0.45, 0.8):.2f}")
        sehirler = ["İstanbul", "Ankara", "İzmir", "Bursa", "Antalya", "Konya", "Adana", "Sakarya", "Kocaeli", "Eskişehir"]
        magaza_satir = ["magaza_id,sehir,metrekare"]
        for m, ag in g["magaza_agirlik"].items():          # büyük şubeler daha geniş
            magaza_satir.append(f"{m},{b.choice(sehirler)},{int(ag * b.uniform(350, 600))}")
        magazalar = list(g["magaza_agirlik"])
        magaza_ag = [g["magaza_agirlik"][m] for m in magazalar]
        sayac = [0]
        def olay(an):
            sayac[0] += 1
            u = urun_sec()
            tur = r.choices(["satis", "iade", "stok_girisi"], [90, 4, 6])[0]
            return {"siparis_id": f"S{sayac[0]:09d}",
                    "magaza_id": r.choices(magazalar, magaza_ag)[0], "urun_id": u,
                    "adet": r.randint(1, 5) if tur != "stok_girisi" else r.randint(20, 200),
                    "birim_fiyat": fiyat[u], "tur": tur}
        olay.boyutlar = {"urunler.csv": urun_satir, "magazalar.csv": magaza_satir}
        return olay

    if sablon == "iot":
        b = random.Random(tohum(tuz) + 3)
        sensor_satir = ["sensor_id,bolge,model,kurulum_yili"]
        txler = set(range(g["ariza_bas"], g["ariza_bas"] + 10)) | set(b.sample(range(1, 201), 15))
        for s_ in range(1, 201):                           # arızalı grubun hepsi aynı modelden
            model = "TX-200" if s_ in txler else b.choice(["SN-10", "SN-12", "KM-5", "TX-100"])
            sensor_satir.append(f"sensor-{s_:03d},{BOLGELER[s_ % len(BOLGELER)]},{model},{b.randint(2018, 2025)}")
        def olay(an):
            s = r.randint(1, 200)
            # bazı sensörler her saat 5 dakika susar; bazıları mesajlarının bir kısmını kaybeder
            if s % g["sus_mod"] == 0 and (an.minute // 5 + s) % 12 == 0:
                return None
            if s % g["kayip_mod"] == 0 and r.random() < g["kayip_oran"]:
                return None
            bolge = BOLGELER[s % len(BOLGELER)]
            taban = BOLGE_SICAKLIK[bolge] + g["bolge_kayma"][bolge]
            gunluk = 4 * math.sin(2 * math.pi * (_tr_saat(an) - 9) / 24)     # gündüz sıcak, gece serin
            sic = taban + (s % 5) * 0.4 + gunluk
            nem = 70 - taban + (s % 7)
            o = {"sensor_id": f"sensor-{s:03d}", "bolge": bolge,
                 "sicaklik": round(r.gauss(sic, 0.8), 2), "nem": round(r.gauss(nem, 2.5), 1),
                 "titresim": round(abs(r.gauss(0.2, 0.05)), 3)}
            # anomaliler: genelde nadir; arızalı bir grupta (10 ardışık sensör) günde bir saat sık
            ariza_saati = _gun_tohumu(an, "iot" + tuz) % 24
            arizali = g["ariza_bas"] <= s < g["ariza_bas"] + 10
            oran = 0.5 if (arizali and _tr_saat(an) == ariza_saati) else 0.003
            if r.random() < oran:
                o["sicaklik"] = round(sic + r.uniform(15, 40), 2)
                o["titresim"] = round(r.uniform(1.5, 4.0), 3)
            return o
        olay.boyutlar = {"sensorler.csv": sensor_satir}
        return olay

    if sablon == "haber":
        kaynak_sec = zipf_secici(len(KAYNAKLAR), g["zipf_us"], r)
        sayac = [0]
        def olay(an):
            sayac[0] += 1
            # her blokta (3, 4 ya da 6 saat) iki konu "gündem" olur ve 6 kat sık geçer
            blok = _gun_tohumu(an, tuz + str(_tr_saat(an) // g["gundem_blok_saat"]))
            gundem = {KONULAR[blok % len(KONULAR)], KONULAR[(blok // 7) % len(KONULAR)]}
            agirlik = [6 if k in gundem else 1 for k in KONULAR]
            konu = r.choices(KONULAR, agirlik)[0]
            konu2 = r.choice([k for k in KONULAR if k != konu])
            kaynak = KAYNAKLAR[kaynak_sec() - 1]
            tercih = g["kaynak_kategori"].get(kaynak)
            kategori = tercih if (tercih and r.random() < 0.6) else r.choice(KATEGORILER)
            baslik = r.choice([f"{buyuk(konu)} {r.choice(FIILLER)}",
                               f"{buyuk(ekle(konu, 'daki'))} gelişmeler {r.choice(FIILLER)}"])
            metin = (f"{buyuk(ekle(konu, 'daki'))} gelişmeler {r.choice(FIILLER)}. "
                     f"Uzmanlar {ekle(konu2, r.choice(['lardan', 'ımızdan']))} söz ederken "
                     f"{ekle(konu, 'ların')} etkisine dikkat çekti. "
                     f"{buyuk(ekle(konu2, 'lar'))} ile ilgili açıklamalar {r.choice(FIILLER)}.")
            return {"haber_id": f"H{sayac[0]:08d}", "kaynak": kaynak, "kategori": kategori,
                    "baslik": baslik, "metin": metin}
        olay.boyutlar = {"kaynaklar.csv": ["kaynak,tur,sehir,kurulus_yili",
                                           "ajans-kuzey,ajans,İstanbul,1952", "ajans-guney,ajans,Adana,1987",
                                           "gazete-merkez,gazete,Ankara,1964", "portal-hizli,portal,İstanbul,2011",
                                           "yerel-haber,gazete,Sakarya,1998", "ekonomi-gunluk,gazete,İstanbul,1979"]}
        return olay

    raise SystemExit("bilinmeyen şablon: " + sablon)


def baslangic_stok(kod=None):
    """Şablon 2: her ürünün dönem başındaki stok miktarı (aynı grup için her çalıştırmada aynı)."""
    g = veri_ayarlari(kod, "satis")
    return {u: g["stok_taban"] + (u * g["stok_adim"]) % 160 for u in range(1, 501)}


def kirlet(o, r, oran):
    """Az sayıda kayda gerçek hayattaki gibi kusur ekler."""
    if r.random() < oran:
        alan = r.choice([k for k in o if k != "olay_zamani"])
        if r.random() < 0.5:
            o.pop(alan)                             # eksik alan
        else:
            o[alan] = None                          # boş değer
    return o


# ─────────────────────────────────────────────────────────── kipler
def akis(a, olay, anahtar, r):
    from confluent_kafka import Producer
    idempotent = a.acks == "all" and not a.idempotent_kapali         # idempotent üretici acks=all ister
    if a.acks != "all" and not a.idempotent_kapali:
        print(f"ℹ️  acks={a.acks} ile idempotent üretici çalışmaz; kapatıldı.")
    p = Producer({"bootstrap.servers": KAFKA, "acks": a.acks, "enable.idempotence": idempotent,
                  "linger.ms": 20, "compression.type": "lz4"})
    konu = a.konu or SABLONLAR[a.sablon]["konu"]
    gec_en_fazla = a.gec_en_fazla_dk * 60
    print(f"→ Kafka '{konu}' · acks={a.acks} · idempotent={'açık' if idempotent else 'kapalı'}")
    print(f"→ Kafka '{konu}' konusuna saniyede ~{a.hiz} olay · geç oranı {a.gec_orani} "
          f"(en fazla {a.gec_en_fazla_dk} dk) · Ctrl+C ile dur")
    gonderilen, saniye = 0, 0
    try:
        while True:
            for _ in range(a.hiz):
                simdi = datetime.now(timezone.utc)
                gec = r.random() < a.gec_orani
                an = simdi - timedelta(seconds=r.uniform(30, gec_en_fazla)) if gec else simdi
                o = olay(an)
                if o is None:                       # o an bu kaynaktan olay yok (ör. susan sensör)
                    continue
                o = {"olay_zamani": zaman_damgasi(an), **kirlet(o, r, a.kirli_orani)}
                deger = json.dumps(o, ensure_ascii=False).encode("utf-8")
                anah = str(o.get(anahtar, "")).encode("utf-8")
                p.produce(konu, key=anah, value=deger)
                if r.random() < a.tekrar_orani:                 # aynı olay ikinci kez
                    p.produce(konu, key=anah, value=deger)
                gonderilen += 1
            p.poll(0)
            saniye += 1                                     # her tur bir saniyelik olay
            if a.sure and saniye >= a.sure:
                break
            time.sleep(1)
            if gonderilen % (a.hiz * 10) < a.hiz:
                print(f"  {gonderilen:,} olay gönderildi".replace(",", "."))
    except KeyboardInterrupt:
        pass
    p.flush(10)
    print(f"✓ toplam {gonderilen:,} olay".replace(",", "."))


def toplu(a, olay, r):
    istemci = s3()
    gun_sayisi = a.gun
    bitis = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    gunler = [bitis - timedelta(days=g) for g in range(gun_sayisi, 0, -1)]
    g = veri_ayarlari(a.grup, a.sablon)
    gun_ag = g.get("gun_agirlik", [1.0] * 7)                           # Pzt…Paz
    paylar = [gun_ag[gun.weekday()] for gun in gunler]
    adetler = [int(a.toplu * p / sum(paylar)) for p in paylar]
    adetler[-1] += a.toplu - sum(adetler)                              # kalan son güne
    saat_ag = saat_agirlik(a.sablon, g)
    toplam = tekrar = 0
    eski = [o["Key"] for sayfa in istemci.get_paginator("list_objects_v2").paginate(Bucket="ham", Prefix=a.sablon + "/")
            for o in sayfa.get("Contents", [])]
    if eski and not a.ekle:                       # aynı komut iki kez çalışınca veri ikiye katlanmasın
        for k in range(0, len(eski), 1000):
            istemci.delete_objects(Bucket="ham", Delete={"Objects": [{"Key": x} for x in eski[k:k + 1000]]})
        print(f"  s3://ham/{a.sablon}/ altındaki eski {len(eski)} dosya silindi (üstüne eklemek için: --ekle)")
    if a.sablon == "satis":                                             # şablon 2: başlangıç stoğu
        stok = "urun_id,baslangic_stok\n" + "".join(f"{u},{m}\n" for u, m in baslangic_stok(a.grup).items())
        istemci.put_object(Bucket="ham", Key="satis_baslangic_stok.csv", Body=stok.encode("utf-8"))
        print("  s3://ham/satis_baslangic_stok.csv  (500 ürünün başlangıç stoğu)")
    for ad, satir in olay.boyutlar.items():                            # küçük boyut tabloları (join için)
        istemci.put_object(Bucket="ham", Key=f"boyut/{a.sablon}/{ad}", Body=("\n".join(satir) + "\n").encode("utf-8"))
        print(f"  s3://ham/boyut/{a.sablon}/{ad}  ({len(satir) - 1} satır · boyut tablosu)")
    for gun, adet in zip(gunler, adetler):
        satirlar = []
        while len(satirlar) < adet:
            tr_saat = r.choices(range(24), saat_ag)[0]                  # Türkiye saati
            an = gun + timedelta(hours=(tr_saat - 3) % 24, seconds=r.uniform(0, 3600))
            o = olay(an)
            if o is None:
                continue
            o = {"olay_zamani": zaman_damgasi(an), **kirlet(o, r, a.kirli_orani)}
            satirlar.append(json.dumps(o, ensure_ascii=False))
            if r.random() < a.tekrar_orani:                             # aynı olay ikinci kez
                satirlar.append(satirlar[-1])
                tekrar += 1
        anahtar = f"{a.sablon}/tarih={gun:%Y-%m-%d}/parca-{uuid.uuid4().hex[:8]}.jsonl"
        istemci.put_object(Bucket="ham", Key=anahtar, Body=("\n".join(satirlar) + "\n").encode("utf-8"))
        toplam += len(satirlar)
        print(f"  s3://ham/{anahtar}  ({len(satirlar):,} olay)".replace(",", "."))
    print(f"✓ toplam {toplam:,} satır S3'e yazıldı (içinde {tekrar:,} tekrar) · grup {a.grup}".replace(",", "."))


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--sablon", required=True, choices=list(SABLONLAR))
    ap.add_argument("--hiz", type=int, default=20, help="akış: saniyedeki olay sayısı (varsayılan 20)")
    ap.add_argument("--sure", type=int, default=0, help="akış: kaç saniye sürsün (0 = Ctrl+C'ye kadar)")
    ap.add_argument("--konu", default=None, help="akış: Kafka konusu (varsayılan şablona göre)")
    ap.add_argument("--toplu", type=int, default=0, help="toplu: S3'e yazılacak olay sayısı")
    ap.add_argument("--gun", type=int, default=7, help="toplu: kaç günlük geçmiş (varsayılan 7)")
    ap.add_argument("--grup", default=None, help="grup kodu: üyelerin en küçük öğrenci numarası (verinizi belirler)")
    ap.add_argument("--gec-orani", type=float, default=None, help="geç olay oranı (varsayılan: senaryo kartınızdaki)")
    ap.add_argument("--gec-en-fazla-dk", type=int, default=None, help="geç olay en fazla kaç dk (varsayılan: senaryo kartı)")
    ap.add_argument("--acks", default="all", choices=["0", "1", "all"], help="akış: üretici onayı (varsayılan all)")
    ap.add_argument("--idempotent-kapali", action="store_true", help="akış: idempotent üreticiyi kapat")
    ap.add_argument("--ekle", action="store_true", help="toplu: eski veriyi silmeden üstüne ekle")
    ap.add_argument("--kirli-orani", type=float, default=0.002, help="kusurlu kayıt oranı")
    ap.add_argument("--tekrar-orani", type=float, default=0.002, help="iki kez gönderilen olay oranı")
    ap.add_argument("--tohum", type=int, default=None, help="rastgelelik tohumu (varsayılan: grup kodundan)")
    a = ap.parse_args()
    if not a.grup:
        print("⚠️  --grup verilmedi: ÖRNEK veri üretiliyor. Projede kendi grup kodunuzu kullanın.")
    a.grup = normalle(a.grup)
    sen = senaryo(a.grup, a.sablon)
    a.gec_orani = sen["gec_orani"] if a.gec_orani is None else a.gec_orani
    a.gec_en_fazla_dk = sen["gec_en_fazla_dk"] if a.gec_en_fazla_dk is None else a.gec_en_fazla_dk
    r = random.Random(tohum(a.grup) if a.tohum is None else a.tohum)
    olay = uretec(a.sablon, r, a.grup)
    if a.toplu:
        toplu(a, olay, r)
    else:
        akis(a, olay, SABLONLAR[a.sablon]["anahtar"], r)
