# BSM 461 Dönem Projesi — Başlangıç Kiti

Bu kit, dönem projesinde kullanacağınız bütün altyapıyı **tek komutla** bilgisayarınızda ayağa kaldırır:
olay günlüğü (Kafka), nesne deposu (S3), toplu ve akış işleme (Spark), arama (Elasticsearch), pano (Kibana)
ve servis deposu örneği (MongoDB).
Ayrıca dört proje şablonu için **sahte veri üreten** bir üretici ve **doldurmanız gereken iskelet işler** içerir.

> ⚠️ **Kit projenin iskeleti değil, zeminidir.** "Ortam kurulamadı" mazeretini ortadan kaldırmak için verilir.
> Mimari kararlar, bileşen seçimi, bölümleme, garanti tasarımı ve analiz **sizindir** ve notlanan da odur.

---

## 1. Gereksinimler

| | En az | Önerilen |
|---|---|---|
| Bellek (RAM) | 8 GB (yalnızca çekirdek kit) | 16 GB |
| Docker'a ayrılan bellek | **5 GB** (çekirdek kit) | **7 GB** (Elasticsearch, Kibana ve MongoDB ile) |
| Boş disk | 10 GB | 20 GB |
| Yazılım | Docker Desktop (Windows / macOS) ya da Docker Engine + Compose (Linux) | — |
| İnternet | İlk kurulumda imajlar (~3 GB) ve Spark kütüphaneleri (~300 MB) indirilir | — |

**Docker'a ayrılan bellek** Docker Desktop → *Settings* → *Resources* → *Memory* bölümünden ayarlanır.
Windows'ta (WSL 2) bu ayar yoktur; WSL varsayılan olarak bilgisayar belleğinin yarısını kullanır — 8 GB'lık bir
bilgisayarda 4 GB yetmez. `%UserProfile%\.wslconfig` dosyasına şunu yazıp `wsl --shutdown` ile yeniden başlatın:

```ini
[wsl2]
memory=5GB
```

**Ölçülen bellek kullanımı** (aynı anda, tepe değer; Apple M4, 2026-10-05):

| Durum | Kafka | S3 | Spark | Elasticsearch | Kibana | MongoDB | **Toplam** |
|---|---|---|---|---|---|---|---|
| Çekirdek, boşta | 0,5 GB | 0,1 GB | — | | | | **0,7 GB** |
| Çekirdek, yük altında ¹ | 0,6 GB | 0,4 GB | 2,6 GB | | | | **3,5 GB** |
| Hepsi açık, boşta | 0,6 GB | 0,2 GB | — | 1,3 GB | 1,0 GB | 0,2 GB | **3,3 GB** |
| Hepsi açık, yük altında ² | 0,6 GB | 0,4 GB | 2,3 GB | 1,3 GB | 0,9 GB | 0,2 GB | **5,3 GB** |

¹ Üretici saniyede 500 olay + akış işi + aynı anda bir toplu iş. ² Buna ek olarak arama örneği 20.000 haber indeksliyor.

Docker'ın kendisi de yaklaşık 0,5 GB kullanır; önerilen değerler bu yüzden toplamın biraz üstündedir.
**En çok belleği Spark yer** — her `spark-submit` yaklaşık 1–1,3 GB'lık ayrı bir süreç açar. Belleğiniz azsa
aynı anda **tek** Spark işi çalıştırın ve kullanmadığınız Kibana'yı kapatın (`docker compose stop kibana`, 1 GB kazandırır).

**Windows:** Docker Desktop'ı **WSL 2** arka ucuyla kurun. Komutları **PowerShell** ya da **WSL terminali**nde
çalıştırın. `dogrula.sh` yalnızca WSL ya da Git Bash içinde çalışır; PowerShell'de 4. adımdaki
`docker compose exec araclar python araclar/dogrula.py` komutunu kullanın.

---

## 2. Kurulum — beş adım

```bash
# 1) Kit klasörüne girin
cd kit

# 2) Çekirdeği başlatın (ilk seferde imajlar indirilir: 5–15 dakika)
docker compose up -d

# 3) Kapsayıcıların durumuna bakın — kafka ve s3 "healthy" olmalı
docker compose ps

# 4) Doğrulayın (≈10 saniye)
./dogrula.sh

# 5) Spark tarafını da doğrulayın (ilk seferde kütüphaneler indirilir: 3–7 dakika)
./dogrula.sh --tam
```

Beklenen çıktı:

```
  ✅  Kafka  · yaz → oku                             yazılan mesaj geri okundu
  ✅  S3     · kovalar ve yaz → oku                  kovalar: ham, islenmis, tablolar · yaz/oku çalışıyor
  ✅  DuckDB · s3:// üzerinden Parquet               DuckDB s3:// üzerinden Parquet yazdı ve okudu (1 satır)
  ➖  Elasticsearch (yalnızca --profile arama ile)   kapalı — açmak için: docker compose --profile arama up -d
  ➖  MongoDB (yalnızca --profile servis ile)        kapalı — açmak için: docker compose --profile servis up -d

  🎉 Çekirdek kit çalışıyor.

✅ Spark → S3 Parquet yazdı ve okudu (1000 satır)
✅ Delta Lake: 2 sürüm · zaman yolculuğu (sürüm 0) 1000 satır
✅ Spark → Kafka okudu ('kit-dogrulama' konusunda 1 mesaj)
🎉 Spark tarafı çalışıyor.
```

🔴 **Bu çıktının ekran görüntüsü, proje raporunuzun ekinde istenir** ("Ek: ortam kanıtı").

---

## 3. Kitte ne var?

| Servis | Ne işe yarar | Bilgisayarınızdan adresi | Hafta |
|---|---|---|---|
| `kafka` | Olay günlüğü — Kafka 4.0, ZooKeeper'sız (KRaft), tek düğüm | `localhost:9092` | H10 |
| `s3` | S3 uyumlu nesne deposu (SeaweedFS); kovalar `ham`, `islenmis`, `tablolar` | `http://localhost:8333` | H4 |
| `spark` | Spark 3.5 tezgâhı: toplu ve akış işleri buradan çalışır; Kafka, S3 ve Delta Lake hazır | Spark UI: `http://localhost:4040` (iş çalışırken) | H6, H7, H9, H11 |
| `araclar` | Python araçları: veri üretici, doğrulama, DuckDB, arama örneği | — | — |
| `elasticsearch` | Arama ve log analitiği (`--profile arama`) | `http://localhost:9200` | H8 |
| `kibana` | Pano (`--profile pano`, isteğe bağlı; Elasticsearch'i de açar — Kibana yalnızca oradaki veriyi gösterir) | `http://localhost:5601` | H8 |
| `mongo` | Servis deposu örneği: MongoDB 7 (`--profile servis`, isteğe bağlı) | `mongodb://localhost:27017` | H8 |

**S3 erişim bilgileri** (yalnızca yerel geliştirme için): anahtar `bsm461`, gizli anahtar `bsm461-gizli`.

### Dosyalar

```
kit/
├── docker-compose.yml        bütün servisler
├── spark-defaults.conf       Spark'ın Kafka / S3 / Delta ayarları (her işe kendiliğinden uygulanır)
├── log4j2.properties         Spark'ın ekrana yazdığı günlükleri kısar
├── dogrula.sh                ortam denetimi
├── bin/spark-submit          spark-submit'in sessiz hâli: kütüphane yükleme iletilerini gizler (süzülmemiş çıktı: /opt/spark/bin/spark-submit)
├── araclar/
│   ├── uretici.py            ⭐ dört şablon için sahte veri — akış (Kafka) ya da toplu (S3)
│   ├── senaryo.py            grubunuzun senaryo kartı (gerçek sistem koşulları)
│   ├── grup.py               grup kodundan veri ayarlarını ve senaryoyu türetir
│   ├── arama_ornek.py        Elasticsearch: Türkçe analizörlü indeks + arama + toplama
│   ├── dogrula.py            dogrula.sh'nin çalıştırdığı denemeler
│   ├── kova_kur.py           S3 kovalarını oluşturur (kendiliğinden çalışır)
│   ├── ortak.py              adresler ve şablon tanımları
│   └── Dockerfile, s3.json   araç imajı ve S3 kimlik bilgileri
└── is/
    ├── toplu_is.py           ⭐ TOPLU HAT İSKELETİ — ham JSON → Parquet → Delta → sorgu + explain()
    ├── akis_iskelet.py       ⭐ AKIŞ HATTI İSKELETİ — Kafka → pencere + watermark → çıktı
    └── dogrula_spark.py      Spark denemesi (dogrula.sh --tam)
```

⭐ işaretli dosyalar **çalışır hâlde** gelir, ama içlerindeki `TODO` satırları sizin işinizdir.

---

## 4. Veri üretici

> 🔑 **Önce grup kodunuz:** üç üyenin öğrenci numaralarından **en küçüğü** (ör. `B231210001`). Üreticiyi her zaman
> `--grup <kodunuz>` ile çalıştırın: veriniz ve senaryonuz bu koddan türetilir, başka gruplarınkinden farklıdır.
> Aynı kod her seferinde **aynı veriyi** üretir. `--grup` vermezseniz yalnızca deneme amaçlı ÖRNEK veri üretilir.
>
> ```bash
> docker compose exec araclar python araclar/senaryo.py --grup <kodunuz> --sablon <şablon>   # senaryo kartınız
> ```

Dört şablonun her biri için gerçekçi olaylar üretir. **Bilerek kusurlu** veri üretir; çünkü gerçek veri öyledir
ve raporunuzda bu kusurları ele almanız beklenir.
Ayrıca veride gerçek sistemlerdeki gibi **desenler** vardır (saate ve güne bağlı değişimler, ani olaylar);
bunları kodda değil, **verinizde** gösterin — her bulgu bir sorgu ve bir ölçümle desteklenmeli. Satış şablonunda toplu üretim, ürünlerin başlangıç stoğunu
`s3://ham/satis_baslangic_stok.csv` dosyasına da yazar.

**Boyut tabloları.** Toplu üretim, olaylarla birleştirmeniz (join) için küçük tabloları da yazar:

| Şablon | Dosya (`s3://ham/boyut/<şablon>/`) | Join anahtarı | Sütunlar |
|---|---|---|---|
| `log` | `ip_bilgisi.csv` (2.000 satır) | `ip` | ulke, kurum_turu |
| `satis` | `urunler.csv` (500) · `magazalar.csv` (20) | `urun_id` · `magaza_id` | kategori, marka, birim_maliyet · sehir, metrekare |
| `iot` | `sensorler.csv` (200) | `sensor_id` | bolge, model, kurulum_yili |
| `haber` | `kaynaklar.csv` (6) | `kaynak` | tur, sehir, kurulus_yili |

**Üretici ayarları (akış).** Varsayılan `acks=all` ve idempotent üreticidir. Denemek için `--acks 0|1|all` ve
`--idempotent-kapali` (idempotent üretici yalnızca `acks=all` ile çalışır; başka değerde kendiliğinden kapanır).

```bash
# Akış: Kafka'ya sürekli olay (Ctrl+C ile durur)
docker compose exec araclar python araclar/uretici.py --grup <kodunuz> --sablon log
docker compose exec araclar python araclar/uretici.py --grup <kodunuz> --sablon iot --hiz 200 --sure 120

# Toplu: S3'e geçmiş günlerin verisi (o şablonun eski ham verisi önce silinir; üstüne eklemek için --ekle)
docker compose exec araclar python araclar/uretici.py --grup <kodunuz> --sablon satis --toplu 2000000 --gun 14

# Tüm seçenekler
docker compose exec araclar python araclar/uretici.py --help
```

| Şablon | Kafka konusu | Anahtar | Alanlar |
|---|---|---|---|
| `log` | `web-loglari` | `ip` | olay_zamani, ip, yontem, yol, durum, bayt, sure_ms, ajan |
| `satis` | `satislar` | `urun_id` | olay_zamani, siparis_id, magaza_id, urun_id, adet, birim_fiyat, tur |
| `iot` | `sensorler` | `sensor_id` | olay_zamani, sensor_id, bolge, sicaklik, nem, titresim |
| `haber` | `haberler` | `kaynak` | olay_zamani, haber_id, kaynak, kategori, baslik, metin |

**Bilerek konan kusurlar:**

| Kusur | Varsayılan | Değiştirmek için | Hangi haftayla ilgili |
|---|---|---|---|
| **Çarpık anahtar:** birkaç anahtar olayların çoğunu üretir | hep açık | — | H3 sıcak nokta, H7 çarpıklık |
| **Geç olay** (yalnızca akışta): `olay_zamani` gönderildiği andan birkaç dakika önce | senaryo kartınızdaki oran ve gecikme | `--gec-orani 0.2` · `--gec-en-fazla-dk 10` | H11 watermark |
| **Tekrar:** aynı olay iki kez gönderilir (akışta) ya da yazılır (toplu) | %0,2 | `--tekrar-orani` | H10–H11 teslim garantisi, H3 idempotentlik |
| **Kirli kayıt:** eksik alan ya da boş değer | %0,2 | `--kirli-orani` | H4–H7 temizleme (Z10) |

Aynı grup kodu her çalıştırmada **aynı** veriyi üretir; ölçümlerinizi karşılaştırmak için ayrıca bir şey yapmanız gerekmez.

> ℹ️ Akışta üreticinin yazdığı "gönderilen" sayısına tekrar eden olaylar **dahil değildir**; Kafka'daki ve
> çıktınızdaki olay sayısı bu yüzden biraz fazla çıkabilir.

---

## 5. İşleri çalıştırmak

```bash
# Toplu hat (önce toplu veri üretin)
docker compose exec spark spark-submit is/toplu_is.py --sablon log

# Akış hattı (iki ayrı terminal)
docker compose exec araclar python araclar/uretici.py --grup <kodunuz> --sablon iot --hiz 50
docker compose exec spark spark-submit is/akis_iskelet.py --sablon iot
#   çıktıyı S3'e yazmak için:  ... akis_iskelet.py --sablon iot --cikti s3

# Arama (şablon 1 ve 4)
docker compose --profile arama up -d
docker compose exec araclar python araclar/arama_ornek.py --sorgu "ihracatımızdan"

# Servis deposu örneği (MongoDB) — Python'dan pymongo ile: mongodb://mongo:27017
docker compose --profile servis up -d

# DuckDB ile S3'teki Parquet'i sorgulamak (araclar kapsayıcısında Python)
docker compose exec araclar python -c "import duckdb; c=duckdb.connect(); c.execute(\"INSTALL httpfs; LOAD httpfs; SET s3_endpoint='s3:8333'; SET s3_use_ssl=false; SET s3_url_style='path'; SET s3_access_key_id='bsm461'; SET s3_secret_access_key='bsm461-gizli'; SET s3_region='us-east-1'\"); print(c.sql(\"SELECT durum, count(*) FROM 's3://islenmis/log/*/*.parquet' GROUP BY 1 ORDER BY 2 DESC\"))"
```

Kendi dosyalarınızı `kit/is/` ya da `kit/` altındaki herhangi bir klasöre koyun; kapsayıcılar `kit/` klasörünü
`/kit` olarak görür. Bilgisayarınızda düzenlediğiniz dosya anında kapsayıcının içinde de değişir.

**Spark UI:** bir iş çalışırken `http://localhost:4040` adresini açın. Aşama, görev süresi dağılımı ve shuffle
boyutu burada görünür (H7). İş bitince arayüz de kapanır.

---

## 6. Durdurmak ve temizlemek

```bash
docker compose stop                                   # durdur, sonra "docker compose start" ile devam
docker compose --profile arama --profile pano --profile servis down      # kapsayıcıları kaldır — VERİ DURUR
docker compose --profile arama --profile pano --profile servis down -v   # kapsayıcıları ve BÜTÜN VERİYİ sil (geri alınamaz)
```

---

## 7. Sık karşılaşılan sorunlar

| Belirti | Neden | Çözüm |
|---|---|---|
| `Cannot connect to the Docker daemon` | Docker Desktop kapalı | Docker Desktop'ı açın, simgesi "running" olana kadar bekleyin |
| `port is already allocated` (9092, 8333, 9200, 5601, 4040, 27017) | Bilgisayarınızda başka bir program o portu kullanıyor | O programı kapatın ya da `docker-compose.yml`'de `"9092:9092"` gibi satırların **sol** tarafını değiştirin |
| Kafka ya da Elasticsearch hemen kapanıyor, günlükte `SIGILL` | Apple **M4** işlemcili Mac'lerde Docker içindeki Java'nın bilinen sorunu | Kit bu sorunu `-XX:UseSVE=0` ayarıyla zaten çözüyor. `docker-compose.yml`'deki `JAVA_TOOL_OPTIONS` / `CLI_JAVA_OPTS` satırlarını silmeyin |
| Elasticsearch `exited (137)` | Docker'a ayrılan bellek yetmiyor | Docker'ın belleğini 7 GB'a çıkarın ya da Kibana'yı açmayın |
| `dogrula.sh: /bin/bash^M: bad interpreter` | Dosya Windows satır sonlarıyla kaydedilmiş | Dosyayı LF satır sonuyla kaydedin, ya da `docker compose exec araclar python araclar/dogrula.py` kullanın |
| Spark ilk çalıştırmada uzun süre bekliyor | Kütüphaneler indiriliyor (~300 MB) | Bir kez olur; sonra önbellekten gelir |
| Spark: `No writable volumes` / `Status Code: 500` S3 hatası | S3 diski dolu | `docker system df` ile yer açın; gerekirse `down -v` (veri silinir) |
| Akış işi hiçbir şey yazmıyor | Üretici çalışmıyor ya da konu adı yanlış | Üreticinin çalıştığı terminale bakın; `--sablon` iki tarafta aynı olmalı |
| Pencere sonuçları "eksik" | Watermark geç olayları atıyor | Bu bir hata değil, **sizin karar vermeniz gereken** ödünleşim (H11) |


---

## 8. Bilinçli sadeleştirmeler — raporunuzda tartışın

Kit **tek bilgisayarda** çalışmak için sadeleştirildi. Gerçek bir sistemde bunların hepsi farklıdır, ve
"Bu sistem nerede kırılır?" bölümünüz için iyi başlangıç noktalarıdır:

- Kafka **tek düğüm, tek kopya** (`replication factor = 1`): düğüm ölürse veri gider. Üretimde 3 olur (H3, H10).
- S3 **tek düğüm**: çoğaltma yok (H3, H4).
- Spark `local[*]` kipinde: sürücü ve yürütücüler aynı süreçte; gerçek küme yok (H6).
- Elasticsearch **tek düğüm, replika 0**: küme sarı ya da yeşil görünür, ama kopyası yoktur (H8, H3).
- Güvenlik **kapalı**: şifre ve TLS yok. Asla internete açık bir makinede böyle çalıştırmayın.
