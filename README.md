# Dağıtık Trendyol crawler

Her makinede bir worker çalışır. Worker `crawler_queue` kuyruğundan bir iş alır,
Chromium ile tarar ve sonucu `crawler_results` kuyruğuna kalıcı olarak gönderir.
Sonuç yayını broker tarafından onaylandıktan sonra asıl mesaj ACK edilir. API
veya SQL veritabanına doğrudan bağlanmaz; Redis erişimi de gerekmez.

## Makine kurulumu

```sh
cp .env.example .env
```

`.env` içine merkezi RabbitMQ host/port/kullanıcı/parola yazın. API ile aynı
`crawler` vhost'unu ve aynı request/results/dead kuyruk adlarını kullanın.
Örnekler test parolası içermez; üretim erişim bilgileri `.env` içinde kalır.
Her makinede `WORKER_ID` farklı olmalı (`crawler-01` … `crawler-10`); boşsa hostname
ve PID kullanılır. Varsayılan `PREFETCH_COUNT=1`, tarama timeout'u 60 saniyedir.

Yayınlanmış imajı kullanmak için:

```sh
docker compose -f docker-compose.prod.yml up -d
docker compose -f docker-compose.prod.yml logs --tail=50
```

Kaynak koddan imaj oluşturmak için:

```sh
docker build -t trendyol-crawler:local .
CRAWLER_IMAGE=trendyol-crawler:local CRAWLER_PULL_POLICY=never docker compose -f docker-compose.prod.yml up -d
```

ARM64 makinede native imaj oluşturduysanız aynı komuta `CRAWLER_PLATFORM=linux/arm64`
ekleyin. Varsayılan yayınlanan imaj linux/amd64'dür. Chromium'un native sürümü
Dockerfile içinde kurulur; Puppeteer ayrıca browser indirmez. İmaj Node 22 kullanır.

Aynı kurulumu 10 makineye uygulayın. Her container kendi browser profilini kullanır;
`user_data` dizinini makineler/süreçler arasında paylaşmayın. API besleyici ve sonuç
tüketicisi ayrıca API sunucusunda çalışmalıdır; kurulum belgesi API deposunda
`docs/trendyol-crawler.md` dosyasıdır.

## Sağlık ve toparlanma

```sh
docker compose -f docker-compose.prod.yml exec puppeteer-api wget -qO- http://localhost:3000/health
```

Hazır browser ve broker bağlantısında HTTP 200, bağlantı yokken 503 döner.
RabbitMQ bağlantısı koptuğunda yeniden bağlanılır; eski kanalın mesajı yeni kanalda
ACK edilmez. Browser çökerse süreç hata koduyla kapanır ve Docker yeniden başlatır.
SIGTERM/SIGINT sırasında bağlantı kapanır; ACK edilmemiş işler başka worker'a döner.

Geçici tarama hatalarında en fazla üç deneme, 30 ve 120 saniyelik gecikmeler vardır.
Sonuç ve retry mesajları expiry taşır. İşin `expiresAt` tarihi bütün denemelerde
korunur; süresi dolmuş iş taranmaz. Makinelerin saatleri senkron olmalıdır.
`MAX_HTML_BYTES` varsayılanı 4 MiB'dir; HTML loglanmaz. Dead-letter metadata'sı
1 gün expiry ve 1000 mesaj/16 MiB sınırıyla saklanır. Docker logları 3 × 10 MiB ile
sınırlıdır. API hata sonucu geldiğinde son geçerli fiyatları korur.

## Geliştirme ve test

```sh
npm ci --ignore-scripts
npm test
```

Gerçek broker testi için ayrı yerel RabbitMQ üzerinde `crawler` vhost'u ve
`crawltest/crawltest` test kullanıcısını hazırlayın:

```sh
TY_CRAWLER_TEST_RABBITMQ_PORT=15673 npm test
```

Test 10 bağımsız consumer kanalının işleri paylaşmasını ve tekil sonuçlarını
doğrular. `tests/pipeline-worker.js`, API'nin uçtan uca testinde gerçek browser ile
yerel fixture sayfalarını taramak için kullanılır; üretim imajına dahil edilmez.
