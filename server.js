require('dotenv').config();

const fs = require('node:fs');
const path = require('node:path');
const express = require('express');
const amqp = require('amqplib');
const puppeteer = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');

puppeteer.use(StealthPlugin());

const {
  RABBITMQ_HOST = 'localhost',
  RABBITMQ_PORT = '5672',
  RABBITMQ_USER = 'guest',
  RABBITMQ_PASSWORD = 'guest',
  RABBITMQ_VHOST = '/',
  RABBITMQ_QUEUE = 'crawler_requests',
  PORT = '3000',
  RENDER_TIMEOUT_MS = '60000',
  PREFETCH_COUNT = '1',
  RECONNECT_DELAY_MS = '5000',
} = process.env;

let browser;
let channel;
let isReady = false;

// Önceki bir çökme sonrası kalan Chromium profil kilidini temizler,
// aksi halde yeni process "profile in use" hatasıyla açılamaz.
function clearStaleProfileLock(userDataDir) {
  for (const file of ['SingletonLock', 'SingletonCookie', 'SingletonSocket']) {
    fs.rmSync(path.join(userDataDir, file), { force: true });
  }
}

// Sunucu başladığında tek bir Browser instance'ı açıyoruz (Performans için)
async function launchBrowser() {
  const userDataDir = path.join(__dirname, 'user_data');
  clearStaleProfileLock(userDataDir);

  console.log('[Browser] Stealth Chromium başlatılıyor...');
  browser = await puppeteer.launch({
    headless: 'new',
    executablePath: process.env.PUPPETEER_EXECUTABLE_PATH || undefined,
    userDataDir,
    args: [
      '--no-sandbox',
      '--disable-setuid-sandbox',
      '--disable-dev-shm-usage',
      '--disable-blink-features=AutomationControlled',
      '--window-size=1920,1080',
      '--lang=tr-TR,tr;q=0.9',
    ],
  });
  console.log('[Browser] Hazır.');
}

// URL taranır ve işlenmiş nihai HTML döndürülür
async function renderUrl(url) {
  const page = await browser.newPage();
  try {
    await page.setViewport({ width: 1920, height: 1080 });

    // Ekran ve Platform taklidini yüklüyoruz
    await page.evaluateOnNewDocument(() => {
      Object.defineProperty(window.screen, 'width', { get: () => 1920 });
      Object.defineProperty(window.screen, 'height', { get: () => 1080 });
      Object.defineProperty(window.screen, 'availWidth', { get: () => 1920 });
      Object.defineProperty(window.screen, 'availHeight', { get: () => 1040 });
      Object.defineProperty(navigator, 'platform', { get: () => 'Win32' });
    });

    await page.setUserAgent(
      'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36'
    );

    // Siteye git ve ağ istekleri bitene kadar bekle (JS render tamamlansın)
    const response = await page.goto(url, { waitUntil: 'networkidle0', timeout: Number(RENDER_TIMEOUT_MS) });

    const html = await page.content();
    return { status: response ? response.status() : null, html };
  } finally {
    await page.close(); // Tab'ı kapatarak bellek sızıntısını önlüyoruz
  }
}

const HTML_PREVIEW_LENGTH = 500;

// HTML verilen endpoint üzerinden post edilir
async function postResult(callbackUrl, payload) {
  const response = await fetch(callbackUrl, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  });

  if (!response.ok) {
    throw new Error(`Callback isteği başarısız: ${response.status} ${response.statusText}`);
  }
}

async function handleMessage(msg) {
  let payload;
  try {
    payload = JSON.parse(msg.content.toString());
  } catch (error) {
    console.error('[Mesaj] JSON parse edilemedi:', error.message);
    channel.ack(msg);
    return;
  }

  const { url, callbackUrl, jobId } = payload;

  if (!url || !callbackUrl) {
    console.error('[Mesaj] Geçersiz payload, "url" ve "callbackUrl" alanları zorunludur:', payload);
    channel.ack(msg);
    return;
  }

  console.log(`[Job ${jobId || '-'}] Taranıyor: ${url}`);

  try {
    const { status, html } = await renderUrl(url);
    console.log(
      `[Job ${jobId || '-'}] Sonuç -> status: ${status}, html uzunluğu: ${html.length} karakter`
    );
    console.log(
      `[Job ${jobId || '-'}] HTML önizleme:\n${html.slice(0, HTML_PREVIEW_LENGTH)}${
        html.length > HTML_PREVIEW_LENGTH ? '...' : ''
      }`
    );
    await postResult(callbackUrl, { jobId, url, success: true, status, html });
    console.log(`[Job ${jobId || '-'}] Tamamlandı, sonuç gönderildi -> ${callbackUrl}`);
  } catch (error) {
    console.error(`[Job ${jobId || '-'}] Hata:`, error.message);
    try {
      await postResult(callbackUrl, { jobId, url, success: false, error: error.message });
    } catch (callbackError) {
      console.error(`[Job ${jobId || '-'}] Callback gönderilemedi:`, callbackError.message);
    }
  } finally {
    // Kalıcı olarak taranamayan URL'lerde kuyruğun kilitlenmemesi için
    // sonucu (başarılı/başarısız) her durumda callback'e bildirip mesajı ack'liyoruz.
    channel.ack(msg);
  }
}

async function connectRabbitMQ() {
  for (;;) {
    try {
      console.log('[RabbitMQ] Bağlanılıyor...');
      const connection = await amqp.connect({
        protocol: 'amqp',
        hostname: RABBITMQ_HOST,
        port: Number(RABBITMQ_PORT),
        username: RABBITMQ_USER,
        password: RABBITMQ_PASSWORD,
        vhost: RABBITMQ_VHOST,
      });

      connection.on('error', (error) => {
        console.error('[RabbitMQ] Bağlantı hatası:', error.message);
      });

      connection.on('close', () => {
        console.error('[RabbitMQ] Bağlantı koptu, yeniden bağlanılıyor...');
        isReady = false;
        connectRabbitMQ();
      });

      channel = await connection.createChannel();
      await channel.assertQueue(RABBITMQ_QUEUE, { durable: true });
      await channel.prefetch(Number(PREFETCH_COUNT));

      channel.consume(RABBITMQ_QUEUE, (msg) => {
        if (msg) {
          handleMessage(msg).catch((error) => console.error('[Mesaj] İşlenemedi:', error));
        }
      });

      isReady = true;
      console.log(`[RabbitMQ] Bağlandı, "${RABBITMQ_QUEUE}" kuyruğu dinleniyor.`);
      return;
    } catch (error) {
      console.error(
        `[RabbitMQ] Bağlantı kurulamadı, ${RECONNECT_DELAY_MS}ms sonra tekrar denenecek:`,
        error.message
      );
      await new Promise((resolve) => setTimeout(resolve, Number(RECONNECT_DELAY_MS)));
    }
  }
}

// Docker/orkestrasyon sağlık kontrolü için minimal HTTP servisi
const app = express();
app.disable('x-powered-by');
app.get('/health', (req, res) => {
  res.json({ status: isReady ? 'ok' : 'starting' });
});

(async () => {
  await launchBrowser();
  await connectRabbitMQ();

  app.listen(Number(PORT), () => {
    console.log(`[HTTP] Sağlık kontrolü servisi ${PORT} portunda çalışıyor.`);
  });
})().catch((error) => {
  console.error('[Başlangıç] Servis başlatılamadı:', error);
  process.exit(1);
});
