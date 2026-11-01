const fs = require('node:fs');
const path = require('node:path');
const puppeteer = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');

puppeteer.use(StealthPlugin());

async function launchRenderer() {
  const userDataDir = process.env.PUPPETEER_USER_DATA_DIR || path.join(__dirname, '..', 'user_data');
  fs.mkdirSync(userDataDir, { recursive: true });
  for (const file of ['SingletonLock', 'SingletonCookie', 'SingletonSocket']) {
    fs.rmSync(path.join(userDataDir, file), { force: true });
  }
  const browser = await puppeteer.launch({
    headless: 'new', executablePath: process.env.PUPPETEER_EXECUTABLE_PATH || undefined, userDataDir,
    args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage',
      '--disable-blink-features=AutomationControlled', '--window-size=1920,1080', '--lang=tr-TR,tr;q=0.9'],
  });
  return {
    connected: () => browser.isConnected(),
    onDisconnected: (callback) => browser.once('disconnected', callback),
    close: () => browser.close(),
    async render(url, timeout) {
      const page = await browser.newPage();
      try {
        await page.setViewport({ width: 1920, height: 1080 });
        await page.setExtraHTTPHeaders({ 'Accept-Language': 'tr-TR,tr;q=0.9' });
        await page.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36');
        const response = await page.goto(url, { waitUntil: 'networkidle2', timeout });
        return { status: response?.status() ?? null, html: await page.content() };
      } finally {
        await page.close();
      }
    },
  };
}

module.exports = { launchRenderer };
