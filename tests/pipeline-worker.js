// API entegrasyon testi bu süreci ayrı worker olarak, yerel ürün sayfasıyla çalıştırır.
const fs = require('node:fs/promises');
const os = require('node:os');
const path = require('node:path');
const http = require('node:http');
const amqp = require('amqplib');
const { loadConfig } = require('../lib/config');
const { declareTopology } = require('../lib/topology');
const { createWorker } = require('../lib/worker');
const { launchRenderer } = require('../lib/renderer');

async function main() {
  const config = loadConfig(process.env);
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), 'ty-crawl-browser-'));
  process.env.PUPPETEER_USER_DATA_DIR = dir;
  let renderer;
  let connection;
  const server = http.createServer((request, response) => {
    const contentId = request.url.match(/-p-(\d+)/)?.[1] || '123';
    const price = (amount) => ({ discountedPrice: { value: amount }, sellingPrice: { value: amount },
      originalPrice: { value: amount }, currency: 'TRY' });
    const data = { product: { id: contentId, merchantListing: {
      merchant: { id: 999, name: 'Test rakip' }, winnerVariant: { price: price(150) },
      otherMerchants: [{ id: 105104, name: 'Test mağaza', inStock: true, price: price(160) }],
    } } };
    response.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    response.end(`<html><body><script>window["__envoy__SHARED_PROPS"] = ${JSON.stringify(data)};</script></body></html>`);
  });
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  const port = server.address().port;
  try {
    renderer = await launchRenderer();
    connection = await amqp.connect({ protocol: 'amqp', hostname: config.host, port: config.port,
      username: config.user, password: config.password, vhost: config.vhost });
    connection.on('error', () => {});
    const channel = await connection.createConfirmChannel();
    channel.on('error', () => {});
    await declareTopology(channel, config);
    await channel.prefetch(1);
    const handle = createWorker({ channel, config,
      render: (url, timeout) => renderer.render(`http://127.0.0.1:${port}${new URL(url).pathname}`, timeout) });
    let resolveJob;
    let rejectJob;
    const done = new Promise((resolve, reject) => { resolveJob = resolve; rejectJob = reject; });
    await channel.consume(config.requestQueue, (message) => {
      if (message) handle(message).then(resolveJob, rejectJob);
    }, { noAck: false });
    console.info('READY');
    await done;
    await channel.close();
  } finally {
    await connection?.close().catch(() => {});
    await renderer?.close();
    await new Promise((resolve) => server.close(resolve));
    await fs.rm(dir, { recursive: true, force: true });
  }
}

main().catch((error) => { console.error(error); process.exitCode = 1; });
