const { test } = require('node:test');
const assert = require('node:assert/strict');
const { EventEmitter } = require('node:events');
const { createWorker, publishConfirmed } = require('../lib/worker');
const { loadConfig } = require('../lib/config');

class Channel extends EventEmitter {
  constructor() { super(); this.sent = []; this.acks = []; this.confirm = null; this.failure = null; }
  sendToQueue(queue, body, options, callback) {
    this.sent.push({ queue, payload: JSON.parse(body), options });
    if (this.failure) callback(this.failure);
    else if (this.confirm) this.confirm(callback, options);
    else queueMicrotask(() => callback(null));
    return true;
  }
  ack(message) { this.acks.push(message); }
}

const now = Date.parse('2026-11-01T12:00:00Z');
const payload = (overrides = {}) => ({ version: 1, jobId: 'a'.repeat(32), productContentId: '123', attempt: 1,
  url: 'https://www.trendyol.com/pd/w/w-w-p-123', expiresAt: new Date(now + 900000).toISOString(), ...overrides });
const message = (job = payload()) => ({ content: Buffer.from(JSON.stringify(job)) });
const config = () => loadConfig({});
const logger = { info() {}, warn() {} };

test('Sonuç broker tarafından onaylanmadan ACK verilmez', async () => {
  const channel = new Channel();
  let confirm;
  channel.confirm = (callback) => { confirm = callback; };
  const worker = createWorker({ channel, config: config(), now: () => now, log: logger,
    render: async () => ({ status: 200, html: '<html>product</html>' }) });
  const job = message();
  const running = worker(job);
  await new Promise(setImmediate);
  assert.equal(channel.acks.length, 0);
  assert.equal(channel.sent[0].queue, 'crawler_results');
  assert.equal(channel.sent[0].payload.success, true);
  assert.equal(channel.sent[0].options.persistent, true);
  assert.ok(Number(channel.sent[0].options.expiration) > 0);
  confirm(null);
  await running;
  assert.deepEqual(channel.acks, [job]);
});

test('Sonuç yayını başarısızsa tarama ACK edilmez ve hata yutulmaz', async () => {
  const channel = new Channel();
  channel.failure = new Error('broker unavailable');
  const worker = createWorker({ channel, config: config(), now: () => now, log: logger,
    render: async () => ({ status: 200, html: 'valid' }) });
  await assert.rejects(worker(message()), /broker unavailable/);
  assert.equal(channel.acks.length, 0);
  assert.equal(channel.sent.length, 1);
});

test('Geçici HTTP hatası gecikmeli retry kuyruğuna onaylı taşınır', async () => {
  const channel = new Channel();
  const worker = createWorker({ channel, config: config(), now: () => now, log: logger,
    render: async () => ({ status: 429, html: '' }) });
  await worker(message());
  assert.equal(channel.sent[0].queue, 'crawler_queue.retry.30000');
  assert.equal(channel.sent[0].payload.attempt, 2);
  assert.equal(channel.sent[0].payload.jobId, payload().jobId);
  assert.equal(channel.acks.length, 1);
});

test('Son denemede hata sonucu gönderilir; yeniden deneme sonsuza kadar sürmez', async () => {
  const channel = new Channel();
  const worker = createWorker({ channel, config: config(), now: () => now, log: logger,
    render: async () => { throw new Error('timeout'); } });
  await worker(message(payload({ attempt: 3 })));
  assert.equal(channel.sent[0].queue, 'crawler_results');
  assert.equal(channel.sent[0].payload.success, false);
  assert.equal(channel.sent[0].payload.error, 'timeout');
});

test('404 geçici hata gibi tekrar taranmaz; eski fiyatı koruyacak hata sonucu üretilir', async () => {
  const channel = new Channel();
  const worker = createWorker({ channel, config: config(), now: () => now, log: logger,
    render: async () => ({ status: 404, html: 'not found' }) });
  await worker(message());
  assert.equal(channel.sent[0].queue, 'crawler_results');
  assert.equal(channel.sent[0].payload.status, 404);
  assert.equal(channel.sent[0].payload.success, false);
});

test('Süresi dolan mesaj tarayıcı açılmadan kaldırılır', async () => {
  const channel = new Channel();
  let rendered = false;
  const worker = createWorker({ channel, config: config(), now: () => now, log: logger,
    render: async () => { rendered = true; } });
  await worker(message(payload({ expiresAt: new Date(now - 1).toISOString() })));
  assert.equal(rendered, false);
  assert.equal(channel.sent.length, 0);
  assert.equal(channel.acks.length, 1);
});

test('Bozuk JSON ve izinsiz URL yalnızca sınırlı dead-letter mesajı üretir', async () => {
  const channel = new Channel();
  const worker = createWorker({ channel, config: config(), now: () => now, log: logger,
    render: async () => { throw new Error('render must not run'); } });
  await worker({ content: Buffer.from('invalid json') });
  await worker(message(payload({ url: 'http://127.0.0.1/private' })));
  assert.equal(channel.sent.length, 2);
  assert.ok(channel.sent.every((sent) => sent.queue === 'crawler_dead'));
  assert.equal(channel.acks.length, 2);
});

test('Büyük HTML kuyrukta birikmez; yalnızca hata sonucu gönderilir', async () => {
  const channel = new Channel();
  const worker = createWorker({ channel, config: { ...config(), maxHtmlBytes: 10 }, now: () => now, log: logger,
    render: async () => ({ status: 200, html: 'x'.repeat(11) }) });
  await worker(message());
  assert.equal(channel.sent[0].payload.success, false);
  assert.equal(channel.sent[0].payload.html, undefined);
});

test('JSON kaçışlarıyla büyüyen çıktı broker boyut sınırını zorlamaz', async () => {
  const channel = new Channel();
  const worker = createWorker({ channel, config: { ...config(), maxHtmlBytes: 100000 }, now: () => now, log: logger,
    render: async () => ({ status: 200, html: '\0'.repeat(100000) }) });
  await worker(message());
  assert.equal(channel.sent[0].payload.success, false);
  assert.equal(channel.sent[0].payload.html, undefined);
  assert.match(channel.sent[0].payload.error, /Kodlanmış/);
});

test('Mandatory dönüşü, broker ACK verse de başarısız yayın sayılır', async () => {
  const channel = new Channel();
  channel.confirm = (callback, options) => {
    channel.emit('return', { properties: { messageId: options.messageId } });
    callback(null);
  };
  await assert.rejects(publishConfirmed(channel, 'missing', {}, 1000), /yönlendirilemedi/);
  assert.equal(channel.listenerCount('return'), 0);
});

test('Kanal başına ACK kullanılır; eski session yeni session mesajını onaylamaz', async () => {
  const old = new Channel();
  const fresh = new Channel();
  const worker = createWorker({ channel: old, config: config(), now: () => now, log: logger,
    render: async () => ({ status: 200, html: 'valid' }) });
  await worker(message());
  assert.equal(old.acks.length, 1);
  assert.equal(fresh.acks.length, 0);
});

test('Süresiz veya aşırı paralel çalışma ayarları reddedilir', () => {
  assert.throws(() => loadConfig({ RENDER_TIMEOUT_MS: '0' }));
  assert.throws(() => loadConfig({ PREFETCH_COUNT: '0' }));
  assert.throws(() => loadConfig({ MAX_ATTEMPTS: '99' }));
  assert.throws(() => loadConfig({ RABBITMQ_RESULTS_QUEUE: 'crawler_queue' }));
});
