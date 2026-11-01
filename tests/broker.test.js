const { test } = require('node:test');
const assert = require('node:assert/strict');
const crypto = require('node:crypto');
const amqp = require('amqplib');
const { loadConfig } = require('../lib/config');
const { declareTopology } = require('../lib/topology');
const { createWorker, publishConfirmed } = require('../lib/worker');

test('On worker ortak kuyrukta on işi paylaşır ve on tekil sonuç üretir', {
  skip: !process.env.TY_CRAWLER_TEST_RABBITMQ_PORT, timeout: 15000,
}, async () => {
  const suffix = crypto.randomBytes(6).toString('hex');
  const config = loadConfig({ RABBITMQ_QUEUE: `ty-node-${suffix}`, RABBITMQ_RESULTS_QUEUE: `ty-node-results-${suffix}`,
    RABBITMQ_DEAD_QUEUE: `ty-node-dead-${suffix}` });
  const connection = await amqp.connect({ protocol: 'amqp', hostname: '127.0.0.1',
    port: Number(process.env.TY_CRAWLER_TEST_RABBITMQ_PORT), username: 'crawltest', password: 'crawltest', vhost: 'crawler' });
  const admin = await connection.createConfirmChannel();
  const channels = [];
  try {
    await declareTopology(admin, config);
    const done = [];
    for (let index = 0; index < 10; index++) {
      const channel = await connection.createConfirmChannel();
      channels.push(channel);
      await channel.prefetch(1);
      const handle = createWorker({ channel, config: { ...config, workerId: `worker-${index}` },
        render: async () => {
          await new Promise((resolve) => setTimeout(resolve, 10));
          return { status: 200, html: '<html>fixture</html>' };
        } });
      let finish;
      let fail;
      done.push(new Promise((resolve, reject) => { finish = resolve; fail = reject; }));
      await channel.consume(config.requestQueue, (message) => { if (message) handle(message).then(finish, fail); }, { noAck: false });
    }
    const expiresAt = new Date(Date.now() + 60000).toISOString();
    await Promise.all(Array.from({ length: 10 }, (_, index) => publishConfirmed(admin, config.requestQueue, {
      version: 1, jobId: crypto.randomBytes(16).toString('hex'), productContentId: String(index + 1),
      url: `https://www.trendyol.com/pd/w/w-w-p-${index + 1}`, attempt: 1, expiresAt,
    }, 60000)));
    await Promise.all(done);
    const results = [];
    for (let index = 0; index < 10; index++) {
      const message = await admin.get(config.resultsQueue, { noAck: true });
      assert.ok(message);
      results.push(JSON.parse(message.content));
    }
    assert.equal(new Set(results.map((result) => result.jobId)).size, 10);
    assert.equal(new Set(results.map((result) => result.workerId)).size, 10);
    assert.ok(results.every((result) => result.success));
    assert.equal((await admin.checkQueue(config.requestQueue)).messageCount, 0);
  } finally {
    for (const channel of channels) await channel.close();
    for (const queue of [config.requestQueue, config.resultsQueue, config.deadQueue,
      ...config.retryDelaysMs.map((delay) => `${config.requestQueue}.retry.${delay}`)]) {
      await admin.deleteQueue(queue);
    }
    await admin.close();
    await connection.close();
  }
});

test('Geçici tarama hatası gerçek gecikmeli kuyruktan dönüp tamamlanır', {
  skip: !process.env.TY_CRAWLER_TEST_RABBITMQ_PORT, timeout: 15000,
}, async () => {
  const suffix = crypto.randomBytes(6).toString('hex');
  const config = loadConfig({ RABBITMQ_QUEUE: `ty-retry-${suffix}`, RABBITMQ_RESULTS_QUEUE: `ty-retry-results-${suffix}`,
    RABBITMQ_DEAD_QUEUE: `ty-retry-dead-${suffix}`, RETRY_DELAYS_MS: '50,100' });
  const connection = await amqp.connect({ protocol: 'amqp', hostname: '127.0.0.1',
    port: Number(process.env.TY_CRAWLER_TEST_RABBITMQ_PORT), username: 'crawltest', password: 'crawltest', vhost: 'crawler' });
  const channel = await connection.createConfirmChannel();
  let calls = 0;
  try {
    await declareTopology(channel, config);
    await channel.prefetch(1);
    const handle = createWorker({ channel, config, log: { info() {}, warn() {} }, render: async () => {
      calls++;
      return calls === 1 ? { status: 503, html: '' } : { status: 200, html: 'product' };
    } });
    let complete;
    let failed;
    const done = new Promise((resolve, reject) => { complete = resolve; failed = reject; });
    await channel.consume(config.requestQueue, (message) => {
      if (message) handle(message).then(() => { if (calls === 2) complete(); }, failed);
    }, { noAck: false });
    await publishConfirmed(channel, config.requestQueue, { version: 1,
      jobId: crypto.randomBytes(16).toString('hex'), productContentId: '123', attempt: 1,
      url: 'https://www.trendyol.com/pd/w/w-w-p-123', expiresAt: new Date(Date.now() + 60000).toISOString(),
    }, 60000);
    await done;
    const result = await channel.get(config.resultsQueue, { noAck: true });
    assert.ok(result);
    assert.equal(JSON.parse(result.content).attempt, 2);
    assert.equal(JSON.parse(result.content).success, true);
    assert.equal(calls, 2);
  } finally {
    await channel.deleteQueue(config.requestQueue);
    await channel.deleteQueue(config.resultsQueue);
    await channel.deleteQueue(config.deadQueue);
    for (const delay of config.retryDelaysMs) await channel.deleteQueue(`${config.requestQueue}.retry.${delay}`);
    await channel.close();
    await connection.close();
  }
});
