require('dotenv').config();

const express = require('express');
const amqp = require('amqplib');
const { once } = require('node:events');
const { loadConfig } = require('./lib/config');
const { createWorker } = require('./lib/worker');
const { launchRenderer } = require('./lib/renderer');
const { declareTopology } = require('./lib/topology');

async function start() {
  const config = loadConfig();
  const renderer = await launchRenderer();
  let stopping = false;
  let ready = false;
  let connection;
  const active = new Set();
  const app = express();
  app.disable('x-powered-by');
  app.get('/health', (req, res) => {
    const healthy = ready && renderer.connected() && !stopping;
    res.status(healthy ? 200 : 503).json({ status: healthy ? 'ok' : 'starting', workerId: config.workerId,
      active: active.size });
  });
  const server = app.listen(config.portHttp);
  renderer.onDisconnected(() => {
    ready = false;
    connection?.close().catch(() => {});
  });
  const shutdown = () => {
    if (stopping) return;
    stopping = true;
    ready = false;
    server.close();
    // Broker bağlantısı kapanınca ACK edilmeyen iş başka makineye verilebilir.
    connection?.close().catch(() => {});
  };
  process.on('SIGTERM', shutdown);
  process.on('SIGINT', shutdown);
  try {
    while (!stopping && renderer.connected()) {
      try {
        connection = await amqp.connect({ protocol: 'amqp', hostname: config.host, port: config.port,
          username: config.user, password: config.password, vhost: config.vhost, heartbeat: 20 }, { timeout: 10000 });
        const session = connection;
        session.on('error', (error) => console.error('[RabbitMQ]', error.message));
        const closed = once(session, 'close').catch(() => {});
        const channel = await session.createConfirmChannel();
        channel.on('error', (error) => console.error('[Kanal]', error.message));
        channel.on('close', () => { ready = false; session.close().catch(() => {}); });
        await declareTopology(channel, config);
        await channel.prefetch(config.prefetch);
        const handle = createWorker({ channel, render: renderer.render, config });
        await channel.consume(config.requestQueue, (message) => {
          if (!message) { session.close().catch(() => {}); return; }
          const task = handle(message).catch((error) => {
            // Yayın belirsizse ACK vermeyiz; aynı session kapatılır ve iş yeniden teslim edilir.
            console.error('[İş]', error.message);
            ready = false;
            return session.close().catch(() => {});
          }).finally(() => active.delete(task));
          active.add(task);
        }, { noAck: false });
        ready = true;
        console.info(`[${config.workerId}] ${config.requestQueue} dinleniyor`);
        await closed;
      } catch (error) {
        console.error('[Bağlantı]', error.message);
      } finally {
        ready = false;
        await connection?.close().catch(() => {});
        // Eski session'daki işler bitmeden yeni session açılmaz; ACK yanlış kanala gitmez.
        await Promise.allSettled([...active]);
      }
      if (!stopping) await new Promise((resolve) => setTimeout(resolve, config.reconnectDelayMs));
    }
  } finally {
    server.close();
    await renderer.close();
    process.removeListener('SIGTERM', shutdown);
    process.removeListener('SIGINT', shutdown);
  }
  if (!stopping) throw new Error('Tarayıcı bağlantısı kapandı; süreç yeniden başlatılmalı');
}

if (require.main === module) {
  start().catch((error) => { console.error('[Başlangıç]', error.message); process.exitCode = 1; });
}

module.exports = { start };
