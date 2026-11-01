const os = require('node:os');

function integer(env, name, fallback, min, max) {
  const raw = env[name] ?? String(fallback);
  const value = Number(raw);
  if (!/^\d+$/.test(String(raw)) || !Number.isSafeInteger(value) || value < min || value > max) {
    throw new Error(`${name} ${min}-${max} arasında tam sayı olmalı`);
  }
  return value;
}

function loadConfig(env = process.env) {
  const requestQueue = env.RABBITMQ_QUEUE || 'crawler_queue';
  const resultsQueue = env.RABBITMQ_RESULTS_QUEUE || 'crawler_results';
  const deadQueue = env.RABBITMQ_DEAD_QUEUE || 'crawler_dead';
  const retryDelaysMs = (env.RETRY_DELAYS_MS ?? '30000,120000').split(',').map((value) => {
    if (!/^\d+$/.test(value) || Number(value) < 1 || Number(value) > 600000) throw new Error('Geçersiz retry gecikmesi');
    return Number(value);
  });
  for (const queue of [requestQueue, resultsQueue, deadQueue]) {
    if (Buffer.byteLength(queue) > 200 || queue.startsWith('amq.') || !/^[a-zA-Z0-9_.-]+$/.test(queue)) {
      throw new Error('Geçersiz RabbitMQ kuyruk adı');
    }
  }
  if (new Set([requestQueue, resultsQueue, deadQueue]).size !== 3) throw new Error('Kuyruk adları farklı olmalı');
  return {
    requestQueue, resultsQueue, deadQueue, retryDelaysMs,
    workerId: env.WORKER_ID || `${os.hostname()}:${process.pid}`,
    host: env.RABBITMQ_HOST || 'localhost', port: integer(env, 'RABBITMQ_PORT', 5672, 1, 65535),
    user: env.RABBITMQ_USER || 'guest', password: env.RABBITMQ_PASSWORD || 'guest',
    vhost: env.RABBITMQ_VHOST || 'crawler',
    portHttp: integer(env, 'PORT', 3000, 1, 65535),
    renderTimeoutMs: integer(env, 'RENDER_TIMEOUT_MS', 60000, 1000, 120000),
    publishTimeoutMs: integer(env, 'PUBLISH_TIMEOUT_MS', 10000, 1000, 30000),
    reconnectDelayMs: integer(env, 'RECONNECT_DELAY_MS', 5000, 100, 60000),
    prefetch: integer(env, 'PREFETCH_COUNT', 1, 1, 4),
    maxAttempts: integer(env, 'MAX_ATTEMPTS', 3, 1, retryDelaysMs.length + 1),
    maxHtmlBytes: integer(env, 'MAX_HTML_BYTES', 4194304, 1024, 4194304),
  };
}

module.exports = { loadConfig };
