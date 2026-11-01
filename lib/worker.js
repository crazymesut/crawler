const crypto = require('node:crypto');

function remainingMs(payload, now = Date.now()) {
  return Math.max(0, Date.parse(payload.expiresAt) - now);
}

function validateJob(payload) {
  if (!payload || payload.version !== 1 || !/^[a-f0-9]{32}$/.test(payload.jobId || '')
    || !/^[1-9][0-9]{0,19}$/.test(payload.productContentId || '')
    || typeof payload.expiresAt !== 'string' || !Number.isFinite(Date.parse(payload.expiresAt))
    || !Number.isInteger(payload.attempt ?? 1) || (payload.attempt ?? 1) < 1) {
    throw new Error('Geçersiz tarama mesajı');
  }
  const url = new URL(payload.url);
  if (url.protocol !== 'https:' || url.hostname !== 'www.trendyol.com' || url.port || url.username || url.password
    || url.pathname !== `/pd/w/w-w-p-${payload.productContentId}` || url.search || url.hash) {
    throw new Error('Tarama URL ve içerik kimliği eşleşmiyor');
  }
}

function publishConfirmed(channel, queue, payload, ttlMs, timeoutMs = 10000) {
  return new Promise((resolve, reject) => {
    const messageId = crypto.randomUUID();
    let returned = false;
    let settled = false;
    const onReturn = (message) => {
      if (message.properties.messageId === messageId) returned = true;
    };
    const finish = (error) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      channel.removeListener('return', onReturn);
      if (error || returned) reject(error || new Error(`Mesaj kuyruğa yönlendirilemedi: ${queue}`));
      else resolve();
    };
    const timer = setTimeout(() => finish(new Error('Broker yayın onayı zaman aşımı')), timeoutMs);
    channel.on('return', onReturn);
    try {
      channel.sendToQueue(queue, Buffer.from(JSON.stringify(payload)), {
        persistent: true, mandatory: true, contentType: 'application/json', messageId,
        expiration: String(Math.max(1, Math.min(86400000, Math.floor(ttlMs)))),
      }, finish);
    } catch (error) {
      finish(error);
    }
  });
}

function createWorker({ channel, render, config, log = console, now = Date.now }) {
  const publish = (queue, payload, ttl) => publishConfirmed(channel, queue, payload, ttl, config.publishTimeoutMs);
  return async function handleMessage(message) {
    let payload;
    try {
      if (message.content.length > 16384) throw new Error('Tarama mesajı boyutu sınırı aşıldı');
      payload = JSON.parse(message.content.toString());
      validateJob(payload);
      if (remainingMs(payload, now()) > 86400000) throw new Error('Tarama expiry süresi sınırı aşıldı');
    } catch (error) {
      await publish(config.deadQueue, { error: error.message, preview: message.content.toString().slice(0, 1000),
        workerId: config.workerId, observedAt: new Date(now()).toISOString() }, 86400000);
      channel.ack(message);
      return;
    }
    if (remainingMs(payload, now()) <= 0) {
      log.info(`[${config.workerId}] Süresi dolmuş iş atlandı: ${payload.jobId}`);
      channel.ack(message);
      return;
    }

    const attempt = payload.attempt ?? 1;
    let result;
    let retryable = true;
    try {
      const rendered = await render(payload.url, Math.min(config.renderTimeoutMs, remainingMs(payload, now())));
      retryable = rendered.status === 403 || rendered.status === 429 || rendered.status >= 500;
      if (!Number.isInteger(rendered.status) || rendered.status < 200 || rendered.status >= 300) {
        const error = new Error(`Trendyol HTTP ${rendered.status}`);
        error.httpStatus = rendered.status;
        throw error;
      }
      if (typeof rendered.html !== 'string' || !rendered.html.length
        || Buffer.byteLength(rendered.html) > config.maxHtmlBytes) {
        retryable = false;
        throw new Error('Tarama HTML çıktısı boş veya boyut sınırını aşıyor');
      }
      result = { ...payload, success: true, status: rendered.status, html: rendered.html };
    } catch (error) {
      const delay = config.retryDelaysMs[attempt - 1];
      if (retryable && attempt < config.maxAttempts && delay !== undefined
        && remainingMs(payload, now()) > delay + 1000) {
        await publish(`${config.requestQueue}.retry.${delay}`, { ...payload, attempt: attempt + 1 }, remainingMs(payload, now()));
        channel.ack(message);
        log.warn(`[${config.workerId}] İş ${attempt + 1}. denemeye bırakıldı: ${payload.jobId}`);
        return;
      }
      result = { ...payload, success: false, status: error.httpStatus ?? null,
        error: String(error.message).slice(0, 1000) };
    }
    const remaining = remainingMs(payload, now());
    if (remaining > 0) {
      let envelope = { ...result, workerId: config.workerId, observedAt: new Date(now()).toISOString(), attempt };
      if (envelope.success && Buffer.byteLength(JSON.stringify(envelope)) > config.maxHtmlBytes * 2 + 65536) {
        envelope = { ...payload, success: false, status: result.status, error: 'Kodlanmış tarama çıktısı boyut sınırını aşıyor',
          workerId: config.workerId, observedAt: new Date(now()).toISOString(), attempt };
      }
      await publish(config.resultsQueue, envelope, remaining);
    }
    // Sonuç/retry broker tarafından kabul edilmeden tarama mesajı ACK edilmez.
    channel.ack(message);
  };
}

module.exports = { createWorker, publishConfirmed, validateJob, remainingMs };
