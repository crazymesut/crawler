async function declareTopology(channel, config) {
  await channel.assertQueue(config.requestQueue, { durable: true });
  await channel.assertQueue(config.resultsQueue, { durable: true });
  await channel.assertQueue(config.deadQueue, { durable: true, arguments: {
    'x-message-ttl': 86400000, 'x-max-length': 1000, 'x-max-length-bytes': 16777216, 'x-overflow': 'reject-publish',
  } });
  for (const delay of config.retryDelaysMs) {
    await channel.assertQueue(`${config.requestQueue}.retry.${delay}`, { durable: true, arguments: {
      'x-message-ttl': delay, 'x-dead-letter-exchange': '', 'x-dead-letter-routing-key': config.requestQueue,
    } });
  }
}

module.exports = { declareTopology };
