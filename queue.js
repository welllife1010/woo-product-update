const Bull = require('bull');

// Create a new queue for processing batches
const batchQueue = new Bull('batchQueue', {
    redis: { port: 6379, host: '127.0.0.1' }
});

module.exports = batchQueue;