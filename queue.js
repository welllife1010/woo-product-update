const Bull = require('bull');
const { createClient } = require('redis'); // For 'redis' library

// Queue configuration options
const queueOptions = {
    redis: { port: 6379, host: '127.0.0.1' }, // Redis connection details
    //limiter: { max: 100, duration: 5000 }, // Rate limiting options
    defaultJobOptions: {
        removeOnComplete: true,
        removeOnFail: false,
        timeout: 300000, // Job timeout (in milliseconds)
        attempts: 3 // Number of retry attempts for failed jobs
    }
};

// Create a new queue for processing batches with options
const batchQueue = new Bull('batchQueue', queueOptions);

// Create a new Redis client
const redisClient = createClient({ url: 'redis://127.0.0.1:6379' });

(async () => {
    await redisClient.connect(); // Ensure the client is connected before using
})();

module.exports = {
    batchQueue,
    redisClient
};