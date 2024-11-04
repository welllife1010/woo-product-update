require("dotenv").config();
const { logger, logErrorToFile } = require("./logger");

//const Bull = require('bull');
const batchQueue = require('./queue'); // Importing batchQueue directly
const { processBatch } = require('./batch-helpers'); 
//const batchQueue = new Bull('batchQueue', { redis: { port: 6379, host: '127.0.0.1' } });

const WooCommerceRestApi = require("woocommerce-rest-ts-api").default;

const wooApi = new WooCommerceRestApi({
    url: process.env.WOO_API_BASE_URL_DEV,
    consumerKey: process.env.WOO_API_CONSUMER_KEY_DEV,
    consumerSecret: process.env.WOO_API_CONSUMER_SECRET_DEV,
    version: "wc/v3",
});

async function testConnection() {
    try {
        const response = await wooApi.get("products", { per_page: 1 });
        console.log("Connection successful:", response.data);
    } catch (error) {
        console.error("Failed to connect to WooCommerce API:", error.message);
    }
}

//testConnection();

const testJob = async () => {
    try {
        logger.info('Running test job');
        // Mock a simple process to see if the worker runs without failure
        await new Promise(resolve => setTimeout(resolve, 1000));  // Mock 1-second delay
        logger.info('Test job completed successfully');
    } catch (error) {
        logger.error(`Test job error: ${error.message}`);
    }
};

//testJob();

// Define the worker to process each batch
batchQueue.process(async (job) => {
    logger.info(`Worker received job ID: ${job.id}`);
    const { batch, fileKey, totalProducts } = job.data;

    if (!batch || !fileKey) {
        logger.error(`Job data or batch is missing for job ID: ${job.id}. Data: ${JSON.stringify(job.data)}`);
        return;
    }

    try {
        logger.info(`Processing batch for job ID: ${job.id}, file: ${fileKey}`);
        await processBatch(batch, 0, totalProducts, fileKey);
        logger.info(`Successfully processed batch for job ID: ${job.id}`);
    } catch (error) {
        logger.error(`Error processing job ID ${job.id}: ${error.message}`);
        logErrorToFile(`Job failed with error: ${error.message}`);
        throw error; // This allows Bull to retry the job
    }
});

// Event listeners for job statuses
batchQueue.on('active', (job) => {
    logger.info(`Job is now active: ${job.id}`);
});

batchQueue.on('waiting', (jobId) => {
    logger.info(`Job waiting to be processed: ${jobId}`);
});

batchQueue.on('completed', (job, result) => {
    logger.info(`Job completed with ID ${job.id}`);
});

batchQueue.on('failed', (job, err) => {
    logger.error(`Job failed with ID ${job.id}: ${err.message}`);
});

batchQueue.on('error', (error) => {
    logger.error(`Redis connection error: ${error.message}`);
});
