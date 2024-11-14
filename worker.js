require("dotenv").config();
const { logger, logErrorToFile, logUpdatesToFile, logInfoToFile, logFileProgress } = require("./logger");
const { batchQueue, redisClient } = require('./queue'); // Importing batchQueue directly
const { processBatch } = require('./batch-helpers'); 
const { saveCheckpoint } = require('./checkpoint'); 

// Define a function to check if all files have been processed
const checkAllFilesProcessed = async () => {
    const fileKeys = await redisClient.keys('total-rows:*'); // Get all file keys for processing

    for (const key of fileKeys) {
        const fileKey = key.split(":")[1];
        const totalRows = parseInt(await redisClient.get(`total-rows:${fileKey}`), 10);
        const successfulUpdates = parseInt(await redisClient.get(`updated-products:${fileKey}`) || 0, 10);
        const failedUpdates = parseInt(await redisClient.get(`failed-products:${fileKey}`) || 0, 10);

        if (successfulUpdates + failedUpdates < totalRows) {
            return false; // Still processing rows in this file
        }
    }
    return true; // All files fully processed
};

// Set up an interval to check for completion and shut down
const shutdownCheckInterval = setInterval(async () => {
    const allProcessed = await checkAllFilesProcessed();
    if (allProcessed) {
        clearInterval(shutdownCheckInterval); // Stop checking
        console.log("All products across all files processed. Shutting down gracefully...");
        process.exit(0); // Shut down the process
    }
}, 5000); // Check every 5 seconds

// Define the worker to process each job (batch)
batchQueue.process( 4, async (job) => { // This will allow up to 4 concurrent job processes
    logger.info(`Worker received job ID: ${job.id}`);
    const { batch, fileKey, totalProductsInFile, lastProcessedRow, batchSize } = job.data;

    if (!batch || !fileKey || !totalProductsInFile || !lastProcessedRow || !batchSize) {
        logErrorToFile(`Job data or batch is missing for job ID: ${job.id}.`);
        logErrorToFile(`Job ID: ${job.id} | Total products in file: ${totalProductsInFile} | Last processed row: ${lastProcessedRow} | Batch size: ${batchSize}`);
        logErrorToFile(`Failed Data: ${JSON.stringify(job.data)}`);
        return;
    }

    logInfoToFile(`Processing job ID: ${job.id} for file: ${job.data.fileKey}`);
    logInfoToFile(`Total products in file: ${job.data.totalProductsInFile}`);
    logInfoToFile(`Last processed row: ${job.data.lastProcessedRow}`);

    let updatedLastProcessedRow;

    try {
        logger.info(`Processing batch for job ID: ${job.id} | File: ${fileKey}`);

        // *** Process the batch ***
        await processBatch(batch, lastProcessedRow, totalProductsInFile, fileKey);

        // Update the last processed row after batch processing
        updatedLastProcessedRow = lastProcessedRow + batch.length;

        // Save checkpoint in Redis and local JSON file
        await redisClient.set(`lastProcessedRow:${fileKey}`, updatedLastProcessedRow);
        await saveCheckpoint(fileKey, updatedLastProcessedRow, totalProductsInFile, batch);  // Save to local JSON checkpoint file

        // Log progress after processing the batch
        await logFileProgress(fileKey);

        logInfoToFile(`Successfully processed batch for job ID: ${job.id} | File: ${fileKey} | Last processed row: ${updatedLastProcessedRow} / ${totalProductsInFile}`);
    } catch (error) {

        // Check for timeout error and log details
        if (error.message.includes('Promise timed out')) {
            logErrorToFile(`Timeout error for job ID ${job.id} | File: ${fileKey} | Last processed row: ${updatedLastProcessedRow} / ${totalProductsInFile} | Error: ${error.message}`);
            // Optionally, you can throw the error to allow Bull to retry the job
            throw error;
        } else {
            logErrorToFile(`Job failed with ID ${job.id} | File: ${fileKey} | Last processed row: ${updatedLastProcessedRow} / ${totalProductsInFile} | Error: ${error.message}`, error.stack);
            throw error; // Re-throw to trigger retry
        }
    }
});

// Event listeners for job statuses
batchQueue.on('active', (job) => {
    logUpdatesToFile(`Job is now active: ${job.id} | File: ${job.data.fileKey} | Last processed row: ${job.data.lastProcessedRow}`);
});

batchQueue.on('waiting', (jobId) => {
    logInfoToFile(`Job waiting to be processed: ${jobId}`);
});

batchQueue.on('completed', (job, result) => {
    logInfoToFile(`Job completed with ID ${job.id} | Result: ${result} | File: ${job.data.fileKey} | Last processed row: ${job.data.lastProcessedRow}`);
});

batchQueue.on('failed', (job, err) => {
    logErrorToFile(`Job failed with ID ${job.id} | File: ${job.data.fileKey}: ${err.message}`, err.stack);
});

batchQueue.on('error', (error) => {
    logErrorToFile(`Redis connection error: ${error.message}`, error.stack);
});