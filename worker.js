require("dotenv").config();
const { performance } = require("perf_hooks");
const { logger, logErrorToFile, logUpdatesToFile, logInfoToFile, logProgressToFile } = require("./logger");
const { batchQueue, redisClient } = require('./queue'); // Importing batchQueue directly
const { processBatch } = require('./batch-helpers'); 
const { saveCheckpoint, getCheckpoint } = require('./checkpoint'); 

// Check if all files have been processed
const checkAllFilesProcessed = async () => {
    const fileKeys = await redisClient.keys('total-rows:*'); // Get all file keys for processing

    for (const key of fileKeys) {
        const fileKey = key.split(":")[1];
        const totalRows = parseInt(await redisClient.get(`total-rows:${fileKey}`), 10);
        const successfulUpdates = parseInt(await redisClient.get(`updated-products:${fileKey}`) || 0, 10);
        const failedUpdates = parseInt(await redisClient.get(`failed-products:${fileKey}`) || 0, 10);
        const skippedUpdates = parseInt(await redisClient.get(`skipped-products:${fileKey}`) || 0, 10);

        // Check if the sum of successful, failed, and skipped updates matches total rows
        if (successfulUpdates + failedUpdates + skippedUpdates < totalRows) {
            return false; // Still processing rows in this file
        }
    }
    return true; // All files fully processed
};

const shutdownCheckInterval = setInterval(async () => {
    const allProcessed = await checkAllFilesProcessed();
    if (allProcessed) {
        clearInterval(shutdownCheckInterval); // Stop checking
        console.log("All products across all files processed. Shutting down gracefully...");
        process.exit(0); // Shut down the process
    }
}, 5000); 

// Define the worker (queue) to process each job (batch)
batchQueue.process( 2, async (job) => { // This will allow up to 2 concurrent job processes
    const queueStartTime = performance.now();
    logger.info(`Starting job ID ${job.id} for file ${job.data.fileKey}`);

    const { batch, fileKey, totalProductsInFile, batchSize } = job.data;

    if (!batch || !fileKey || !totalProductsInFile || !batchSize) {
        logErrorToFile(`Job data or batch is missing for job ID: ${job.id}.`);
        logErrorToFile(`Job ID: ${job.id} | Total products in file: ${totalProductsInFile} | Batch size: ${batchSize}`);
        logErrorToFile(`Failed Data: ${JSON.stringify(job.data)}`);
        return;
    }

     // Fetch the checkpoint or start from 0 if this is the first run
     let lastProcessedRow = (await getCheckpoint(fileKey)) || 0; // Fallback to 0 if undefined
     logInfoToFile(`Last processed row for file ${fileKey} is ${lastProcessedRow}. Starting job.`);

    logInfoToFile(`Processing job ID: ${job.id} for file: ${job.data.fileKey}`);
    logInfoToFile(`Total products in file: ${job.data.totalProductsInFile}`);
    logInfoToFile(`Last processed row: ${lastProcessedRow}`);

    try {
        logger.info(`Processing batch for job ID: ${job.id} | File: ${fileKey}`);

        // *** Process the batch ***
        await processBatch(batch, lastProcessedRow, totalProductsInFile, fileKey);

        const queueEndTime = performance.now();
        const queueDuration = ((queueEndTime - queueStartTime) / 1000).toFixed(2);
        logInfoToFile(`Job ID ${job.id} completed in ${queueDuration} seconds`);

        // Update the last processed row after batch processing
        let updatedLastProcessedRow = lastProcessedRow + batch.length;

        // Save checkpoint in Redis and local JSON file
        await redisClient.set(`lastProcessedRow:${fileKey}`, updatedLastProcessedRow);
        await saveCheckpoint(fileKey, updatedLastProcessedRow, totalProductsInFile, batch);  // Save to local JSON checkpoint file

        // Log progress after processing the batch
        await logProgressToFile(fileKey);

        logInfoToFile(`Successfully processed batch for job ID: ${job.id} | File: ${fileKey} | Last processed row: ${updatedLastProcessedRow} / ${totalProductsInFile}`);
    } catch (error) {

        // Check for timeout error and log details
        if (error.message.includes('Promise timed out') || error.message.includes('timeout')) {
            logErrorToFile(`Timeout error for job ID ${job.id} | File: ${fileKey} | Last processed row: ${updatedLastProcessedRow} / ${totalProductsInFile} | Error: ${error.message}`);
            // Optionally, you can throw the error to allow Bull to retry the job
            throw error;
        } else {
            logErrorToFile(`Job failed with ID ${job.id} in "batchQueue" process | File: ${fileKey} | Last processed row: ${updatedLastProcessedRow} / ${totalProductsInFile} | Error: ${error.message}`, error.stack);
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

// Event listener for failed jobs in batchQueue
batchQueue.on("failed", (job, err) => {
    const retryCount = job.attemptsMade; // attemptsMade counts the current attempt number
    const maxRetries = job.opts.attempts || 3; // Get max retry attempts, defaulting to 3 if undefined

    logErrorToFile(`Job failed with ID ${job.id} on attempt ${retryCount}/${maxRetries} | File: ${job.data.fileKey} | Error: ${err.message}`, err.stack);

    // Check if job will retry or is permanently failed
    if (retryCount < maxRetries) {
        logInfoToFile(`Retrying job ${job.id} for file "${job.data.fileKey}". Next attempt: ${retryCount + 1}`);
    } else {
        logErrorToFile(`Job ${job.id} permanently failed after ${maxRetries} attempts.`);
    }
});

batchQueue.on('error', (error) => {
    logErrorToFile(`Redis connection error: ${error.message}`, error.stack);
});