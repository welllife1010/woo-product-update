require("dotenv").config();
const { performance } = require("perf_hooks");
const { logger, logErrorToFile, logUpdatesToFile, logInfoToFile, logProgressToFile } = require("./logger");
const { batchQueue, redisClient } = require('./queue'); // Importing batchQueue directly
const { processBatch } = require('./batch-helpers'); 
const { saveCheckpoint, getCheckpoint } = require('./checkpoint'); 

// Initialize dynamic concurrency and batch size
let concurrency = parseInt(process.env.CONCURRENCY) || 2;
let batchSize = parseInt(process.env.BATCH_SIZE) || 10;

// Graceful shutdown handling
const gracefulShutdown = async () => {
    console.log("Received shutdown signal. Cleaning up...");
    clearInterval(shutdownCheckInterval);
    try {
        const allProcessed = await checkAllFilesProcessed();
        if (!allProcessed) {
            console.log("Not all jobs processed. Progress will resume on restart.");
        }
        await redisClient.quit(); // Disconnect from Redis
        console.log("Shutdown complete.");
    } catch (error) {
        console.error("Error during shutdown:", error.message);
    } finally {
        process.exit(0);
    }
};

process.on("SIGINT", gracefulShutdown); // Handle Ctrl+C
process.on("SIGTERM", gracefulShutdown); // Handle termination signals

// Check if all files have been processed
const checkAllFilesProcessed = async () => {
    try {
        const fileKeys = await redisClient.keys("total-rows:*"); // Get all file keys for processing

        for (const key of fileKeys) {
            const fileKey = key.split(":")[1];
            const totalRows = parseInt(await redisClient.get(`total-rows:${fileKey}`), 10);
            const successfulUpdates = parseInt(await redisClient.get(`updated-products:${fileKey}`) || 0, 10);
            const failedUpdates = parseInt(await redisClient.get(`failed-products:${fileKey}`) || 0, 10);
            const skippedUpdates = parseInt(await redisClient.get(`skipped-products:${fileKey}`) || 0, 10);

            // Check if the sum of successful, failed, and skipped updates matches total rows
            if (successfulUpdates + failedUpdates + skippedUpdates < totalRows) {
                return false; // Processing still ongoing
            }
        }
        return true; // All files processed
    } catch (error) {
        logger.error(`Error checking file processing status: ${error.message}`);
        return false;
    }
};

const shutdownCheckInterval = setInterval(async () => {
    const allProcessed = await checkAllFilesProcessed();
    if (allProcessed) {
        clearInterval(shutdownCheckInterval); // Stop checking
        console.log("All products across all files processed. Shutting down gracefully...");
        process.exit(0); // Shut down the process
    }
}, 5000); // Check every 5 seconds

// Retry logic with exponential backoff for transient errors
const retryWithBackoff = async (fn, retries = 3, delay = 1000) => {
    let attempt = 0;
    while (attempt < retries) {
        try {
            return await fn(); // Attempt the function
        } catch (error) {
            attempt++;
            if (attempt >= retries) throw error; // Rethrow after max retries
            const backoff = delay * Math.pow(2, attempt); // Exponential backoff
            console.log(`Retrying after ${backoff}ms...`);
            await new Promise(resolve => setTimeout(resolve, backoff));
        }
    }
};

// Adjust concurrency and batch size based on performance
const adjustConcurrencyAndBatchSize = (jobTime) => {
    if (jobTime > 2000) {
        batchSize = Math.max(5, batchSize - 1); // Reduce batch size if processing is slow
        concurrency = Math.max(1, concurrency - 1); // Reduce concurrency
    } else {
        batchSize = Math.min(20, batchSize + 1); // Increase batch size if processing is fast
        concurrency = Math.min(10, concurrency + 1); // Increase concurrency
    }
    console.log(`Adjusted concurrency: ${concurrency}, batch size: ${batchSize}`);
};

// Define the worker (queue) to process each job (batch)
batchQueue.process( concurrency, async (job) => { // This will allow up to '2' concurrent job processes
    const queueStartTime = performance.now();
    logger.info(`Starting job ID ${job.id} for file ${job.data.fileKey}`);

    const { batch, fileKey, totalProductsInFile } = job.data;

    if (!batch || !fileKey || !totalProductsInFile) {
        logger.error(`Invalid job data: ${JSON.stringify(job.data)}`);
        throw new Error("Invalid job data");
    }

    // Fetch the checkpoint or start from 0 if this is the first run
    let lastProcessedRow = (await getCheckpoint(fileKey)) || 0; // Fallback to 0 if undefined
    logInfoToFile(`Last processed row for file ${fileKey} is ${lastProcessedRow}. Starting job.`);

    // Update the last processed row after batch processing
    let updatedLastProcessedRow = lastProcessedRow;

    try {
        logger.info(`Processing batch for job ID: ${job.id} | File: ${fileKey}`);

        // *** Process the batch ***
        await retryWithBackoff(() => processBatch(batch, lastProcessedRow, totalProductsInFile, fileKey));

        // Update the last processed row after batch processing
        updatedLastProcessedRow = lastProcessedRow + batch.length;

        // Save checkpoint in Redis
        await redisClient.set(`lastProcessedRow:${fileKey}`, updatedLastProcessedRow);

        // Save checkpoint in local JSON
        await saveCheckpoint(fileKey, updatedLastProcessedRow, totalProductsInFile, batch);

        const queueEndTime = performance.now();
        adjustConcurrencyAndBatchSize(queueEndTime - queueStartTime);
        logger.info(`Job ID ${job.id} completed in ${(queueEndTime - queueStartTime).toFixed(2)}ms.`);
        //const queueDuration = ((queueEndTime - queueStartTime) / 1000).toFixed(2);
        //logInfoToFile(`Job ID ${job.id} completed in ${queueDuration} seconds`);

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

const sendNotification = async (message) => {
    await axios.post(process.env.SLACK_WEBHOOK_URL, { text: message });
};

// Event listener for failed jobs in batchQueue
batchQueue.on("failed", async (job, err) => {
    const retryCount = job.attemptsMade;
    const maxRetries = job.opts.attempts || 3;

    if (retryCount >= maxRetries) {
        await sendNotification(`Job ${job.id} permanently failed: ${err.message}`);
    }
});

// Event listener for failed jobs in batchQueue
// batchQueue.on("failed", (job, err) => {
//     const retryCount = job.attemptsMade; // attemptsMade counts the current attempt number
//     const maxRetries = job.opts.attempts || 3; // Get max retry attempts, defaulting to 3 if undefined

//     logErrorToFile(`Job failed with ID ${job.id} on attempt ${retryCount}/${maxRetries} | File: ${job.data.fileKey} | Error: ${err.message}`, err.stack);

//     // Check if job will retry or is permanently failed
//     if (retryCount < maxRetries) {
//         logInfoToFile(`Retrying job ${job.id} for file "${job.data.fileKey}". Next attempt: ${retryCount + 1}`);
//     } else {
//         logErrorToFile(`Job ${job.id} permanently failed after ${maxRetries} attempts.`);
//     }
// });

batchQueue.on('error', (error) => {
    logErrorToFile(`Redis connection error: ${error.message}`, error.stack);
});