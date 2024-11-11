require("dotenv").config();
const { logger, logErrorToFile, logUpdatesToFile, logInfoToFile, logFileProgress } = require("./logger");
const { batchQueue, redisClient } = require('./queue'); // Importing batchQueue directly
const { processBatch } = require('./batch-helpers'); 

logInfoToFile('Worker process is running and listening for jobs...');
// Define the worker to process each job (batch)
batchQueue.process( 5, async (job) => { // This will allow up to 5 concurrent job processes
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
        await redisClient.set(`lastProcessedRow:${fileKey}`, updatedLastProcessedRow);

        // Log progress after processing the batch
        await logFileProgress(fileKey);

        logger.info(`Successfully processed batch for job ID: ${job.id} | File: ${fileKey} | Last processed row: ${updatedLastProcessedRow} / ${totalProductsInFile}`);
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
    logUpdatesToFile(`Job waiting to be processed: ${jobId}`);
});

batchQueue.on('completed', (job, result) => {
    logUpdatesToFile(`Job completed with ID ${job.id} | Result: ${result} | File: ${job.data.fileKey} | Last processed row: ${job.data.lastProcessedRow}`);
});

batchQueue.on('failed', (job, err) => {
    logErrorToFile(`Job failed with ID ${job.id} | File: ${job.data.fileKey}: ${err.message}`, err.stack);
});

batchQueue.on('error', (error) => {
    logErrorToFile(`Redis connection error: ${error.message}`, error.stack);
});