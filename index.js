const dotenv = require("dotenv");
dotenv.config();

const { batchQueue } = require('./queue');
const { processCSVFilesInLatestFolder } = require('./s3-helpers');
const { logger, logErrorToFile,logUpdatesToFile, logInfoToFile, logProgressToFile } = require("./logger");
const { addBatchJob } = require('./job-manager');
const { performance } = require("perf_hooks"); // Import performance to track time
const { BullAdapter } = require('@bull-board/api/bullAdapter');
const { createBullBoard } = require('@bull-board/api');
const { ExpressAdapter } = require('@bull-board/express');

const express = require('express');
const app = express();

app.use(express.json()); // Needed to parse JSON request bodies

const getS3BucketName = (executionMode) => {
  return (executionMode === 'development') ? process.env.S3_TEST_BUCKET_NAME : process.env.S3_BUCKET_NAME;
};

// Unified error handling for process termination
const handleProcessError = (error, type = "Error") => {
  const duration = ((Date.now() - startTime) / 1000).toFixed(2);
  logger.error(`${type} occurred after ${duration} seconds:`, error);
  process.exit(1);
};

const serverAdapter = new ExpressAdapter(); // Provides an Express-compatible middleware router for the Bull Board dashboard
serverAdapter.setBasePath('/admin/queues');

app.use('/admin/queues', serverAdapter.getRouter());

createBullBoard({
    queues: [new BullAdapter(batchQueue)],
    serverAdapter: serverAdapter,
});

// Start time to track the whole process duration
const startTime = performance.now();

const executionMode = process.env.EXECUTION_MODE || 'production';
logInfoToFile(`Running in ${executionMode} mode`);

// Optional Bull Board setup, enabled only in development mode
if (executionMode === 'development') {
  const serverAdapter = new ExpressAdapter();
  serverAdapter.setBasePath('/admin/queues');

  app.use('/admin/queues', serverAdapter.getRouter());

  createBullBoard({
    queues: [new BullAdapter(batchQueue)],
    serverAdapter: serverAdapter,
  });
}

// Main process function to process CSV files
const mainProcess = async () => {
  try {
    const s3BucketName = getS3BucketName(executionMode);

    if (!s3BucketName) {
      logErrorToFile("Environment variable S3_BUCKET_NAME is not set.");
      return;
    }

    logger.info(`Starting process for S3 bucket: ${s3BucketName}`);

    // Process files in the latest folder, enqueuing each batch
    await processCSVFilesInLatestFolder(s3BucketName, 20);

    // Record completion message and elapsed time
    const endTime = performance.now();
    const duration = ((endTime - startTime) / 1000).toFixed(2);
    logUpdatesToFile(`"processCSVFilesInLatestFolder" function completed in ${duration} seconds.`);
  } catch (error) {
    logErrorToFile(`Unhandled error in mainProcess: ${error.message}`);
    handleProcessError(error);
  }
};

// Start periodic progress logging
setInterval(async () => {
  try {
    await logProgressToFile();
  } catch (error) {
    console.error(`Error during periodic progress logging: ${error.message}`);
  }
}, 1 * 60 * 1000); // 1 minute

// Start the main process
mainProcess().catch(error => handleProcessError(error, "Critical error in main"));

// For Manual Job Triggering & Testing and Debugging 
app.post('/api/start-batch', async (req, res) => {
  try {
    const batchData = req.body.batchData; // Assuming batch data is passed in the request body

    // Generate a unique job ID for this batch job
    const jobId = `manual-batch-${Date.now()}`;

    // Use the centralized function to add the batch job
    const job = await addBatchJob({ batch: batchData }, jobId);

    logger.info(`Enqueued batch job with ID: ${job.id}`);
    res.json({ jobId: job.id });
  } catch (error) {
    logErrorToFile(`Failed to enqueue batch: ${error.message}`, error);
    res.status(500).json({ error: "Failed to enqueue batch" });
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
  if (executionMode === 'development') {
    console.log('Bull Dashboard is available at http://localhost:3000/admin/queues');
  }
});

process.on('uncaughtException', (error) => handleProcessError(error, "Uncaught exception"));
process.on('unhandledRejection', (reason) => handleProcessError(reason, "Unhandled rejection"));