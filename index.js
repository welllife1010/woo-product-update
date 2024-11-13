const dotenv = require("dotenv");
dotenv.config();

const { batchQueue } = require('./queue');
const { processCSVFilesInLatestFolder } = require('./s3-helpers');
const { logger, logErrorToFile,logUpdatesToFile, logInfoToFile } = require("./logger");
const { performance } = require("perf_hooks"); // Import performance to track time
const { BullAdapter } = require('@bull-board/api/bullAdapter');
const { createBullBoard } = require('@bull-board/api');
const { ExpressAdapter } = require('@bull-board/express');

const express = require('express');
const app = express();

app.use(express.json()); // Needed to parse JSON request bodies

const serverAdapter = new ExpressAdapter();
serverAdapter.setBasePath('/admin/queues');

app.use('/admin/queues', serverAdapter.getRouter());

createBullBoard({
    queues: [new BullAdapter(batchQueue)],
    serverAdapter: serverAdapter,
});

// Start time to track the whole process duration
const startTime = performance.now();

const executionMode = process.env.EXECUTION_MODE || 'production';

if (executionMode === 'development') {
  logInfoToFile("Running in development mode");
  // Add any specific logic for development mode
} else if (executionMode === 'stage') {
  logInfoToFile("Running in staging mode");
  // Add any specific logic for staging mode
} else {
  logInfoToFile("Running in production mode");
  // Production-specific logic here
}

// Main process function to process CSV files
const mainProcess = async () => {
  try {
    const s3BucketName = (executionMode === 'development')
                        ? process.env.S3_TEST_BUCKET_NAME 
                        : process.env.S3_BUCKET_NAME;

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
    const durationMessage = `"processCSVFilesInLatestFolder" function (read CSV files) evoked in mainProcess function completed in ${duration} seconds.`;

    logUpdatesToFile(durationMessage);
  } catch (error) {
    logErrorToFile(`Unhandled error in mainProcess: ${error.message}`);
    handleUnexpectedEnd();
  }
};

// Handle unexpected termination
function handleUnexpectedEnd() {
  const duration = (Date.now() - startTime) / 1000;
  logger.error(`Process ended unexpectedly after ${duration} seconds.`);
  process.exit(1);
}

// Start the main process
mainProcess().catch(error => {
  logErrorToFile(`Critical error in main: ${error.message}, error`);
});

// For Manual Job Triggering & Testing and Debugging 
app.post('/api/start-batch', async (req, res) => {
  const batchData = req.body.batchData; // Assuming batch data is passed in the request body
  const job = await batchQueue.add({ batch: batchData });
  logger.info(`Enqueued batch job with ID: ${job.id}`);
  res.json({ jobId: job.id });
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
  console.log('Bull Dashboard is available at http://localhost:3000/admin/queues');
});

process.on('uncaughtException', (error) => {
  logger.error("Uncaught exception", error);
  handleUnexpectedEnd();
});

process.on('unhandledRejection', (reason, promise) => {
  logger.error("Unhandled rejection", reason);
  handleUnexpectedEnd();
});