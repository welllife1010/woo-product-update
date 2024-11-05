const dotenv = require("dotenv");
dotenv.config();

const batchQueue = require('./queue');
const { processCSVFilesInLatestFolder } = require("./s3-helpers");
const { logger, logErrorToFile } = require("./logger");
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

// Enqueue a batch job
async function enqueueBatchJob(batchData, fileKey) {
  const jobData = {
    batch: batchData,              // The actual batch array
    totalProducts: batchData.length, // Total number of products in the batch
    fileKey: fileKey                // File key for tracking purposes
  };

  console.log(`DEBUG: Attempting to enqueue batch with ${batchData.length} items for file: ${fileKey}`);
  logErrorToFile(`DEBUG: Attempting to enqueue batch with ${batchData.length} items for file: ${fileKey}, error`);

  if (!batchQueue) {
    console.error("Error: batchQueue is undefined.");
    return;
  }

  const job = await batchQueue.add(jobData);
  console.log(`DEBUG: Successfully enqueued job with ID: ${job.id}`);
  logger.info(`Enqueued batch job with ID: ${job.id}`);
  return job.id; // Return job ID for tracking
}

// Main process function to process CSV files
const mainProcess = async () => {
  try {
    const bucket = process.env.S3_BUCKET_NAME;
    if (!bucket) {
      logErrorToFile("Environment variable S3_BUCKET_NAME is not set.");
      return;
    }

    logger.info(`Starting process for S3 bucket: ${bucket}`);

    // Process files in the latest folder, enqueuing each batch
    await processCSVFilesInLatestFolder(bucket, 80, async (batch, fileKey) => {
      if (!Array.isArray(batch) || batch.length === 0) {
        logger.error("Invalid batch data. It should be a non-empty array.");
        return;
      }

      const jobId = await enqueueBatchJob(batch, fileKey); // Queue each batch as a job
      logger.info(`Queued batch job with ID: ${jobId}`);
    });

    // Record completion message and elapsed time
    const endTime = performance.now();
    const duration = ((endTime - startTime) / 1000).toFixed(2);
    const durationMessage = `Process completed in ${duration} seconds.`;

    console.log(durationMessage);
    logErrorToFile(durationMessage);
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

app.post('/api/start-batch', async (req, res) => {
  const batchData = req.body.batchData; // Assuming batch data is passed in the request body
  const job = await batchQueue.add({ batch: batchData });
  logger.info(`Enqueued batch job with ID: ${job.id}`);
  res.json({ jobId: job.id });
});

app.get('/api/job-status/:jobId', async (req, res) => {
  const job = await batchQueue.getJob(req.params.jobId);
  if (!job) return res.status(404).send('Job not found');

  const state = await job.getState();
  const progress = job.progress();
  res.json({ state, progress });
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