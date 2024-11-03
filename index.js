const dotenv = require("dotenv");
dotenv.config();

const { processCSVFilesInLatestFolder } = require("./s3-helpers");
const { processBatch } = require("./batch-helpers");
const { logger, logErrorToFile } = require("./logger");
const { performance } = require("perf_hooks"); // Import performance to track time

// Start time to track the whole process duration
const startTime = performance.now();

// Main process function, e.g., processCSVFiles()
const mainProcess = async () => {
  try {
    const bucket = process.env.S3_BUCKET_NAME;
    if (!bucket) {
      logErrorToFile("Environment variable S3_BUCKET_NAME is not set.");
      return;
    }
    
    logger.info(`Starting process for S3 bucket: ${bucket}`);

    // Pass the generated filename to processBatch
    await processCSVFilesInLatestFolder(bucket, 30, (batch, ...args) => processBatch(batch, ...args));

    // Record completion message and elapsed time
    const endTime = performance.now();
    const duration = ((endTime - startTime) / 1000).toFixed(2);
    const durationMessage = `Process completed in ${duration} seconds.`;

    // Log the duration to both console and error-log.txt
    console.log(durationMessage);
    logErrorToFile(durationMessage);
  } catch (error) {
    logErrorToFile(`Unhandled error in mainProcess: ${error.message}, error`);
    handleUnexpectedEnd();
  }
};

function handleUnexpectedEnd() {
  const duration = (Date.now() - startTime) / 1000;
  logger.error(`Process ended unexpectedly after ${duration} seconds.`);
  process.exit(1);
}

mainProcess().catch(error => {
  logErrorToFile(`Critical error in main: ${error.message}, error`);
});

process.on('uncaughtException', (error) => {
  logger.error("Uncaught exception", error);
  handleUnexpectedEnd();
});

process.on('unhandledRejection', (reason, promise) => {
  logger.error("Unhandled rejection", reason);
  handleUnexpectedEnd();
});