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
    
    logger.info(`Starting process for bucket: ${bucket}`);

    // Pass the generated filename to processBatch
    await processCSVFilesInLatestFolder(bucket, 30, (batch, ...args) => processBatch(batch, ...args));
    //await processCSVFilesInLatestFolder(bucket, 20, processBatch);

    // Record completion message and elapsed time
    const endTime = performance.now();
    const duration = ((endTime - startTime) / 1000).toFixed(2);
    const durationMessage = `Process completed in ${duration} seconds.`;

    // Log the duration to both console and error-log.txt
    console.log(durationMessage);
    logErrorToFile(durationMessage);
  } catch (error) {
    logErrorToFile(`An error occurred during processing: ${error.message}`);
  }
};


mainProcess();