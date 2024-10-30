const path = require("path");
const dotenv = require("dotenv");
dotenv.config();

const { processCSVFilesInLatestFolder } = require("./s3-helpers");
const { processBatch } = require("./batch-helpers");
const { logger, logErrorToFile } = require("./logger");
const { performance } = require("perf_hooks"); // Import performance to track time

// Start time to track the whole process duration
const startTime = performance.now();

// Generate the unique filename with date and version
const date = new Date().toISOString().split("T")[0]; // YYYY-MM-DD format
const version = "v1"; // Change this if you need to track different versions
const updatedProductsFile = path.join(__dirname, `updated-products-${date}-${version}.txt`);

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
    await processCSVFilesInLatestFolder(bucket, 10, (batch, ...args) => processBatch(batch, ...args, updatedProductsFile));
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