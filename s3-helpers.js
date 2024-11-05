const fs = require("fs");
const path = require("path");
const { S3Client, ListObjectsV2Command, GetObjectCommand } = require("@aws-sdk/client-s3");
const { promisify } = require("util");
const { Readable, pipeline } = require("stream"); // Promisify the stream pipeline utility
const streamPipeline = promisify(pipeline); // Use async pipeline with stream promises
const csvParser = require("csv-parser");
const { logger, logErrorToFile, logUpdatesToFile, logDetailedErrorToFile } = require("./logger");
const batchQueue = require('./queue');

// AWS S3 setup (using AWS SDK v3)
const s3Client = new S3Client({ 
  region: process.env.AWS_REGION_NAME,
  endpoint: "https://s3.us-west-1.amazonaws.com", // Use your specific bucket's region
  forcePathStyle: true, // This helps when using custom endpoints
  requestTimeout: 600000 // Set timeout to 10 minutes
});

// Define the path to the checkpoint file
const checkpointFilePath = path.join(__dirname, "process_checkpoint.json");

// Function to save progress to the checkpoint file
const saveCheckpoint = (fileKey, lastProcessedRow) => {
    const checkpoints = fs.existsSync(checkpointFilePath)
        ? JSON.parse(fs.readFileSync(checkpointFilePath, "utf-8"))
        : {};
    checkpoints[fileKey] = {
      lastProcessedRow,
      timestamp: new Date().toISOString(),
    };
    fs.writeFileSync(checkpointFilePath, JSON.stringify(checkpoints, null, 2));
};

// Function to get the last checkpoint for a given file
const getCheckpoint = (fileKey) => {
    if (!fs.existsSync(checkpointFilePath)) return 0;
    const checkpoints = JSON.parse(fs.readFileSync(checkpointFilePath, "utf-8"));
    return checkpoints[fileKey]?.lastProcessedRow || 0;
};

 // Function to get the latest folder key by sorting folders by date
 const getLatestFolderKey = async (bucket) => {
    try {
      const listParams = { Bucket: bucket, Delimiter: '/' }; // Delimit by "/" to get folders
      const listData = await s3Client.send(new ListObjectsV2Command(listParams));
  
      // Filter for folders and sort by date
      const folders = listData.CommonPrefixes
        .map(prefix => prefix.Prefix)
        .filter(prefix => /^\d{2}-\d{2}-\d{4}\/$/.test(prefix)) // Match pattern like "10-31-2024/"
        .sort((a, b) => new Date(b.slice(0, 10)) - new Date(a.slice(0, 10))); // Sort by date descending
  
      if (folders.length === 0) {
        logErrorToFile(`No valid folders found in the bucket: ${bucket}.`);
        return null;
      }
  
      return folders[0]; // Return the latest folder
    } catch (error) {
      logErrorToFile(`Error in getLatestFolderKey for bucket "${bucket}": ${error.message}, error`);
      return null;
    }
  };

// Function to process CSV files within the latest folder
const processCSVFilesInLatestFolder = async (bucket, batchSize, processBatchFunction) => {
    try {
      const latestFolder = await getLatestFolderKey(bucket);
      if (!latestFolder) {
        logErrorToFile("No latest folder found, exiting.");
        return;
      }
  
      logger.info(`Processing files in the latest folder: ${latestFolder}`);
      const listParams = { Bucket: bucket, Prefix: latestFolder };
      const listData = await s3Client.send(new ListObjectsV2Command(listParams));
  
      if (!listData.Contents) {
        logErrorToFile(`No contents found in folder: ${latestFolder} of bucket: ${bucket}`);
        return;
      }
  
      const csvFiles = listData.Contents.filter((file) => file.Key.toLowerCase().endsWith(".csv"));
      logger.info(`Retrieved ${csvFiles.length} CSV files in folder: ${latestFolder}`);
      csvFiles.forEach(file => logger.info(`Found file: ${file.Key}`));

      if (csvFiles.length === 0) {
        logErrorToFile(`No CSV files found in folder: ${latestFolder} of bucket: ${bucket}`);
        return;
      }

      const fileProcessingTasks = csvFiles.map(async (file) => {
        try {
            logger.info(`Processing file: ${file.Key}`);
            await readCSVAndProcess(bucket, file.Key, batchSize, async (batch) => {
                const job = await batchQueue.add({ batch, fileKey: file.Key, totalProducts: batch.length });
                logger.info(`Enqueued batch job with ID: ${job.id} for file: ${file.Key}`);
                logger.debug(`Job data: ${JSON.stringify({ batch, fileKey: file.Key, totalProducts: batch.length })}`);
            });
        } catch (error) {
            logErrorToFile(`Error processing file ${file.Key}: ${error.message}`);
        }        
      });

      await Promise.all(fileProcessingTasks); // Wait for all files to process
      logger.info("All CSV files in the latest folder have been processed.");
      logUpdatesToFile("All CSV files in the latest folder have been processed.");
    } catch (error) {
      logErrorToFile(`Error in processCSVFilesInLatestFolder for bucket "${bucket}": ${error.message}, error`);
    }
};

// Function to read CSV from S3 and process in batches with checkpointing
const readCSVAndProcess = async (bucket, key, batchSize, processBatchFunction) => {
    const params = { Bucket: bucket, Key: key};
    const MAX_RETRIES = 3;
    let consecutiveErrors = 0;
    let batch = [];
    let totalProducts = 0;
    const lastProcessedRow = getCheckpoint(key); // Retrieve the last processed row for this file from checkpoint

    logger.info(`Starting file processing for "${key}" from row ${lastProcessedRow}`);
    try {
      const data = await s3Client.send(new GetObjectCommand(params)); // Fetch the CSV data from S3
      const readableStream = Readable.from(data.Body); // Create a readable stream from the S3 object
  
      // Process CSV rows, starting from the last processed row
      await streamPipeline(
        readableStream,
        csvParser(),
        // Iterates over each row in the CSV asynchronously, allowing us to handle each chunk (row) as it arrives, without waiting for the entire file to load.
        async function* (source) {
          logger.info(`Starting to process CSV file: ${key}`);
          for await (const chunk of source) {
            try {
              totalProducts++;

              logger.debug(`Processing row ${totalProducts}: ${JSON.stringify(chunk)}`);

              if (totalProducts <= lastProcessedRow) continue; // Skip rows up to checkpoint

              const normalizedData = Object.keys(chunk).reduce((acc, key) => {
                acc[key.trim().toLowerCase().replace(/\s+/g, "_")] = chunk[key];
                return acc;
              }, {});
    
              batch.push(normalizedData);
              logger.debug(`Added to batch: ${normalizedData.part_number} at row ${totalProducts}`);
    
              // Process the batch if it reaches batchSize
              if (batch.length >= batchSize) {
                await processAndCheckpoint(batch, totalProducts, key, processBatchFunction); // Pass batch to enqueue job
                batch = []; // Clear batch after processing
                consecutiveErrors = 0; // Reset error count on success
              }

              logger.info(`Completed processing file: ${key}, total rows processed: ${totalProducts}`);
              
            } catch (error) {
              // Detailed error logging
              if (error.code === 'ENOTFOUND' || error.code === 'ECONNRESET') {
                logErrorToFile(`Network error: ${error.message}, error`);
              } else if (error.name === 'CSVError') {  // Assuming csv-parser throws errors with name 'CSVError'
                  logErrorToFile(`CSV parsing error at row ${totalProducts} in file "${key}": ${error.message}, error`);
              } else {
                  logErrorToFile(`Unhandled exception processing row ${totalProducts} in file "${key}": ${error.message}, error`);
                  logDetailedErrorToFile(error, `Error processing row ${totalProducts} in file "${key}"`);
              }

              // Increment error count and check if max retries are reached
              consecutiveErrors++;
              if (consecutiveErrors >= MAX_RETRIES) {
                  throw new Error(`Processing aborted after ${MAX_RETRIES} consecutive row errors.`);
              }
            };
          }

          if (batch.length > 0) {
            await processAndCheckpoint(batch, totalProducts, key, processBatchFunction); // Process any remaining data
          }
        }
      );
  
      logger.warn(`Completed processing of file: "${key}", total products: ${totalProducts}`);
    } catch (error) {
      logErrorToFile(`Error in readCSVAndProcess for file "${key}" in bucket "${bucket}": ${error.message}, error`);
      throw error; // Ensure any error bubbles up to be caught in Promise.all
    }
  };

// Helper function to process batch and save checkpoint
const processAndCheckpoint = async (batch, totalProducts, key, processBatchFunction) => {
  const MAX_BATCH_RETRIES = 5;
  let attempts = 0;

  console.log(`DEBUG: Entering processAndCheckpoint for batch of size ${batch.length} in file ${key}`);

  while (attempts < MAX_BATCH_RETRIES) {
    try {
        //await processBatchFunction(batch, totalProducts - batch.length, totalProducts, key);
        await processBatchFunction(batch);
        saveCheckpoint(key, totalProducts); // Update checkpoint after processing batch
        logger.info(`Processed and saved checkpoint at row ${totalProducts} for file "${key}"`);
        return;
    } catch (error) {
      attempts++;
      const delay = Math.pow(2, attempts) * 1000; // Exponential backoff: 2, 4, 8 seconds, etc.
      logErrorToFile(`Error processing batch at row ${totalProducts} in file "${key}" on attempt ${attempts}: ${error.message}, error`);
      
      // Specific check for 524 error to retry with backoff
      if (error.response && error.response.status === 524) {
          logger.warn(`524 Timeout detected. Retrying after ${delay / 1000} seconds...`);
          logErrorToFile(`524 Timeout detected. Retrying after ${delay / 1000} seconds...`);
          await new Promise(resolve => setTimeout(resolve, delay));
      } else {
          throw new Error(`Batch processing error at row ${totalProducts} for file "${key}"`);
      }

      console.error(`Error in processAndCheckpoint: ${error.message}`);
      throw error;
    }
  }
};

module.exports = {
  getLatestFolderKey,
  processCSVFilesInLatestFolder,
};