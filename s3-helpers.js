const fs = require("fs");
const path = require("path");
const { S3Client, ListObjectsV2Command, GetObjectCommand } = require("@aws-sdk/client-s3");
const { promisify } = require("util");
const { Readable, pipeline } = require("stream"); // Promisify the stream pipeline utility
const streamPipeline = promisify(pipeline); // Use async pipeline with stream promises
const csvParser = require("csv-parser");
const { logger, logErrorToFile, logUpdatesToFile } = require("./logger");

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

// AWS S3 setup (using AWS SDK v3)
const s3Client = new S3Client({ 
    region: process.env.AWS_REGION_NAME,
    endpoint: "https://s3.us-west-1.amazonaws.com", // Use your specific bucket's region
    forcePathStyle: true, // This helps when using custom endpoints
    requestTimeout: 600000 // Set timeout to 10 minutes
});

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
      logErrorToFile(`Error in getLatestFolderKey for bucket "${bucket}": ${error.message}`);
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
      if (csvFiles.length === 0) {
        logErrorToFile(`No CSV files found in folder: ${latestFolder} of bucket: ${bucket}`);
        return;
      }
  
       await Promise.all(
        csvFiles.slice(0, 3).map(async (file) => {
          try {
            logger.info(`Processing file: ${file.Key}`);
            await readCSVAndProcess(bucket, file.Key, batchSize, processBatchFunction);
          } catch (error) {
            logErrorToFile(`Error processing file ${file.Key}: ${error.message}`);
          }
        })
      );

      //Process files sequentially
      // for (const file of csvFiles.slice(0, 3)) {
      //   try {
      //     logger.info(`Processing file: ${file.Key}`);
      //     await readCSVAndProcess(bucket, file.Key, batchSize, processBatchFunction);
      //   } catch (error) {
      //     logErrorToFile(`Error processing file ${file.Key}: ${error.message}`);
      //   }
      // }
  
      logger.info("All CSV files in the latest folder have been processed.");
      logUpdatesToFile("All CSV files in the latest folder have been processed.");
    } catch (error) {
      logErrorToFile(`Error in processCSVFilesInLatestFolder for bucket "${bucket}": ${error.message}`);
    }
  };

// Function to read CSV from S3 and process in batches with checkpointing
const readCSVAndProcess = async (bucket, key, batchSize, processBatchFunction) => {
    const params = { Bucket: bucket, Key: key};
    const MAX_RETRIES = 3;
    let consecutiveErrors = 0;
  
    try {
      const data = await s3Client.send(new GetObjectCommand(params)); // Fetch the CSV data from S3
      const readableStream = Readable.from(data.Body); // Create a readable stream from the S3 object
      let batch = [];
      let totalProducts = 0;
      const lastProcessedRow = getCheckpoint(key); // Retrieve last processed row for this file (from checkpoint)

      logger.warn(`Resuming from row ${lastProcessedRow} for file ${key}`);
  
      // Process CSV rows, starting from the last processed row
      await streamPipeline(
        readableStream,
        csvParser(),
        async function* (source) {
          // Iterates over each row in the CSV asynchronously, allowing us to handle each chunk (row) as it arrives, without waiting for the entire file to load.
          for await (const chunk of source) {
            try {

              totalProducts++;
              if (totalProducts <= lastProcessedRow) continue; // Skip rows up to checkpoint

              const normalizedData = Object.keys(chunk).reduce((acc, key) => {
                acc[key.trim().toLowerCase().replace(/\s+/g, "_")] = chunk[key];
                return acc;
              }, {});
    
              batch.push(normalizedData);
              logger.debug(`Added to batch: ${normalizedData.part_number} at row ${totalProducts}`);
    
              // Process batch if it reaches batchSize
              if (batch.length >= batchSize) {
                try {
                  await processBatchFunction(batch, totalProducts - batch.length, totalProducts, key);
                  saveCheckpoint(key, totalProducts); // Update checkpoint after processing batch
                  batch = []; // Clear batch after processing
                  consecutiveErrors = 0; // Reset error count on success
                } catch (error) {
                  logErrorToFile(`Error processing batch at row ${totalProducts}: ${error.message}`);
                  consecutiveErrors++;
                  if (consecutiveErrors >= MAX_RETRIES) {
                    throw new Error(`Processing aborted after ${MAX_RETRIES} consecutive errors.`);
                  }
                }
              }

            } catch (error) {
              logErrorToFile(`Error processing row ${totalProducts} in file "${key}": ${error.message}`);
              consecutiveErrors++;
              if (consecutiveErrors >= MAX_RETRIES) {
                throw new Error(`Processing aborted after ${MAX_RETRIES} consecutive row errors.`);
              }
            };
            
          }
  
          // Process remaining rows if any
          if (batch.length > 0) {
            await processBatchFunction(batch, totalProducts - batch.length, totalProducts, key);
            saveCheckpoint(key, totalProducts); // Update checkpoint
          }
        }
      );
  
      logger.warn(`Completed processing of file: "${key}", total products: ${totalProducts}`);
    } catch (error) {
      logErrorToFile(`Error in readCSVAndProcess for file "${key}" in bucket "${bucket}": ${error.message}`);
      throw error; // Ensure any error bubbles up to be caught in Promise.all
    }
  };

module.exports = {
  getLatestFolderKey,
  processCSVFilesInLatestFolder,
};