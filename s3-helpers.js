const { S3Client, ListObjectsV2Command, GetObjectCommand } = require("@aws-sdk/client-s3");
const { promisify } = require("util");
const { Readable, pipeline } = require("stream"); // Promisify the stream pipeline utility
const streamPipeline = promisify(pipeline); // Use async pipeline with stream promises
const csvParser = require("csv-parser");
const { logErrorToFile, logUpdatesToFile, logInfoToFile } = require("./logger");
const { batchQueue, redisClient } = require('./queue');
const { addBatchJob } = require('./job-manager');

const executionMode = process.env.EXECUTION_MODE || 'production';

const initializeFileTracking = async (fileKey, totalRows) => {
  await redisClient.set(`total-rows:${fileKey}`, totalRows);
  await redisClient.set(`updated-products:${fileKey}`, 0);  // Initialize if missing
  await redisClient.set(`skipped-products:${fileKey}`, 0);  // Initialize if missing
  await redisClient.set(`failed-products:${fileKey}`, 0);   // Initialize if missing
};

// AWS S3 setup (using AWS SDK v3)
const s3Client = new S3Client({ 
  region: process.env.AWS_REGION_NAME,
  endpoint: "https://s3.us-west-1.amazonaws.com", // Use specific bucket's region
  forcePathStyle: true, // This helps when using custom endpoints
  requestTimeout: 300000 // Set timeout to 10 minutes
});

const pattern = (executionMode === 'production') ? /^\d{2}-\d{2}-\d{4}\/$/ : /^\d{2}-\d{2}-\d{4}-test\/$/;

// Get the latest folder key (name) by sorting folders by date
const getLatestFolderKey = async (bucketName) => {
  try {
    const listParams = { Bucket: bucketName, Delimiter: '/' }; // Delimit by "/" to get folders
    const listData = await s3Client.send(new ListObjectsV2Command(listParams));

    // Filter for folders and sort by date, ensure CommonPrefixes is defined
    const folders = (listData.CommonPrefixes || [])
      .map(prefix => prefix.Prefix)
      .filter(prefix => pattern.test(prefix)) // Match pattern like "10-31-2024/" for production mode or "10-31-2024-test/" for development mode
      .sort((a, b) => new Date(b.slice(0, 10)) - new Date(a.slice(0, 10))); // Sort by date descending

    if (folders.length === 0) {
      logErrorToFile(`No valid folders found in the bucket: ${bucketName}.`);
      return null;
    }

    return folders[0]; // Return the latest folder
  } catch (error) {
    logErrorToFile(`Error in getLatestFolderKey for bucket "${bucketName}": ${error.message}`, error.stack);
    return null;
  }
};

// Process CSV files within the latest folder
const processCSVFilesInLatestFolder = async (bucketName, batchSize) => {
  try {
    const latestFolder = await getLatestFolderKey(bucketName);
    if (!latestFolder) {
      logErrorToFile("No latest folder found, exiting.");
      return;
    }

    logInfoToFile(`Processing files in the latest folder: ${latestFolder}`);
    const listParams = { Bucket: bucketName, Prefix: latestFolder };
    const listData = await s3Client.send(new ListObjectsV2Command(listParams));

    if (!listData.Contents) {
      logErrorToFile(`No contents found in folder: ${latestFolder} of bucket: ${bucketName}`);
      return;
    }

    const csvFiles = listData.Contents.filter((file) => file.Key.toLowerCase().endsWith(".csv"));
    logInfoToFile(`Retrieved ${csvFiles.length} CSV files in folder: ${latestFolder}`);
    csvFiles.forEach(file => logInfoToFile(`Found file: ${file.Key}`));

    if (csvFiles.length === 0) {
      logErrorToFile(`No CSV files found in folder: ${latestFolder} of bucket: ${bucketName}`);
      return;
    }

    const fileProcessingTasks = csvFiles.map(async (file) => {
      try {
          logInfoToFile(`Processing file: ${file.Key}`);
          await readCSVAndEnqueueJobs(bucketName, file.Key, batchSize);

      } catch (error) {
          logErrorToFile(`Error processing file ${file.Key}. Error: ${error.message}`, error.stack);
      }        
    });

    await Promise.all(fileProcessingTasks); // Wait for all files to process
    logUpdatesToFile("All CSV files in the latest folder have been read.");
  } catch (error) {
    logErrorToFile(`Error in processCSVFilesInLatestFolder for bucket "${bucketName}": ${error.message}`, error.stack);
  }
};

// Read CSV from S3 and enqueue jobs
const readCSVAndEnqueueJobs = async (bucketName, key, batchSize) => {
  const params = { Bucket: bucketName, Key: key};
  const MAX_RETRIES = 3;
  let consecutiveErrors = 0;
  let batch = [];
  let totalRows = 0;  // Initialize total rows count
  let lastProcessedRow = 0;  // Track the last processed row

  try {
    // Fetch the CSV data from S3
    const data = await s3Client.send(new GetObjectCommand(params));

    // Convert data.Body to a string or Buffer (adjust based on the SDK version)
    const bodyContent = await data.Body.transformToString(); // Or use 'await streamToString(data.Body)' if needed

    const rows = bodyContent.split('\n'); // Split CSV content into rows
    totalRows = rows.length - 1; // Subtract header row

    // Initialize tracking in Redis for this file
    await initializeFileTracking(key, totalRows);
    await redisClient.set(`total-rows:${key}`, totalRows); // Store individual file's row count
    await redisClient.incrBy('overall-total-rows', totalRows); // Increment the overall total row count

    // Create reusable data streams from the cached content
    const dataStream1 = Readable.from(bodyContent);
    const dataStream2 = Readable.from(bodyContent);

    // Use dataStream1 for counting rows
    await streamPipeline(
      dataStream1,
      csvParser(),
      async function* (source) {
        let count = 0;
        for await (const row of source) {
          count++;
        }
        await redisClient.set(`total-rows:${key}`, count);
        totalRows = count; // Set the totalRows variable for further use
        logUpdatesToFile(`Total rows for file ${key}: ${totalRows}, saved to Redis.`);
      }
    );

    // Use dataStream2 for processing rows
    await streamPipeline(
      dataStream2,
      csvParser(),
      // Iterates over each row in the CSV asynchronously, allowing us to handle each chunk (row) as it arrives, without waiting for the entire file to load.
      async function* (source) {
        logInfoToFile(`Starting to process CSV file: ${key}, on row ${lastProcessedRow + 1} / ${totalRows}`);
        for await (const chunk of source) {
          try {
            lastProcessedRow++;

            // Process each row and normalize the data
            const normalizedData = Object.keys(chunk).reduce((acc, key) => {
              acc[key.trim().toLowerCase().replace(/\s+/g, "_")] = chunk[key];
              return acc;
            }, {});
  
            batch.push(normalizedData);
            //logUpdatesToFile(`Added to batch: ${normalizedData.part_number} at row ${lastProcessedRow}`);
  
            // Check if the batch size is reached
            if (batch.length >= batchSize) {

              // Create a job with the last processed row and total rows
              const jobData = {
                batch,
                fileKey: key,
                totalProductsInFile: totalRows,
                lastProcessedRow,
                batchSize: batch.length
              };

              // Generate a unique job ID
              const jobId = `readCSVAndEnqueueJobs_${key}_${lastProcessedRow}`;

              // Before adding a job, check jobData and add detailed logging
              try {
                const job = await addBatchJob(jobData, jobId);;
                logInfoToFile(`Successfully enqueued job with ID: ${job.id} for rows up to ${lastProcessedRow} in file: ${key}`);
              } catch (error) {
                  logErrorToFile(`Failed to enqueue job for rows up to ${lastProcessedRow} in file: ${key}. Error: ${error.message}`, error.stack);
              }

              batch = [];  // Clear the batch after processing
              consecutiveErrors = 0;  // Reset error count
            }
            
          } catch (error) {
            // Detailed error logging
            if (error.code === 'ENOTFOUND' || error.code === 'ECONNRESET') {
              logErrorToFile(`Network error: ${error.message}`, error.stack);
            } else if (error.name === 'CSVError') {  // Assuming csv-parser throws errors with name 'CSVError'
                logErrorToFile(`CSV parsing error at row ${totalRows} in file "${key}": ${error.message}`, error.stack);
            } else {
                logErrorToFile(`Error processing row ${totalRows} in file "${key}: ${error.message}"`, error.stack);
            }

            // Increment error count and check if max retries are reached
            consecutiveErrors++;
            if (consecutiveErrors >= MAX_RETRIES) {
                throw new Error(`Processing aborted after ${MAX_RETRIES} consecutive row errors.`);
            }
          };
        }

        // If any remaining rows are in the batch, process them
        if (batch.length > 0) {
          // Create a final job for any remaining data with totalRows included
          const jobData = {
            batch,
            fileKey: key,
            totalProductsInFile: totalRows, // Add totalRows to job data
            lastProcessedRow,
            batchSize: batch.length
          };

          // Generate a unique job ID
          const jobId = `readCSVAndEnqueueJobs_${key}_${lastProcessedRow}`;

          try {
            // Use the centralized function to add the batch job
            const job = await addBatchJob(jobData, jobId);
    
            logInfoToFile(`Enqueued final batch job for rows up to ${lastProcessedRow} in file: ${key}`);
            logInfoToFile(`DEBUG: Enqueued batch job with ID: ${job.id} for rows up to ${lastProcessedRow} in file: ${key}`);
          } catch (error) {
              logErrorToFile(`Failed to enqueue final batch job for rows up to ${lastProcessedRow} in file: ${key}. Error: ${error.message}`, error.stack);
          }
        }
      }
    );

    logUpdatesToFile(`Completed reading the file: "${key}", total rows: ${totalRows}`);
  } catch (error) {
    logErrorToFile(`Error in readCSVAndEnqueueJobs for file "${key}" in bucket "${bucketName}": ${error.message}, error`);
    throw error; // Ensure any error bubbles up to be caught in Promise.all
  }
};

module.exports = {
  getLatestFolderKey,
  processCSVFilesInLatestFolder,
};