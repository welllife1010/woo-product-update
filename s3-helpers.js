const { S3Client, ListObjectsV2Command, GetObjectCommand } = require("@aws-sdk/client-s3");
const { promisify } = require("util");
const { Readable, pipeline } = require("stream"); // Promisify the stream pipeline utility
const streamPipeline = promisify(pipeline); // Use async pipeline with stream promises
const csvParser = require("csv-parser");
const { logger, logErrorToFile } = require("./logger");

// AWS S3 setup (using AWS SDK v3)
const s3Client = new S3Client({ 
    region: process.env.AWS_REGION_NAME,
    endpoint: "https://s3.us-west-1.amazonaws.com", // Use your specific bucket's region
    forcePathStyle: true, // This helps when using custom endpoints
});

 // Function to get the latest folder key by sorting folders by date
 const getLatestFolderKey = async (bucket) => {
    try {
      const listParams = { Bucket: bucket, Delimiter: '/' }; // Delimit by "/" to get folders
      const listData = await s3Client.send(new ListObjectsV2Command(listParams));
  
      // Filter for folders and sort by date
      const folders = listData.CommonPrefixes
        .map(prefix => prefix.Prefix)
        .filter(prefix => /^\d{2}-\d{2}-\d{4}_product_updates\/$/.test(prefix)) // Match pattern like "10-29-2024_product_updates/"
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
          logger.info(`Processing file: ${file.Key}`);
          await readCSVAndProcess(bucket, file.Key, batchSize, processBatchFunction);
        })
      );
  
      logger.info("All CSV files in the latest folder have been processed.");
    } catch (error) {
      logErrorToFile(`Error in processCSVFilesInLatestFolder for bucket "${bucket}": ${error.message}`);
    }
  };

  // Function to read CSV from S3 and process in batches directly
const readCSVAndProcess = async (bucket, key, batchSize, processBatchFunction) => {
    const params = {
      Bucket: bucket,
      Key: key,
    };
  
    try {
      const data = await s3Client.send(new GetObjectCommand(params)); // Fetch the CSV data from S3
      const readableStream = Readable.from(data.Body); // Create a readable stream from the S3 object
      let batch = [];
      let totalProducts = 0;
  
      await streamPipeline(
        readableStream,
        csvParser(),
        async function* (source) {
          for await (const chunk of source) {
            const normalizedData = Object.keys(chunk).reduce((acc, key) => {
              acc[key.trim().toLowerCase().replace(/\s+/g, "_")] = chunk[key];
              return acc;
            }, {});
  
            batch.push(normalizedData);
            totalProducts++;
  
            if (batch.length >= batchSize) {
              await processBatchFunction(batch, totalProducts - batch.length, totalProducts, key);
              batch = [];
            }
          }
  
          if (batch.length > 0) {
            await processBatchFunction(batch, totalProducts - batch.length, totalProducts, key);
          }
        }
      );
  
      logger.info(`CSV reading and processing completed successfully for file: "${key}", total products: ${totalProducts}`);
    } catch (error) {
      logErrorToFile(`Error in readCSVAndProcess for file "${key}" in bucket "${bucket}": ${error.message}`);
    }
  };

module.exports = {
  getLatestFolderKey,
  processCSVFilesInLatestFolder,
};
