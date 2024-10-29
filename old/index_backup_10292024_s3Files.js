// Import required modules
const WooCommerceRestApi = require("woocommerce-rest-ts-api").default;
const fs = require("fs");
const csvParser = require("csv-parser");
const pino = require("pino");
const pinoPretty = require("pino-pretty");
const dotenv = require("dotenv");
const Bottleneck = require("bottleneck");
const { S3Client, ListObjectsV2Command, GetObjectCommand } = require("@aws-sdk/client-s3");
const { promisify } = require("util");
const { Readable, pipeline } = require("stream"); // Promisify the stream pipeline utility
const streamPipeline = promisify(pipeline);
const { performance } = require("perf_hooks"); // Import performance to track time

let stripHtml;
(async () => {
  stripHtml = (await import("string-strip-html")).stripHtml;
})();

// To use dotenv with import (ES Module)
dotenv.config();
console.log("AWS Region:", process.env.AWS_REGION || process.env.AWS_REGION_NAME);

// Start time to track the whole process duration
const startTime = performance.now();

// Logger setup with pino-pretty
const logger = pino(
  pinoPretty({
    levelFirst: true,
    colorize: true,
    translateTime: "SYS:standard",
  })
);

// Error logger setup for error tracking
// const errorLogger = pino(
//   pinoPretty({
//     levelFirst: true,
//     colorize: true,
//     translateTime: "SYS:standard",
//     autoFlush: true, // Force immediate writes to the file
//   }),
//   fs.createWriteStream("error-log.txt", { flags: "a" })
// );

// Function to log skipped items or errors directly to the file
const logErrorToFile = (message) => {
  const formattedMessage = `[${new Date().toISOString()}] ERROR: ${message}\n`;
  fs.appendFileSync("error-log.txt", formattedMessage);
};

// WooCommerce API credentials
const wooApi = new WooCommerceRestApi({
  url: process.env.WOO_API_BASE_URL_DEV,
  consumerKey: process.env.WOO_API_CONSUMER_KEY_DEV,
  consumerSecret: process.env.WOO_API_CONSUMER_SECRET_DEV,
  version: "wc/v3",
});

// Create a Bottleneck instance with appropriate settings
const limiter = new Bottleneck({
  maxConcurrent: 15, // Number of concurrent requests allowed
  minTime: 400, // Minimum time between requests (in milliseconds)
});

// Configure retry options to handle 504 or 429 errors
limiter.on("failed", async (error, jobInfo) => {
  if (jobInfo.retryCount < 5 && /(502|504|429)/.test(error.message)) {
    logger.warn(`Retrying job ${jobInfo.options.id} due to ${error.message}. Retry count: ${jobInfo.retryCount}`);
    return 1000 * Math.pow(2, jobInfo.retryCount); // Exponential backoff
  }
});

// AWS S3 setup (using AWS SDK v3)
const s3Client = new S3Client({ 
  region: process.env.AWS_REGION_NAME,
  endpoint: "https://s3.us-west-1.amazonaws.com", // Use your specific bucket's region
  forcePathStyle: true, // This helps when using custom endpoints
 });

// Function to read and process multiple CSV files from S3
const processCSVFiles = async (bucket, batchSize, processBatchFunction) => {
  try {
    const listParams = { Bucket: bucket };
    const listData = await s3Client.send(new ListObjectsV2Command(listParams));

    // Log the entire listData to inspect its structure
    //logger.info(`listData response: ${JSON.stringify(listData)}`);

    if (!listData.Contents) {
      logErrorToFile(`No contents found in bucket: ${bucket}. Check permissions or bucket policy.`);
      return;
    }

    logger.info(`Bucket ${bucket} found with ${listData.Contents.length} files.`);

    // Check for CSV files specifically
    const csvFiles = listData.Contents.filter((file) => file.Key.toLowerCase().endsWith(".csv"));

    if (csvFiles.length === 0) {
      logErrorToFile("No CSV files found in the bucket.");
      return;
    }

    await Promise.all(
      csvFiles.slice(0, 3).map(async (file) => {
        logger.info(`Processing file: ${file.Key}`);
        await readCSVAndProcess(bucket, file.Key, batchSize, processBatchFunction);
      })
    );

    logger.info("All CSV files have been processed.");
  } catch (error) {
    logErrorToFile(`Error in processCSVFiles function: ${error.message}`);
  }
};

// Function to read CSV from S3 and process in batches directly
const readCSVAndProcess = async (bucket, key, batchSize, processBatchFunction) => {
  const params = {
    Bucket: bucket,
    Key: key,
  };

  //console.log(`Reading CSV with params: ${JSON.stringify(params)}`);

  try {
    const data = await s3Client.send(new GetObjectCommand(params)); // Fetch the CSV data from S3
    const readableStream = Readable.from(data.Body); // Create a readable stream from the S3 object
    let batch = [];
    let totalProducts = 0;

    // Stream pipeline with CSV parsing and batch processing
    await streamPipeline(
      readableStream,
      csvParser(),
      async function* (source) {
        for await (const chunk of source) {
          // Normalize data keys
          const normalizedData = Object.keys(chunk).reduce((acc, key) => {
            acc[key.trim().toLowerCase().replace(/\s+/g, "_")] = chunk[key];
            return acc;
          }, {});

          batch.push(normalizedData);
          totalProducts++;

          if (batch.length >= batchSize) {
            await processBatchFunction(batch, totalProducts - batch.length, totalProducts);
            batch = [];
          }
        }

        // Process the remaining batch if any
        if (batch.length > 0) {
          await processBatchFunction(batch, totalProducts - batch.length, totalProducts);
        }
      }
    );

    logger.info(`CSV reading and processing completed successfully, total products: ${totalProducts}`);
  } catch (error) {
    logErrorToFile(`Error in readCSVAndProcess function: ${error.message}`)
  }
};

// Function to normalize input texts
const normalizeText = (text) => {
  if (!text) return "";

  // Strip HTML tags
  let normalized = stripHtml(text)?.result.trim()|| "";

  // Replace special characters; Normalize whitespace and line breaks
  return normalized.replace(/\u00ac\u00c6/g, "®").replace(/&deg;/g, "°").replace(/\s+/g, " ");

};

// Function to check if product update is needed
const isUpdateNeeded = (currentData, newData, currentIndex, totalProducts, partNumber) => {
  return Object.keys(newData).some((key) => {
    let newValue = newData[key];
    let currentValue = currentData[key];

    // Handle meta_data as an array of objects
    if (key === "meta_data" && Array.isArray(newValue) && Array.isArray(currentValue)) {
      return newValue.some((newMeta) => {
        const currentMeta = currentValue.find((meta) => meta.key === newMeta.key);
        return !currentMeta || normalizeText(newMeta.value) !== normalizeText(currentMeta.value);
      });
    }

    if (typeof newValue === "string") {
      newValue = normalizeText(newValue);
      currentValue = currentValue ? normalizeText(currentValue) : "";
    }

    return currentValue !== newValue;
  });
};

// Function to get product details by product ID
const getProductById = async (productId) => {
  try {
    const response = await limiter.schedule(() => wooApi.get(`products/${productId}`));
    logger.debug(`Fetched Product Data for ID ${productId}: ${JSON.stringify(response.data)}`);
    return response.data;
  } catch (error) {
    logger.error(
      `Error fetching product with ID ${productId}: ${
        error.response ? JSON.stringify(error.response.data) : error.message
      }`
    );
    return null;
  }
};

// Function to find product ID by custom field "part_number"
const getProductByPartNumber = async (partNumber, currentIndex, totalProducts) => {
  try {
    const response = await limiter.schedule(() =>
      wooApi.get("products", {
        search: partNumber,
        per_page: 1,
      })
    );
    if (response.data.length) {
      logger.info(
        `2. ${currentIndex} / ${totalProducts} - Product ID ${response.data[0].id} found for Part Number ${partNumber}`
      );
      return response.data[0].id;
    } else {
      logger.info(`2. ${currentIndex} / ${totalProducts} - No product found for Part Number ${partNumber}`);
      return null;
    }
  } catch (error) {
    logger.error(
      `Error fetching product with Part Number ${partNumber}: ${
        error.response ? JSON.stringify(error.response.data) : error.message
      }`
    );
    return null;
  }
};

// Function to process a batch of products using WooCommerce Bulk API
const processBatch = async (batch, startIndex, totalProducts) => {
  // Prepare an array to hold products to update in the bulk request
  const productsToUpdate = await Promise.all(
    batch.map(async (item, index) => {
      const currentIndex = startIndex + index + 1;
      const partNumber = item.part_number;

      // Check if part_number exists, if not, skip this item
      if (!partNumber) {
        const message = `Item at index ${currentIndex} skipped: Missing 'part_number'`;
        //logger.warn(message);
        logErrorToFile(message);
        return null;
      }

      logger.info(`Processing item ${currentIndex} / ${totalProducts} - Part Number: ${partNumber}`);

      try {
        // Fetch product ID by part number
        const productId = await getProductByPartNumber(partNumber, currentIndex, totalProducts);
        if (!productId) {
          const message = `Product ID not found for Part Number ${partNumber} at index ${currentIndex}`;
          //logger.warn(message);
          logErrorToFile(message);
          return null;
        }

        const currentProductData = await getProductById(productId);

        // Define the new data structure with WooCommerce Bulk API format
        const newData = {
          id: productId, // Required for Bulk API
          sku: item.sku,
          description: item.product_description,
          meta_data: [
            { key: "spq", value: item.spq },
            { key: "manufacturer", value: item.manufacturer },
            { key: "image_url", value: item.image_url },
            { key: "datasheet_url", value: item.datasheet_url },
            { key: "series_url", value: item.series_url },
            { key: "series", value: item.series },
            { key: "quantity", value: item.quantity },
            { key: "operating_temperature", value: item.operating_temp },
            { key: "voltage", value: item.supply_voltage },
            { key: "package", value: item.packaging_type },
            { key: "supplier_device_package", value: item.supplier_device_package },
            { key: "mounting_type", value: item.mounting_type },
            { key: "product_description", value: item.product_description },
            { key: "detail_description", value: item.long_description },
            { key: "additional_key_information", value: item.additional_info },
          ],
        };

        if (isUpdateNeeded(currentProductData, newData, currentIndex, totalProducts, partNumber)) {
          return newData; // Include only if an update is needed
        }

        logger.info(`No update needed for product ID ${productId} at index ${currentIndex}`);
        return null;
        
      } catch (error) {
        const message = `Error processing Part Number ${partNumber} at index ${currentIndex}: ${error.message}`;
        logErrorToFile(message);
        return null;
      }
    })
  );

  // Filter out any null entries (skipped products) from the array
  const filteredProducts = productsToUpdate.filter(Boolean);
  if (filteredProducts.length) {
    try {
      await limiter.schedule(() => wooApi.put("products/batch", { update: filteredProducts }));
      logger.info(`Batch update successful for ${filteredProducts.length} products`);
    } catch (error) {
      logErrorToFile(`Batch update failed: ${error.message}`);
    }
  } else {
    logger.info("No valid products to update in this batch.");
  }
};


// Main process function, e.g., processCSVFiles()
const mainProcess = async () => {
  try {
    const bucket = process.env.S3_BUCKET_NAME;
    if (!bucket) {
      logErrorToFile("Environment variable S3_BUCKET_NAME is not set.");
      return;
    }
    
    logger.info(`Starting process for bucket: ${bucket}`);
    await processCSVFiles(bucket, 100, processBatch);

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

// Run the main process
mainProcess();