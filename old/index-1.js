// Import required modules
const WooCommerceRestApi = require("woocommerce-rest-ts-api").default;
const fs = require("fs");
const csvParser = require("csv-parser");
const pino = require("pino");
const pinoPretty = require("pino-pretty");
const dotenv = require("dotenv");
const Bottleneck = require("bottleneck");
const { Readable } = require("stream");
const { S3Client, ListObjectsV2Command, GetObjectCommand } = require("@aws-sdk/client-s3");
const { pipeline } = require("stream"); // Promisify the stream pipeline utility
const { promisify } = require("util");
const streamPipeline = promisify(pipeline);
const { performance } = require("perf_hooks"); // Import performance to track time

let stripHtml;
(async () => {
  stripHtml = (await import("string-strip-html")).stripHtml;
})();

// To use dotenv with import (ES Module)
dotenv.config();

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
const errorLogger = pino(
  pinoPretty({
    levelFirst: true,
    colorize: true,
    translateTime: "SYS:standard",
    autoFlush: true, // Force immediate writes to the file
  }),
  fs.createWriteStream("error-log.txt", { flags: "a" })
);

// Function to log skipped items or errors directly to the file
const logErrorToFile = (message) => {
  errorLogger.error(message);
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
    const listParams = { Bucket: bucket};
    const listData = await s3Client.send(new ListObjectsV2Command(listParams)); // List CSV files in the bucket
    const csvFiles = listData.Contents.filter((file) => file.Key.endsWith(".csv"));

    // Process CSV files concurrently (up to 3 at a time)
    await Promise.all(
      csvFiles.slice(0, 3).map(async (file) => {
        await readCSVAndProcess(bucket, file.Key, batchSize, processBatchFunction);
      })
    );

    logger.info("All CSV files have been processed.");
  } catch (error) {
    console.error("Error in processCSVFiles function:", error);
    throw error;
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
    console.error("Error in readCSVAndProcess function:", error);
    throw error;
  }
};

// Function to normalize input texts
const normalizeText = (text) => {
  if (!text) return "";

  // Strip HTML tags
  let normalized = stripHtml(text).result.trim();

  // Replace special characters
  normalized = normalized.replace(/\u00ac\u00c6/g, "®").replace(/&deg;/g, "°").trim();

  // Normalize whitespace and line breaks
  normalized = normalized.replace(/\s+/g, " ");

  return normalized;
};

// Function to check if product update is needed
const isUpdateNeeded = (currentData, newData, currentIndex, totalProducts, partNumber) => {
  const updateNeeded = Object.keys(newData).some((key) => {
    let newValue = newData[key];
    let currentValue = currentData[key];

    // Handle meta_data which is an array of objects
    if (key === "meta_data") {
      logger.info(`DEBUG: ${currentIndex} / ${totalProducts}, ${partNumber} -  Comparing meta_data values for update check.`);

      if (!Array.isArray(newValue) || !Array.isArray(currentValue)) {
        logger.info(`DEBUG: ${currentIndex} / ${totalProducts}, ${partNumber} -  Meta_data is not an array in either current or new data.`);
        return true;
      }

      for (let i = 0; i < newValue.length; i++) {
        const newMeta = newValue[i];
        const currentMeta = currentValue.find((meta) => meta.key === newMeta.key);
        if (!currentMeta) {
          logger.info(`DEBUG: ${currentIndex} / ${totalProducts}, ${partNumber} - Key '${newMeta.key}' not found in current meta_data.`);
          return true; // Key missing in current data, update is needed
        }

        // Normalize values for meta_data and compare
        const newMetaValue = normalizeText(newMeta.value);
        const currentMetaValue = normalizeText(currentMeta.value);

        if (newMetaValue !== currentMetaValue) {
          logger.info(`DEBUG: ${currentIndex} / ${totalProducts}, ${partNumber} - Mismatch detected for meta key '${newMeta.key}'. Current value: '${currentMetaValue}', New value: '${newMetaValue}'`);
          return true; // Values differ, update is needed
        }
      }

      logger.info(`DEBUG: ${currentIndex} / ${totalProducts}, ${partNumber} - No changes needed for meta_data.`);
      return false;
    }

    // Normalize text for general string fields (not WYSIWYG fields)
    if (typeof newValue === "string") {
      newValue = normalizeText(newValue);
      currentValue = currentValue ? normalizeText(currentValue) : "";
    }

    if (currentValue === undefined || currentValue !== newValue) {
      logger.info(`DEBUG: ${currentIndex} / ${totalProducts}, ${partNumber} - Update needed for key '${key}'. Current value: '${currentValue}', New value: '${newValue}'`);
      return true;
    }

    return false;
  });

  if (!updateNeeded) {
    logger.info(`DEBUG: ${currentIndex} / ${totalProducts}, ${partNumber} - No update needed for product.`);
  }

  return updateNeeded;
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

// Function to update product on WooCommerce
const updateProduct = async (productId, data, partNumber, currentIndex, totalProducts) => {
  try {
    const response = await limiter.schedule(() => wooApi.put(`products/${productId}`, data));
    if (response.data) {
      logger.info(
        `4. ${currentIndex} / ${totalProducts} - Part Number ${partNumber}, Product ID ${productId} updated successfully`
      );
    } else {
      logger.info(
        `4. ${currentIndex} / ${totalProducts} - Part Number ${partNumber}, Product ID ${productId} - No changes applied`
      );
    }
    return response.data;
  } catch (error) {
    logger.error(
      `4. ${currentIndex} / ${totalProducts} - Part Number ${partNumber}, Product ID ${productId} failed to update: ${
        error.response ? JSON.stringify(error.response.data) : error.message
      }`
    );
    throw error;
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
        logger.warn(message);
        logErrorToFile(message);
        return null;
      }

      logger.info(`Processing item ${currentIndex} / ${totalProducts} - Part Number: ${partNumber}`);

      try {
        // Fetch product ID by part number
        const productId = await getProductByPartNumber(partNumber, currentIndex, totalProducts);
        if (!productId) {
          const message = `Product ID not found for Part Number ${partNumber} at index ${currentIndex}`;
          logger.warn(message);
          logErrorToFile(message);
          return null;
        }

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

        return newData; // Return the product data to be included in the bulk update
      } catch (error) {
        const message = `Error processing Part Number ${partNumber} at index ${currentIndex}: ${error.message}`;
        logger.error(message);
        logErrorToFile(message);
        return null;
      }
    })
  );

  // Filter out any null entries (skipped products) from the array
  const filteredProducts = productsToUpdate.filter((product) => product !== null);

  if (filteredProducts.length > 0) {
    try {
      // Send a bulk update request with WooCommerce Bulk API
      const response = await limiter.schedule(() =>
        wooApi.put("products/batch", { update: filteredProducts })
      );

      if (response.data) {
        logger.info(`Batch update successful for ${filteredProducts.length} products`);
      } else {
        logger.warn(`Batch update request did not return data for ${filteredProducts.length} products`);
      }
    } catch (error) {
      const message = `Batch update failed: ${error.message}`;
      logger.error(message);
      logErrorToFile(message);
    }
  } else {
    logger.info("No valid products to update in this batch.");
  }
};

// Start processing CSV files
// const bucket = process.env.S3_BUCKET_NAME;
// processCSVFiles(bucket, 100, processBatch)
//   .then(() => {
//     logger.info("All CSV files processing completed successfully");
//   })
//   .catch((error) => {
//     logger.error("Error processing CSV files:", error);
//   });


// Main process function, e.g., processCSVFiles()
const mainProcess = async () => {
  try {
    // Your existing processing logic
    const bucket = process.env.S3_BUCKET_NAME;
    await processCSVFiles(bucket, 100, processBatch);

    // Record completion message and elapsed time
    const endTime = performance.now();
    const duration = ((endTime - startTime) / 1000).toFixed(2); // Duration in seconds
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