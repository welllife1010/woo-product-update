const WooCommerceRestApi = require("woocommerce-rest-ts-api").default;
const fs = require("fs");
const { pipeline } = require('stream');
const { promisify } = require('util');
const csvParser = require('csv-parser');
const path = require("path");
const pino = require("pino");
const pinoPretty = require("pino-pretty");
const dotenv = require("dotenv");
const Bottleneck = require("bottleneck");
let stripHtml;
(async () => {
  stripHtml = (await import("string-strip-html")).stripHtml;
})();
const AWS = require("aws-sdk");

// To use dotenv with import (ES Module)
dotenv.config();

// Logger setup with pino-pretty
const logger = pino(
  pinoPretty({
    levelFirst: true,
    colorize: true,
    translateTime: "SYS:standard",
  })
);

// WooCommerce API credentials
const wooApi = new WooCommerceRestApi({
  url: process.env.WOO_API_BASE_URL_DEV,
  consumerKey: process.env.WOO_API_CONSUMER_KEY_DEV,
  consumerSecret: process.env.WOO_API_CONSUMER_SECRET_DEV,
  version: "wc/v3",
});

// Create a Bottleneck instance with appropriate settings
const limiter = new Bottleneck({
  maxConcurrent: 15,  // Number of concurrent requests allowed
  minTime: 300       // Minimum time between requests (in milliseconds)
});

// Configure retry options to handle 504 or 429 errors
limiter.on("failed", async (error, jobInfo) => {
  if (jobInfo.retryCount < 5 && (error.message.includes("502") || error.message.includes("504") || error.message.includes("429"))) {
    console.log(`Retrying job ${jobInfo.options.id} after failure. Retry count: ${jobInfo.retryCount}`);
    return 1000 * Math.pow(2, jobInfo.retryCount); // Exponential backoff: 1s, 2s, 4s, 8s, 16s
  }
});

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

// AWS S3 setup
const s3 = new AWS.S3();

// Function to read CSV from S3 and return data as JSON
// const readCSV = async (bucket, key) => {
//   return new Promise((resolve, reject) => {
//     const results = [];
//     const params = {
//       Bucket: bucket,
//       Key: key,
//       // Bucket: "woocommerce-product-data-for-auto-update",
//       // Key: "product-suntsu-sample-2.csv",
//     };

//     console.log(`Test Params: ${JSON.stringify(params)}`);

//     s3.getObject(params, (err, data) => {
//       if (err) {
//         reject(err);
//       } else {
//         const stream = data.Body.toString('utf-8');
//         const readableStream = require('stream').Readable.from(stream);
//         readableStream
//           .pipe(csvParser())
//           .on("data", (data) => {
//             logger.debug(`CSV Row: ${JSON.stringify(data)}`); // Debug log for each row
//             // Convert keys to consistent casing
//             const normalizedData = Object.keys(data).reduce((acc, key) => {
//               acc[key.trim().toLowerCase().replace(/\s+/g, '_')] = data[key];
//               return acc;
//             }, {});
//             results.push(normalizedData);
//           })
//           .on("end", () => {
//             resolve(results);
//           })
//           .on("error", (error) => reject(error));
//       }
//     });
//   });
// };

const streamPipeline = promisify(pipeline);

async function readCSV(bucket, key) {
  return new Promise(async (resolve, reject) => {
    const results = [];
    const params = {
      Bucket: bucket,
      Key: key,
    };

    console.log(`Reading CSV with params: ${JSON.stringify(params)}`);

    try {
      // Get S3 object
      const data = await s3.getObject(params).promise();

      // Create a readable stream from S3 data
      const readableStream = require('stream').Readable.from(data.Body.toString('utf-8'));

      // Use pipeline to manage the readable stream and CSV parsing
      await streamPipeline(
        readableStream,
        csvParser(),
        async (source) => {
          for await (const chunk of source) {
            // Normalize data keys
            const normalizedData = Object.keys(chunk).reduce((acc, key) => {
              acc[key.trim().toLowerCase().replace(/\s+/g, '_')] = chunk[key];
              return acc;
            }, {});
            results.push(normalizedData);
          }
        }
      );

      resolve(results);
    } catch (error) {
      console.error('Error in readCSV function:', error);
      reject(error);
    }
  });
}

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
const updateProduct = async (productId, data, partNumber, currentIndex, totalProducts,) => {
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
    const response = await limiter.schedule(() => wooApi.get("products", {
      search: partNumber,
      per_page: 1,
    }));
    if (response.data.length) {
      logger.info(
        `3. ${currentIndex} / ${totalProducts} - Product ID ${response.data[0].id} found for Part Number ${partNumber}`
      );
      return response.data[0].id;
    } else {
      logger.info(`3. ${currentIndex} / ${totalProducts} - No product found for Part Number ${partNumber}`);
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

// Batch process products
const processBatch = async (batch, startIndex, totalProducts) => {
  await Promise.all(batch.map(async (item, index) => {
    const currentIndex = startIndex + index + 1;

    // Log each key and its value
    Object.keys(item).forEach((key) => {
      logger.debug(`Key: ${key}, Value: ${item[key]}`);
    });

    // Check if 'part_number' exists in the item
    if (!item.hasOwnProperty('part_number')) {
      logger.error(`part_number key is missing in item at index ${currentIndex}`);
      return; // skip this item
    }

    const partNumber = item.part_number;

    logger.info(`1. ${currentIndex} / ${totalProducts}`);
    logger.info(`2. Processing Part Number: ${partNumber || 'undefined'}`);

    try {
      if (partNumber) {
        const productId = await getProductByPartNumber(partNumber, currentIndex, totalProducts);

        if (productId) {
          const product = await getProductById(productId);
          if (product) {
            const newData = {
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

            const currentData = {
              sku: product.sku,
              description: product.description,
              meta_data: product.meta_data.filter((meta) =>
                ["manufacturer", "spq", "image_url", "datasheet_url", "series_url", "series", "quantity", "operating_temperature", "voltage", "product_type", "supplier_device_package", "mounting_type", "product_description", "detail_description", "additional_key_information", "package"].includes(meta.key)
              ),
            };

            if (isUpdateNeeded(currentData, newData, currentIndex, totalProducts, partNumber)) {
              await updateProduct(productId, newData, partNumber, currentIndex, totalProducts,);
            } else {
              logger.info(
                `4. ${currentIndex} / ${totalProducts} - Part Number ${partNumber}, Product ID ${productId} - No update needed`
              );
            }
          }
        } else {
          logger.info(`4. ${currentIndex} / ${totalProducts} - Part Number ${partNumber}, Product ID not found`);
        }
      } else {
        logger.warn(`Warning: ${currentIndex} / ${totalProducts} - Part Number is undefined for item at index ${currentIndex}`);
      }
    } catch (error) {
      logger.error(
        `Error processing Part Number ${partNumber} at index ${currentIndex}: ${error.message}`
      );
    }
  }));
};

// Main function to process CSV and update products in batches
const processCSV = async (bucket, key, batchSize = 20) => {
  const data = await readCSV(bucket, key);
  const totalProducts = data.length;
  logger.info(`Total products to update: ${totalProducts}`);

  for (let i = 0; i < totalProducts; i += batchSize) {
    const batch = data.slice(i, i + batchSize);
    logger.info(
      `Processing batch ${Math.ceil(i / batchSize) + 1} of ${Math.ceil(
        totalProducts / batchSize
      )}`
    );
    await processBatch(batch, i, totalProducts);
  }

  logger.info("CSV processing completed");
};

// Update the handler function as an async function to use `await`
module.exports.handler = async (event) => {
  const bucket = process.env.S3_BUCKET_NAME;
  const key = process.env.S3_OBJECT_KEY;

  console.log(`Environment S3_BUCKET_NAME: ${process.env.S3_BUCKET_NAME}, S3_BUCKET_NAME type: ${typeof process.env.S3_BUCKET_NAME}`);
  console.log(`Environment S3_OBJECT_KEY: ${process.env.S3_OBJECT_KEY}, S3_OBJECT_KEY type: ${typeof process.env.S3_OBJECT_KEY}`);

  // Log bucket and key to check if they are correctly retrieved
  console.log(`Bucket: ${bucket}, Bucket type: ${typeof bucket}`);
  console.log(`Key: ${key}, Key type: ${typeof key}`);

  // Ensure both bucket and key are provided
  if (!bucket || !key) {
    return {
      statusCode: 500,
      body: JSON.stringify('Error: Bucket name or Key is missing from environment variables'),
    };
  }

  try {
    // Call processCSV with the bucket and key
    await processCSV(bucket, key, 100);
    return {
      statusCode: 200,
      body: JSON.stringify('CSV processing completed successfully!'),
    };
  } catch (error) {
    return {
      statusCode: 500,
      body: JSON.stringify(`Error processing CSV: ${error.message}`),
    };
  }
};


// const __filename = fileURLToPath(import.meta.url);
// const __dirname = dirname(__filename);
// const filePath = path.join(__dirname, "csvFiles/product-suntsu-sample-2.csv");

const bucket = process.env.S3_BUCKET_NAME;
const key = process.env.S3_OBJECT_KEY;

processCSV(bucket, key, 100)
  .then(() => {
    logger.info("CSV processing completed successfully");
  })
  .catch((error) => {
    logger.error("Error processing CSV:", error);
  });