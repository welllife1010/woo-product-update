const dotenv = require("dotenv");
dotenv.config();

const WooCommerceRestApi = require("woocommerce-rest-ts-api").default;
const Bottleneck = require("bottleneck");
const { logger, logErrorToFile } = require("./logger");

// WooCommerce API credentials
const wooApi = new WooCommerceRestApi({
    url: process.env.WOO_API_BASE_URL_TEST,
    consumerKey: process.env.WOO_API_CONSUMER_KEY_TEST,
    consumerSecret: process.env.WOO_API_CONSUMER_SECRET_TEST,
    version: "wc/v3",
});
  
// Create a Bottleneck instance with appropriate settings
const limiter = new Bottleneck({
    maxConcurrent: 5, // Number of concurrent requests allowed - Limit to 5 concurrent 100-item requests at once
    minTime: 300, // Minimum time between requests (in milliseconds) - 500ms between each request
});

// Define a set to keep track of products that were retried
const retriedProducts = new Set();

// Configure retry options to handle 504 or 429 errors
limiter.on("failed", async (error, jobInfo) => {
    const jobId = jobInfo.options.id || "<unknown>";
    const { file = "<unknown file>", function: functionName = "<unknown function>", part = "<unknown part>" } = jobInfo.options.context || {};
    const retryCount = jobInfo.retryCount || 0;

    logger.warn(`Retrying job ${jobId} for ${functionName} in ${file}. Retry #${retryCount + 1}.`);

    logErrorToFile(
        `Retrying job ${jobId} due to ${error.message}. File: ${file}, Function: ${functionName}. Retry count: ${retryCount}`
    );

    // Add part number to retriedProducts if a retry occurs
    if (partNumber) retriedProducts.add(partNumber);

    if (retryCount < 5 && /(502|504|429)/.test(error.message)) {
        const retryDelay = 1000 * Math.pow(2, jobInfo.retryCount); // Exponential backoff
        logger.warn(`Applying delay of ${retryDelay / 1000}s before retrying job ${jobId}`);
        return retryDelay;
    }

    if (retryCount >= 5) {
        logErrorToFile(`Job ${jobId} failed permanently for part ${part} after maximum retries due to ${error.message}.`);
    }

});

// Function to get product details by product ID
const getProductById = async (productId, fileKey) => {
    try {
        // Schedule with a unique job ID and log details
        const jobId = `getProductById-${productId}-${fileKey}`;
        const response = await limiter.schedule( 
            { 
                id: jobId,
                context: { file: "woo-helpers.js", function: "getProductById" }
             }, 
            () => wooApi.get(`products/${productId}`)
        );
        logger.debug(`Fetched Product Data for ID ${productId}: ${JSON.stringify(response.data)} in file "${fileKey}"`);
        return response.data;
    } catch (error) {
        logger.error(
            `Error fetching product with ID ${productId} in file "${fileKey}": ${
            error.response ? JSON.stringify(error.response.data) : error.message
            }`
        );
        return null;
    }
};
  
// Function to find product ID by custom field "part_number"
const getProductByPartNumber = async (partNumber, currentIndex, totalProducts, fileKey) => {
    try {
        // Schedule with a unique job ID and log details
        const jobId = `getProductByPartNumber-${partNumber}-${fileKey}-${currentIndex}`;
        const response = await limiter.schedule(
            { 
                id: jobId,
                context: { file: "woo-helpers.js", function: "getProductByPartNumber", part: `${partNumber}`}
            }, 
            () =>
            wooApi.get("products", {
                search: partNumber,
                per_page: 1,
            })
        );
        if (response.data.length) {
            logger.info(`${currentIndex} / ${totalProducts} - Product ID ${response.data[0].id} found for Part Number ${partNumber} in file "${fileKey}"`);
            return response.data[0].id;
        } else {
            logger.info(`${currentIndex} / ${totalProducts} - No product found for Part Number ${partNumber} in file "${fileKey}"`);
            logErrorToFile(`${currentIndex} / ${totalProducts} - No product found for Part Number ${partNumber} in file "${fileKey}"`)
            return null;
        }
    } catch (error) {
        logger.error(
            `Error fetching product with Part Number ${partNumber} in file "${fileKey}": ${
                error.response ? JSON.stringify(error.response.data) : error.message
            }`
        );
        logErrorToFile(`Error fetching product with Part Number ${partNumber} in file "${fileKey}": ${
                error.response ? JSON.stringify(error.response.data) : error.message
            }, error`);
        return null;
    }
};


  module.exports = {
    wooApi,
    getProductByPartNumber,
    getProductById,
    limiter,
    retriedProducts
  };