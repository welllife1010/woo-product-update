const dotenv = require("dotenv");
dotenv.config();

const WooCommerceRestApi = require("woocommerce-rest-ts-api").default;
const Bottleneck = require("bottleneck");
const { logger, logErrorToFile } = require("./logger");

// Function to get WooCommerce API credentials based on execution mode
const getWooCommerceApiCredentials = (executionMode) => {
    return (executionMode === 'development') ? {
        url: process.env.WOO_API_BASE_URL_DEV,
        consumerKey: process.env.WOO_API_CONSUMER_KEY_DEV,
        consumerSecret: process.env.WOO_API_CONSUMER_SECRET_DEV,
        version: "wc/v3",
        timeout: 300000 // Set a longer timeout (in milliseconds)
    } : {
        url: process.env.WOO_API_BASE_URL,
        consumerKey: process.env.WOO_API_CONSUMER_KEY,
        consumerSecret: process.env.WOO_API_CONSUMER_SECRET,
        version: "wc/v3",
        timeout: 300000
    };
};

const wooApi = new WooCommerceRestApi(getWooCommerceApiCredentials(process.env.EXECUTION_MODE));

// Create a Bottleneck instance with appropriate settings
const limiter = new Bottleneck({
    maxConcurrent: 2, // Number of concurrent requests allowed - Limit to 2 concurrent 100-item requests at once
    minTime: 1000, // Minimum time between requests (in milliseconds) - 500ms between each request
});

// Define a set to keep track of products that were retried
const retriedProducts = new Set();

// Configure retry options to handle 504 or 429 errors
limiter.on("failed", async (error, jobInfo) => {
    const jobId = jobInfo.options.id || "<unknown>";
    const { file = "<unknown file>", functionName = "<unknown function>", part = "<unknown part>" } = jobInfo.options.context || {};
    const retryCount = jobInfo.retryCount || 0;

    logErrorToFile(
        `Retrying job ${jobId} due to ${error.message}. File: ${file}, Function: ${functionName}. Retry count: ${retryCount + 1}`
    );

    // Add part number to retriedProducts if a retry occurs
    if (part) retriedProducts.add(part);

    if (retryCount < 5 && /(ECONNRESET|socket hang up|502|504|429)/.test(error.message)) {
        const retryDelay = 1000 * Math.pow(2, jobInfo.retryCount); // Exponential backoff
        logger.warn(`Applying delay of ${retryDelay / 1000}s before retrying job ${jobId}`);
        logErrorToFile(`Retrying job due to ${error.message}. Retry count: ${retryCount + 1}`);
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
                context: { 
                    file: "woo-helpers.js", 
                    functionName: "getProductById", 
                    part: `${productId}`
                }
             }, 
            () => wooApi.get(`products/${productId}`)
        );
        //logger.debug(`Fetched Product Data for ID ${productId}: ${JSON.stringify(response.data)} in file "${fileKey}"`);
        return response.data;
    } catch (error) {
        logger.error(
            `Error fetching product with ID ${productId} in file "${fileKey}": ${
            error.response ?? error.message
            }`
        );
        return null;
    }
};
  
// Find product ID by custom field "part_number"
const getProductIdByPartNumber = async (partNumber, currentIndex, totalProducts, fileKey) => {
    try {
        // Schedule with a unique job ID and log details
        const jobId = `getProductIdByPartNumber-${partNumber}-${fileKey}-${currentIndex}`;
        const response = await limiter.schedule(
            { 
                id: jobId,
                context: { 
                    file: "woo-helpers.js", 
                    function: "getProductIdByPartNumber", 
                    part: `${partNumber}`
                }
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
            logErrorToFile(`${currentIndex} / ${totalProducts} - No product found for Part Number ${partNumber} in file "${fileKey}"`)
            return null;
        }
    } catch (error) {
        logErrorToFile(`Error fetching product with Part Number ${partNumber} in file "${fileKey}": ${error.message}`, error.stack);
        return null;
    }
};


  module.exports = {
    wooApi,
    getProductIdByPartNumber,
    getProductById,
    limiter,
    retriedProducts
  };