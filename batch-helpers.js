const { performance } = require("perf_hooks"); // Import performance to track time

const { logger, logErrorToFile, logUpdatesToFile, logInfoToFile } = require("./logger");
const { wooApi, getProductById, getProductIdByPartNumber, limiter } = require("./woo-helpers");
const { redisClient } = require('./queue');
const { scheduleApiRequest } = require('./job-manager');
const { createUniqueJobId } = require('./utils');

let stripHtml;
(async () => {
  stripHtml = (await import("string-strip-html")).stripHtml;
})();

// Function to normalize input texts
const normalizeText = (text) => {
    if (!text) return "";

    // Strip HTML tags
    let normalized = stripHtml(text)?.result.trim()|| "";

    // Replace special characters; Normalize whitespace and line breaks
    return normalized.replace(/\u00ac\u00c6/g, "®").replace(/&deg;/g, "°").replace(/\s+/g, " ");
};

function isMetaKeyMissing(newMetaValue, currentMeta) {
    return (!newMetaValue && !currentMeta) || (!newMetaValue && !currentMeta?.value);
}

function isCurrentMetaMissing(newMetaValue, currentMeta) {
    return newMetaValue && !currentMeta;
}

function isMetaValueDifferent(newMetaValue, currentMetaValue) {
    return normalizeText(currentMetaValue) !== normalizeText(newMetaValue);
}

  
// Function to check if product update is needed
const isUpdateNeeded = (currentData, newData, currentIndex, totalProductsInFile, partNumber, fileName) => {
    const fieldsToUpdate = [];

    Object.keys(newData).forEach((key) => {
        if (key === "id" || key === "part_number") return;

        let newValue = newData[key];
        let currentValue = currentData[key];

        // Handle meta_data specifically, as it is an array of objects
        if (key === "meta_data") {
            if (!Array.isArray(newValue) || !Array.isArray(currentValue)) {
                logger.info(`DEBUG: meta_data is not an array in either current or new data for Part Number: ${partNumber} in ${fileName}.`);
                fieldsToUpdate.push(key);
                return true;
            }

            newValue.forEach((newMeta) => {
                const newMetaValue = newMeta.value;
                const currentMeta = currentValue.find(meta => meta.key === newMeta.key);
                const currentMetaValue = currentMeta?.value;

                // if (isMetaKeyMissing(newMetaValue, currentMeta)) {
                //     logInfoToFile(`No update needed for the key '${newMeta.key}'. No meta value for Part Number: ${partNumber} in file ${fileName}. \n`);
                // }
            
                if (isCurrentMetaMissing(newMetaValue, currentMeta)) {
                    logInfoToFile(`DEBUG: Key '${newMeta.key}' missing in currentData meta_data for Part Number: ${partNumber} in file ${fileName}. Marking for update. \n`);
                    fieldsToUpdate.push(`meta_data.${newMeta.key}`);
                    return true;
                }
            
                if (isMetaValueDifferent(newMetaValue, currentMetaValue)) {
                    fieldsToUpdate.push(`meta_data.${newMeta.key}`);
                }
            })
        } else {
            // Normalize and compare general string fields
            if (typeof newValue === "string") {
                newValue = normalizeText(newValue);
                currentValue = currentValue ? normalizeText(currentValue) : "";
            }

            // Check if values are different or if current value is undefined
            if (currentValue === undefined || currentValue !== newValue) {
                fieldsToUpdate.push(key);
                logInfoToFile(`Update needed for key '${key}' for Part Number: ${partNumber} in ${fileName}. \nCurrent value: '${currentValue}', \nNew value: '${newValue}' \n`);
            }
        }
    });

    // Log updates for each field in fieldsToUpdate
    if (fieldsToUpdate.length > 0) {
        fieldsToUpdate.forEach(field => {
            const currentFieldValue = field.startsWith("meta_data.") 
                ? currentData.meta_data?.find(meta => meta.key === field.split(".")[1])?.value 
                : currentData[field];
                
            const newFieldValue = field.startsWith("meta_data.") 
                ? newData.meta_data?.find(meta => meta.key === field.split(".")[1])?.value 
                : newData[field];
            
            //logInfoToFile(`Update needed for field '${field}' in Part Number: ${partNumber}. Current value: '${currentFieldValue}', New value: '${newFieldValue}'`);
        });
        return true;
    } else {
        logger.info(`No update required for Part Number: ${partNumber} in ${fileName}`);
        return false;
    }
};

const createNewData = (item, productId, part_number) => {
    return {
        id: productId, // Required for Bulk API
        part_number, // Attach part number here for later reference
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
            { key: "short_description", value: item.product_description }, // Mapped CSV's product_description field to WooCommerce's short_description custom field
            { key: "detail_description", value: item.long_description },
            { key: "additional_key_information", value: item.additional_info },
        ],
    };
}

const filterCurrentData = (product) => {
    return {
        sku: product.sku,
        description: product.description,
        meta_data: product.meta_data.filter((meta) =>
            ["spq", "manufacturer", "image_url", "datasheet_url", "series_url", "series", "quantity", "operating_temperature", "voltage", "package", "supplier_device_package", "mounting_type", "short_description", "detail_description", "additional_key_information"].includes(meta.key)
        ),
    };
};

// Process a batch of products using WooCommerce Bulk API
const processBatch = async (batch, startIndex, totalProductsInFile, fileKey) => {
    const MAX_RETRIES = 5;
    let attempts = 0;
    const action = 'processBatch';
    const lastProcessedRow = startIndex + batch.length; // Calculate the last processed row for logging

    const batchStartTime = performance.now(); // Track overall batch time
    logInfoToFile(`Starting "processBatch" with startIndex: ${startIndex} | totalProductsInFile: ${totalProductsInFile} | fileKey: ${fileKey}`);

    if (!Array.isArray(batch)) {
        throw new Error(`Expected batch to be an array, but got ${typeof batch}`);
    }

    // Array to collect products that need updating
    const productsToUpdate = await Promise.all(
        batch.map(async (item, index) => {
            const currentIndex = startIndex + index;

            // Ensure we don't go beyond the totalProductsInFile
            if (currentIndex >= totalProductsInFile) return null;

            const part_number = item.part_number;
            if (!part_number) return null;

            logger.info(`Processing ${currentIndex + 1} / ${totalProductsInFile} - Part Number: ${part_number} in ${fileKey}`);

            try {
                // Measure time to fetch Product ID by Part Number
                const productIdStart = performance.now();
                logInfoToFile(`Requesting Product ID for Part Number: ${part_number}, currentIndex: ${currentIndex}`);
                const productId = await getProductIdByPartNumber(part_number, currentIndex, totalProductsInFile, fileKey);
                logInfoToFile(`Received Product ID for Part Number: ${part_number}: ${productId}`);
                const productIdEnd = performance.now();
                logInfoToFile(`Time to fetch product ID for Part Number ${part_number}: ${(productIdEnd - productIdStart).toFixed(2)} ms`);

                if (!productId) {
                    await redisClient.incr(`failed-products:${fileKey}`);
                    return null;
                }

                // Measure time to fetch Product details by Product ID
                const productFetchStart = performance.now();
                const product = await getProductById(productId, fileKey, currentIndex);
                const productFetchEnd = performance.now();
                logInfoToFile(`Time to fetch product details for Product ID ${productId}: ${(productFetchEnd - productFetchStart).toFixed(2)} ms`);

                // Prepare new data structure for comparison and potential update
                const newData = createNewData(item, productId, part_number);
                const currentData = filterCurrentData(product);

                if (product && isUpdateNeeded(currentData, newData, currentIndex, totalProductsInFile, part_number, fileKey)) {
                    return { ...newData, currentIndex, totalProductsInFile }; 
                }

                await redisClient.incr(`skipped-products:${fileKey}`);
                logInfoToFile((`No update needed for Part Number: ${part_number} in ${fileKey}`));
                return null;
                
            } catch (error) {
                logErrorToFile(`Error processing Part Number ${part_number} at index ${currentIndex}: ${error.message}`, error.stack);
                return null;
            }
        })
    );

    // Filter out any null entries (products that don't need updates)
    const filteredProducts = productsToUpdate.filter(Boolean);

    if (filteredProducts.length > 0) {
        while (attempts < MAX_RETRIES) {

            const retryStartTime = performance.now();
            const jobId = createUniqueJobId(fileKey, action, startIndex, attempts);  

            try {
                const apiCallStart = performance.now();

                // Use centralized job scheduling for WooCommerce API batch update
                const response = await scheduleApiRequest(
                    () => wooApi.put("products/batch", { update: filteredProducts }),
                    {   
                        id: jobId,
                        context: { 
                            file: "batch-helpers.js", 
                            functionName: "processBatch", 
                            part: filteredProducts.map(p => p.part_number).join(", ")
                        }
                    }
                );

                const apiCallEnd = performance.now();
                logInfoToFile(`Time for WooCommerce API batch update call: ${(apiCallEnd - apiCallStart).toFixed(2)} ms`);

                await redisClient.incrBy(`updated-products:${fileKey}`, filteredProducts.length); // Increment the count of updated products in Redis

                // Log completion time for the batch update
                const retryEndTime = performance.now();
                logInfoToFile(`Batch update attempt ${attempts + 1} successful in ${(retryEndTime - retryStartTime).toFixed(2)} ms`);

                filteredProducts.forEach(product => 
                    logUpdatesToFile(`Updated: ${product.currentIndex} / ${product.totalProductsInFile} | Product ID ${product.id} | Part Number: ${product.part_number} | Source File: ${fileKey} updated.\n`)
                );

                return response; // Exit the retry loop if successful 
            } catch (error) {
                attempts++;

                const retryEndTime = performance.now();
                logErrorToFile(`Batch update attempt ${attempts} failed. Time taken: ${(retryEndTime - retryStartTime).toFixed(2)} ms. Error: ${error.message}`, error.stack);

                // Log all part numbers in the failed batch
                const failedPartNumbers = filteredProducts.map(p => `[ Part Number: ${p.part_number}, ID: ${p.id} ]`).join("; ");
                logErrorToFile(`Products in batch - ${failedPartNumbers}`);
                    
               if (attempts >= MAX_RETRIES) {
                    await redisClient.incr(`failed-products:${fileKey}`);
                    logErrorToFile(`Batch update failed permanently after ${MAX_RETRIES} attempts for file "${fileKey}" | Failed Part Numbers: ${failedPartNumbers} | Error: ${error.message}`);
                    throw new Error(`Batch update failed permanently after ${MAX_RETRIES} attempts.`);
               }

                const delay = Math.pow(2, attempts) * 1000; // Exponential backoff: 2, 4, 8 seconds, etc.
                logErrorToFile(`Timeout detected. Retrying after ${delay / 1000} seconds...`);
                await new Promise(resolve => setTimeout(resolve, delay));
                
            }
        }
    } else {
        logger.info(`No valid products to update in the batch for file: "${fileKey}"`);
    }
    
    const batchEndTime = performance.now();
    logInfoToFile(`Total time for processBatch (File: ${fileKey}, StartIndex: ${startIndex}): ${(batchEndTime - batchStartTime).toFixed(2)} ms`);
};

module.exports = {
  normalizeText,
  isUpdateNeeded,
  processBatch,
};