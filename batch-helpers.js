const { logger, logErrorToFile, logUpdatesToFile } = require("./logger");
const { wooApi, getProductById, getProductByPartNumber, limiter, retriedProducts } = require("./woo-helpers");

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
  
// Function to check if product update is needed
const isUpdateNeeded = (currentData, newData, currentIndex, totalProducts, partNumber, fileName) => {
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
                const currentMeta = currentValue.find(meta => meta.key === newMeta.key);

                if (!currentMeta) {
                    logger.warn(`DEBUG: Key '${newMeta.key}' missing in currentData meta_data for Part Number: ${partNumber} in file ${fileName}. Marking for update. \n`);
                    fieldsToUpdate.push(`meta_data.${newMeta.key}`);
                    return true;
                }

                if (normalizeText(currentMeta.value) !== normalizeText(newMeta.value)) {
                    fieldsToUpdate.push(`meta_data.${newMeta.key}`);
                    //logger.warn(`DEBUG: Mismatch for meta_data key '${newMeta.key}' for Part Number: ${partNumber} in ${fileName}.\nCurrent: '${currentMeta.value}', New: '${newMeta.value}' \n`);

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
                logUpdatesToFile(`Update needed for key '${key}' for Part Number: ${partNumber} in ${fileName}. \nCurrent value: '${currentValue}', \nNew value: '${newValue}' \n`);
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
            
            //logger.info(`DEBUG: Update needed for field '${field}' in Part Number: ${partNumber}. Current value: '${currentFieldValue}', New value: '${newFieldValue}'`);
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
            { key: "product_description", value: item.product_description }, // CSV field
            { key: "short_description", value: item.product_description }, // Mapped to WooCommerce's short_description
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
            ["manufacturer", "spq", "image_url", "datasheet_url", "series_url", "series", "quantity", "operating_temperature", "voltage", "package", "supplier_device_package", "mounting_type", "product_description", "short_description" ,"detail_description", "additional_key_information"].includes(meta.key)
        ),
    };
};

// Function to process a batch of products using WooCommerce Bulk API
const processBatch = async (batch, startIndex, totalProducts, fileKey) => {
    const MAX_RETRIES = 5;
    let attempts = 0;

    logger.info(`Processing batch with startIndex: ${startIndex}, totalProducts: ${totalProducts}, fileKey: ${fileKey}`);
    logger.debug(`Batch data: ${JSON.stringify(batch, null, 2)}`); // Log the full batch data for debugging

    if (!Array.isArray(batch)) {
        throw new Error(`Expected batch to be an array, but got ${typeof batch}`);
    }

    // Array to collect products that need updating
    const productsToUpdate = await Promise.all(
        batch.map(async (item, index) => {
            const currentIndex = startIndex + index + 1;

            logger.info(`Processing batch for file: ${fileKey}, startIndex: ${startIndex}, totalProducts: ${totalProducts}`);

            // Check if 'part_number' exists in the item
            if (!item.hasOwnProperty('part_number') || !item.part_number) {
                const msg = `part_number key is missing in item at index ${currentIndex}, Skip this item.`;
                logger.error(msg);
                logErrorToFile(`Skipped product at index ${currentIndex} / ${totalProducts} in ${fileKey}: ${msg}`);

                return null;
            }

            const part_number = item.part_number;
            logger.info(`Processing ${currentIndex} / ${totalProducts} - Part Number: ${part_number}`);

            try {
                const productId = await getProductByPartNumber(part_number, currentIndex, totalProducts, fileKey);

                if (productId) {
                    // Fetch current product data
                    const product = await getProductById(productId, fileKey);
                    //logger.info(`DEBUG: Current meta_data for Product ID ${productId}: ${JSON.stringify(product.meta_data, null, 2)}`);
                    if (product) {
                        // Prepare new data structure for comparison and potential update
                        const newData = createNewData(item, productId, part_number);
                        const currentData = filterCurrentData(product);

                        // Check if an update is needed
                        if (isUpdateNeeded(currentData, newData, currentIndex, totalProducts, part_number, fileKey)) {
                            return newData; // Include only if an update is needed
                        } 
                    }
                } else {
                    logger.info(`Product ID not found for Part Number: ${part_number} at index ${currentIndex}`);
                }
            } catch (error) {
                const errorMsg = `Error processing Part Number ${part_number} at index ${currentIndex}: ${error.message}`;
                logger.error(errorMsg);
                logErrorToFile(errorMsg, error);
            }
            return null; // Skip products that don't need updating or encountered an error
        })
    );

    // Filter out any null entries (products that don't need updates)
    const filteredProducts = productsToUpdate.filter(Boolean);

    if (filteredProducts.length > 0) {
        while (attempts < MAX_RETRIES) {
            try {
                // Use WooCommerce Bulk API to update products
                const response = await limiter.schedule(
                    { id: `batch-${fileKey}`, context: { file: "batch-helpers.js", function: "processBatch" }},
                    () => wooApi.put("products/batch", { update: filteredProducts })
                );

                // Log the response to verify successful updates
                logger.info(`Batch update successful for ${filteredProducts.length} products in file: "${fileKey}"`);

                filteredProducts.forEach(product => logUpdatesToFile(`Updated: Product ID ${product.id} | Part Number: ${product.part_number} | Source File: ${fileKey}\n`));
                return;  // Exit the retry loop if successful
            } catch (error) {
                attempts++;

                // Log all part numbers in the failed batch
                const failedPartNumbers = filteredProducts.map(p => `[ Part Number: ${p.part_number}, ID: ${p.id} ]`).join("; ");

                logErrorToFile(`Batch update attempt ${attempts} failed for file "${fileKey}" due to: ${error.message}, error`);
                logErrorToFile(`Products in batch - ${failedPartNumbers}`);
                
                if (error.response && error.response.status === 524 && attempts < MAX_RETRIES) {
                    const delay = Math.pow(2, attempts) * 1000; // Exponential backoff: 2, 4, 8 seconds, etc.
                    logger.warn(`524 Timeout detected. Retrying after ${delay / 1000} seconds...`);
                    await new Promise(resolve => setTimeout(resolve, delay));
                } else if (attempts >= MAX_RETRIES) {
                    logErrorToFile(`Batch update failed permanently after ${MAX_RETRIES} attempts for file "${fileKey}"`);
                    throw new Error(`Batch update permanently failed for file "${fileKey}"`);
                } else {
                    throw error; // Exit and propagate any other errors
                }
            }
        }
    } else {
        logger.info(`No valid products to update in the batch for file: "${fileKey}"; filteredProducts.length: ${filteredProducts.length}`);
    }  
};

module.exports = {
  normalizeText,
  isUpdateNeeded,
  processBatch,
};