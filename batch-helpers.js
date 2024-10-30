const { logger, logErrorToFile } = require("./logger");
const { wooApi, getProductById, getProductByPartNumber, limiter } = require("./woo-helpers");

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
const isUpdateNeeded = (currentData, newData, currentIndex, totalProducts, partNumber) => {
    const updateNeeded = Object.keys(newData).some((key) => {
        if (key === "id") return false; // Skip 'id' field

        let newValue = newData[key];
        let currentValue = currentData[key];

        // Handle meta_data specifically, as it is an array of objects
        if (key === "meta_data") {
            logger.info(`DEBUG: Comparing meta_data values for update check for Part Number: ${partNumber}`);

            if (!Array.isArray(newValue) || !Array.isArray(currentValue)) {
                logger.info(`DEBUG: Meta_data is not an array in either current or new data for Part Number: ${partNumber}.`);
                return true;
            }

            for (let i = 0; i < newValue.length; i++) {
                const newMeta = newValue[i];
                const currentMeta = currentValue.find((meta) => meta.key === newMeta.key);

                if (!currentMeta) {
                    logger.info(`DEBUG: Key '${newMeta.key}' not found in current meta_data for Part Number: ${partNumber}.`);
                    return true;
                }

                const newMetaValue = normalizeText(newMeta.value);
                const currentMetaValue = normalizeText(currentMeta.value);

                if (newMetaValue !== currentMetaValue) {
                    logger.info(`DEBUG: Mismatch detected for meta key '${newMeta.key}' in Part Number: ${partNumber}. Current value: '${currentMetaValue}', New value: '${newMetaValue}'`);
                    return true;
                }
            }

            logger.info(`DEBUG: No changes needed for meta_data in Part Number: ${partNumber}.`);
            return false;
        }

        if (typeof newValue === "string") {
            newValue = normalizeText(newValue);
            currentValue = currentValue ? normalizeText(currentValue) : "";
        }

        if (currentValue === undefined || currentValue !== newValue) {
            logger.info(`DEBUG: Update needed for key '${key}' in Part Number: ${partNumber}. Current value: '${currentValue}', New value: '${newValue}'`);
            return true;
        }

        return false;
    });

    if (!updateNeeded) {
        logger.info(`DEBUG: No update needed for Part Number: ${partNumber}.`);
    }

    return updateNeeded;
};

// Function to process a batch of products using WooCommerce Bulk API
const processBatch = async (batch, startIndex, totalProducts, fileKey) => {
    // Array to collect products that need updating
    const productsToUpdate = await Promise.all(
        batch.map(async (item, index) => {
            const currentIndex = startIndex + index + 1;

            // Log each key and its value
            Object.keys(item).forEach((key) => {
                logger.debug(`Key: ${key}, Value: ${item[key]}`);
            });

            // Check if 'part_number' exists in the item
            if (!item.hasOwnProperty('part_number')) {
                logger.error(`part_number key is missing in item at index ${currentIndex}`);
                return null; // Skip this item
            }

            const partNumber = item.part_number;
            logger.info(`Processing ${currentIndex} / ${totalProducts} - Part Number: ${partNumber}`);

            try {
                if (partNumber) {
                    // Fetch product ID by part number
                    const productId = await getProductByPartNumber(partNumber, currentIndex, totalProducts, fileKey);

                    if (productId) {
                        // Fetch current product data
                        const product = await getProductById(productId, fileKey);
                        if (product) {
                            // Prepare new data structure for comparison and potential update
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

                            const currentData = {
                                sku: product.sku,
                                description: product.description,
                                meta_data: product.meta_data.filter((meta) =>
                                    ["manufacturer", "spq", "image_url", "datasheet_url", "series_url", "series", "quantity", "operating_temperature", "voltage", "package", "supplier_device_package", "mounting_type", "product_description", "detail_description", "additional_key_information"].includes(meta.key)
                                ),
                            };

                            // Check if an update is needed
                            if (isUpdateNeeded(currentData, newData, currentIndex, totalProducts, partNumber)) {
                                return newData; // Include only if an update is needed
                            } else {
                                logger.info(`No update needed for Part Number ${partNumber} (Product ID: ${productId})`);
                                return null;
                            }
                        }
                    } else {
                        logger.info(`Product ID not found for Part Number: ${partNumber}`);
                    }
                }
            } catch (error) {
                logger.error(`Error processing Part Number ${partNumber} at index ${currentIndex}: ${error.message}`);
            }
            return null; // Skip products that don't need updating or encountered an error
        })
    );

    // Filter out any null entries (products that don't need updates)
    const filteredProducts = productsToUpdate.filter(Boolean);

    if (filteredProducts.length > 0) {
        try {
            // Use WooCommerce Bulk API to update products
            const jobId = `processBatch-${fileKey}`;
            const response = await limiter.schedule(
                { id: jobId, context: { file: "batch-helpers.js", function: "processBatch" } },
                () => wooApi.put("products/batch", { update: filteredProducts })
            );

            // Log the response to verify successful updates
            logger.info(`Batch update successful for ${filteredProducts.length} products in file: "${fileKey}"`);
            response.data.update.forEach((product) => {
                if (product.id) {
                    logger.info(`Product ID ${product.id} updated successfully.`);
                }
            });
        } catch (error) {
            logErrorToFile(`Batch update failed for file "${fileKey}": ${error.message}`);
        }
    } else {
        logger.info(`No valid products to update in the batch for file: "${fileKey}"`);
    }
};

module.exports = {
  normalizeText,
  isUpdateNeeded,
  processBatch,
};