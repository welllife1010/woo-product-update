const fs = require("fs");
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
        if (key === "part_number") return false;

        let newValue = newData[key];
        let currentValue = currentData[key];

        // logger.warn(`DEBUG: Complete current meta_data for Part Number ${partNumber}: ${JSON.stringify(currentValue, null, 2)}`);
        // logger.warn(`DEBUG: Complete new meta_data for Part Number ${partNumber}: ${JSON.stringify(newValue, null, 2)}`);

        // Handle meta_data specifically, as it is an array of objects
        if (key === "meta_data") {
            //logger.info(`DEBUG: Comparing meta_data values for update check for Part Number: ${partNumber}`);

            if (!Array.isArray(newValue) || !Array.isArray(currentValue)) {
                logger.info(`DEBUG: meta_data is not an array in either current or new data for Part Number: ${partNumber}.`);
                return true;
            }

            for (const newMeta of newValue) {
                const currentMeta = currentValue.find(meta => meta.key === newMeta.key);

                if (currentMeta && normalizeText(currentMeta.value) !== normalizeText(newMeta.value)) {
                    logger.info(`DEBUG: Mismatch for key '${newMeta.key}'. Current: '${currentMeta.value}', New: '${newMeta.value}'`);
                    return true;
                }

                if (!currentMeta) {
                    logger.info(`DEBUG: Key '${newMeta.key}' not found in current meta_data for Part Number: ${partNumber}. Marking for update.`);
                    return true; // Trigger update if key is missing
                }
        
                //logger.info(`DEBUG: Found key '${newMeta.key}' in both new and current meta_data.`); 
            }

            logger.info(`DEBUG: No changes needed for meta_data in Part Number: ${partNumber}.`);
            return false;
        }

        // Normalize text for general string fields
        if (typeof newValue === "string") {
            newValue = normalizeText(newValue);
            currentValue = currentValue ? normalizeText(currentValue) : "";
        }

        // Check if values are different or if current value is undefined
        if (currentValue === undefined || currentValue !== newValue) {
            logger.info(`DEBUG: Update needed for key '${key}' in Part Number: ${partNumber}. Current value: '${currentValue}', New value: '${newValue}'`);
            return true;
        }

        return false; // No difference for this key
    });

    if (!updateNeeded) {
        logger.info(`DEBUG: No update needed for Part Number: ${partNumber}.`);
    }

    return updateNeeded;
};

// Function to process a batch of products using WooCommerce Bulk API
const processBatch = async (batch, startIndex, totalProducts, fileKey, updatedProductsFile) => {
    // Array to collect products that need updating
    const productsToUpdate = await Promise.all(
        batch.map(async (item, index) => {
            const currentIndex = startIndex + index + 1;

            // Log each key and its value
            // Object.keys(item).forEach((key) => {
            //     logger.debug(`Key: ${key}, Value: ${item[key]}`);
            //     logger.info(`Key: ${key}, Value: ${item[key]}`);
            // });

            // Check if 'part_number' exists in the item
            if (!item.hasOwnProperty('part_number') || !item.part_number) {
                logger.error(`part_number key is missing in item at index ${currentIndex}`);
                return null; // Skip this item
            }

            const part_number = item.part_number;
            logger.info(`Processing ${currentIndex} / ${totalProducts} - Part Number: ${part_number}`);

            try {
                // Fetch product ID by part number
                const productId = await getProductByPartNumber(part_number, currentIndex, totalProducts, fileKey);

                if (productId) {
                    // Fetch current product data
                    const product = await getProductById(productId, fileKey);
                    //logger.info(`DEBUG: Current meta_data for Product ID ${productId}: ${JSON.stringify(product.meta_data, null, 2)}`);
                    if (product) {
                        // Prepare new data structure for comparison and potential update
                        const newData = {
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

                        const currentData = {
                            sku: product.sku,
                            description: product.description,
                            meta_data: product.meta_data.filter((meta) =>
                                ["manufacturer", "spq", "image_url", "datasheet_url", "series_url", "series", "quantity", "operating_temperature", "voltage", "package", "supplier_device_package", "mounting_type", "product_description", "short_description" ,"detail_description", "additional_key_information"].includes(meta.key)
                            ),
                        };

                        // Check if an update is needed
                        if (isUpdateNeeded(currentData, newData, currentIndex, totalProducts, part_number)) {
                            return newData; // Include only if an update is needed
                        } else {
                            logger.info(`No update needed for Part Number ${part_number} (Product ID: ${productId})`);
                            return null;
                        }
                    }
                } else {
                    logger.info(`Product ID not found for Part Number: ${part_number} at index ${currentIndex}`);
                }
            } catch (error) {
                logger.error(`Error processing Part Number ${part_number} at index ${currentIndex}: ${error.message}`);
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
                { 
                    id: jobId, 
                    context: { file: "batch-helpers.js", function: "processBatch" } 
                },
                () => wooApi.put("products/batch", { update: filteredProducts })
            );

            // Log the response to verify successful updates
            logger.info(`Batch update successful for ${filteredProducts.length} products in file: "${fileKey}"`);
            
            // Log each updated product's details to the unique updatedProductsFile
            filteredProducts.forEach((product) => {
                fs.appendFileSync(
                    updatedProductsFile,
                    `Updated: Product ID ${product.id} | Part Number: ${product.part_number} | Source File: ${fileKey}\n`
                );
                logger.info(`Product ID ${product.id} (${product.part_number}) updated successfully.`);
            });
            // response.data.update.forEach((product) => {
            //     if (product.id) {
            //         logger.info(`Product ID ${product.id}, Part Number ${product.name} updated successfully.`);
            //     }
            // });
        } catch (error) {
            // Log all part numbers in the failed batch
            //const failedPartNumbers = filteredProducts.map(p => p.part_number).join(", ");
            const failedPartNumbers = filteredProducts.map(p => `Part Number: ${p.part_number}, ID: ${p.id}`).join("; ");
            logErrorToFile(`Batch update failed for file "${fileKey}": ${error.message}. Products in batch: ${failedPartNumbers}`);
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