// batch-helpers.js
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

// Function to process a batch of products using WooCommerce Bulk API
const processBatch = async (batch, startIndex, totalProducts, fileKey) => {

    let productId;

    // Prepare an array to hold products to update in the bulk request
    const productsToUpdate = await Promise.all(
        batch.map(async (item, index) => {
            const currentIndex = startIndex + index + 1;
            const partNumber = item.part_number;

            // Check if part_number exists, if not, skip this item
            if (!partNumber) {
                const message = `Item at index ${currentIndex} in file "${fileKey}" skipped: Missing 'part_number'`;
                logErrorToFile(message);
                return null;
            }

            logger.info(`Processing item ${currentIndex} / ${totalProducts} - Part Number: ${partNumber} in file: "${fileKey}"`);

            try {
                // Fetch product ID by part number
                productId = await getProductByPartNumber(partNumber, currentIndex, totalProducts, fileKey);
                if (!productId) {
                    const message = `Product ID not found for Part Number ${partNumber} at index ${currentIndex} in file "${fileKey}"`;
                    logErrorToFile(message);
                    return null;
                }

                const currentProductData = await getProductById(productId, fileKey);

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

                logger.info(`No update needed for Part Number ${partNumber} (id: ${productId}) at index ${currentIndex} / ${totalProducts} in file "${fileKey}"`);
                return null;
                
            } catch (error) {
                const message = `Error processing Part Number ${partNumber} (id: ${productId}) at index ${currentIndex} / ${totalProducts} in file "${fileKey}": ${error.message}`;
                logErrorToFile(message);
                return null;
            }
        })
    );
  
    // Filter out any null entries (skipped products) from the array
    const filteredProducts = productsToUpdate.filter(Boolean);
    if (filteredProducts.length) {
        try {
            const jobId = `processBatch-${productId}-${fileKey}`;
            await limiter.schedule(
                { id: jobId, context: { file: "batch-helpers.js", function: "processBatch", line: 12 } }, 
                () => wooApi.put("products/batch", { update: filteredProducts })
            );
            logger.info(`Batch update successful for ${filteredProducts.length} products in file: "${fileKey}"`);
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
