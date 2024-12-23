const fs = require("fs");
const path = require("path");
const pino = require("pino");
const pinoPretty = require("pino-pretty");
const dayjs = require('dayjs');
const utc = require('dayjs/plugin/utc');
const timezone = require('dayjs/plugin/timezone');
const { redisClient } = require('./queue');


// Create the output-files directory if it doesn't exist
const outputDir = path.join(__dirname, "output-files");
if (!fs.existsSync(outputDir)) {
    fs.mkdirSync(outputDir);
}

// Get a file path within the output-files directory
const getOutputFilePath = (filename) => path.join(outputDir, filename);

// Generate the unique filename with date and increment version dynamically if file exists
const date = new Date().toISOString().split("T")[0]; // YYYY-MM-DD format

// Generate the current date and time in PST
const getPSTDate = () => {
    return new Date().toLocaleString("en-US", {
        timeZone: "America/Los_Angeles",
        year: "numeric",
        month: "2-digit",
        day: "2-digit",
        hour: "2-digit",
        minute: "2-digit",
        second: "2-digit",
        hour12: true, // Optional: change to false for 24-hour format
    });
};

// Generate a file-safe timestamp (e.g., YYYY-MM-DD_HH-MM-SS)
const getFileSafePSTDate = () => {
    const date = new Date().toLocaleString("en-US", {
        timeZone: "America/Los_Angeles",
        year: "numeric",
        month: "2-digit",
        day: "2-digit",
        hour: "2-digit",
        minute: "2-digit",
        second: "2-digit",
        hour12: false, // 24-hour format
    });

    // Replace characters to make it file-name safe
    return date.replace(/[/, ]/g, "-").replace(/:/g, "-");
};

// Generate a unique file name by checking for existing files
const generateFileName = (fileName = "log-info") => {
    const creationTimestamp = getFileSafePSTDate(); // Capture creation timestamp
    let version = 1;
    let generatedFileName;

    do {
        generatedFileName = `${fileName}-${creationTimestamp}-v${version}.txt`;
        version += 1;
    } while (fs.existsSync(getOutputFilePath(generatedFileName)));
    
    return generatedFileName;
};

let updatedProductsFileName = getOutputFilePath(generateFileName("updated-products"));

// Set the updatedProductsFileName to the unique file name
// updatedProductsFileName = generateFileName("updated-products");

const maxSize = 5 * 1024 * 1024; // Max 5 MB per log file
const rotateLogFile = (currentFileName) => {
    const filePath = getOutputFilePath(currentFileName);
    if (fs.existsSync(filePath) && fs.statSync(filePath).size > maxSize) {
        const rotatedFileName = filePath.replace(".txt", `-end-${getFileSafePSTDate()}.txt`);
        fs.renameSync(filePath, rotatedFileName);
        updatedProductsFileName = getOutputFilePath(generateFileName(currentFileName.split('-')[0]));
    }
};

// Extend dayjs with UTC and timezone plugins
dayjs.extend(utc);
dayjs.extend(timezone);

// Define a custom timestamp function for Pacific Time
const customTimestamp = () => `,"time":"${dayjs().tz('America/Los_Angeles').format('YYYY-MM-DD HH:mm:ss')}"`;

// Logger setup with pino-pretty
const logger = pino(
    {
        base: null, // Removes default 'pid' and 'hostname' fields
        timestamp: customTimestamp, // Use the custom timestamp function
    },
    pinoPretty({
        levelFirst: true,
        colorize: true,
        translateTime: false, // Disable default time translation
    })
);

const logDetailedErrorToFile = (error, message = "") => {
    rotateLogFile("error-log");
    let formattedMessage = `[${getPSTDate()}] ${message}\n`;
    formattedMessage += `Error Name: ${error.name}\n`;
    formattedMessage += `Error Code: ${error.code || "N/A"}\n`;
    formattedMessage += `Error Message: ${error.message}\n`;
    formattedMessage += `Error Stack Trace: ${error.stack}\n`;
    logger.error(`Error occurred: ${formattedMessage}`);
    fs.appendFileSync(getOutputFilePath("error-log.txt"), formattedMessage);
};

  
// Function to log skipped items or errors directly to the file
const logErrorToFile = (message, error = null) => { // Add an optional error parameter
    rotateLogFile("error-log");
    let formattedMessage = `[${getPSTDate()}] ${message}\n`;

    if (error) formattedMessage += `- Error Stack Trace: ${error.stack}\n`;

    logger.error(`Error occurred: ${formattedMessage}`);
    fs.appendFileSync(getOutputFilePath("error-log.txt"), formattedMessage);
};

const logUpdatesToFile = (message) => {
    rotateLogFile(updatedProductsFileName);
    const formattedMessage = `[${getPSTDate()}] ${message}\n`;
    logger.warn(`${formattedMessage}`);
    fs.appendFileSync(updatedProductsFileName, formattedMessage);
};

const logInfoToFile = (message) => {
    rotateLogFile("log-info");
    const formattedMessage = `[${getPSTDate()}] ${message}\n`;
    logger.info(`${formattedMessage}`);
    fs.appendFileSync(getOutputFilePath("log-info.txt"), formattedMessage);
};

// Function to read existing progress from the file
const readProgressFile = (filePath) => {
    if (fs.existsSync(filePath)) {
        console.log(`Debug - Reading progress file at: ${filePath}`);
        const content = fs.readFileSync(filePath, "utf-8");
        console.log("Debug - File content:", content);

        return content.split("\n").reduce((acc, line) => {
            const match = line.match(/Progress for file "(.+?)": (\d+) updated, (\d+) skipped, (\d+) failed, out of (\d+) rows \((\d+)%\)\./);
            if (match) {
                acc[match[1]] = {
                    updatedCount: parseInt(match[2], 10),
                    skippedCount: parseInt(match[3], 10),
                    failedCount: parseInt(match[4], 10),
                    totalCount: parseInt(match[5], 10),
                    progress: parseInt(match[6], 10),
                };
            }
            return acc;
        }, {});
    } else {
        console.log(`Debug - Progress file not found at: ${filePath}`);
    }
    return {};
};

// Function to write updated progress to the file
const writeProgressFile = (newProgress) => {
    const progressFilePath = getOutputFilePath("update-progress.txt");
    
    // Read existing progress entries
    const existingProgress = readProgressFile(progressFilePath) || {}; // Load existing progress if any
    
    // Merge the new progress with existing progress
    const updatedProgress = { ...existingProgress, ...newProgress };

    // Convert the updated progress object to a string format for writing
    const content = Object.keys(updatedProgress).map((fileKey) => {
        const { updatedCount, skippedCount, failedCount, totalCount, progress: progressPercentage } = updatedProgress[fileKey];
        return `[${getPSTDate()}] Progress for file "${fileKey}": ${updatedCount} updated, ${skippedCount} skipped, ${failedCount} failed, out of ${totalCount} rows (${progressPercentage}%).`;
    }).join("\n");

    // Write back to the file, replacing old content with the latest state for each file
    fs.writeFileSync(progressFilePath, content + "\n", 'utf-8');
};

// Main function to log file progress
const logFileProgress = async (fileKey) => {
    try {
        const progressFilePath = getOutputFilePath("update-progress.txt");
        const existingProgress = readProgressFile(progressFilePath);

        // Debug log for initial existing progress
        logInfoToFile(`Debug - Initial existingProgress: ${JSON.stringify(existingProgress, null, 2)}`);

        // Fetch current counts from Redis
        const totalRows = await redisClient.get(`total-rows:${fileKey}`);
        const updatedProducts = await redisClient.get(`updated-products:${fileKey}`);
        const skippedProducts = await redisClient.get(`skipped-products:${fileKey}`);
        const failedProducts = await redisClient.get(`failed-products:${fileKey}`);

        const totalRowsCount = totalRows ? parseInt(totalRows, 10) : 0;
        const updatedProductsCount = updatedProducts ? parseInt(updatedProducts, 10) : 0;
        const skippedProductsCount = skippedProducts ? parseInt(skippedProducts, 10) : 0;
        const failedProductsCount = failedProducts ? parseInt(failedProducts, 10) : 0;

        // Calculate progress for this file
        const totalProcessedCount = Math.min(updatedProductsCount + skippedProductsCount + failedProductsCount, totalRowsCount);
        const progress = totalRowsCount > 0 ? Math.round((totalProcessedCount / totalRowsCount) * 100) : 0;

        // Update progress for the individual file
        existingProgress[fileKey] = {
            updatedCount: updatedProductsCount,
            skippedCount: skippedProductsCount,
            failedCount: failedProductsCount,
            totalCount: totalRowsCount,
            progress,
        };

        // Write the file-specific progress entry
        writeProgressFile({ [fileKey]: existingProgress[fileKey] });

        // Log human-readable progress for this file
        logUpdatesToFile(`[${getPSTDate()}] Progress for file "${fileKey}": ${updatedProductsCount} updated, ${skippedProductsCount} skipped, ${failedProductsCount} failed, out of ${totalRowsCount} rows (${progress}%).`);
    } catch (error) {
        logErrorToFile(`Error logging progress for file "${fileKey}": ${error.message}`);
    }
};

// Function to log overall progress
const logOverallProgress = async () => {
    try {
        const totalOverallCount = await redisClient.get('overall-total-rows') || 0;

        let totalUpdatedOverall = 0;
        let totalSkippedOverall = 0;
        let totalFailedOverall = 0;

        // Get all keys representing individual file counts in Redis
        const fileKeys = await redisClient.keys('total-rows:*');

        // Iterate through each file's keys to accumulate progress counts
        for (const fileKey of fileKeys) {
            // Extract the unique file identifier from the key (e.g., "total-rows:<fileKey>")
            const fileIdentifier = fileKey.split(':')[1];

            const updatedProducts = await redisClient.get(`updated-products:${fileIdentifier}`);
            const skippedProducts = await redisClient.get(`skipped-products:${fileIdentifier}`);
            const failedProducts = await redisClient.get(`failed-products:${fileIdentifier}`);

            // Parse values and accumulate counts for overall progress
            totalUpdatedOverall += updatedProducts ? parseInt(updatedProducts, 10) : 0;
            totalSkippedOverall += skippedProducts ? parseInt(skippedProducts, 10) : 0;
            totalFailedOverall += failedProducts ? parseInt(failedProducts, 10) : 0;
        }

        // Calculate the total processed count and the overall progress percentage
        const totalProcessedOverall = Math.min(
            totalUpdatedOverall + totalSkippedOverall + totalFailedOverall,
            parseInt(totalOverallCount, 10)
        );
        const overallProgress = totalOverallCount > 0 ? Math.round((totalProcessedOverall / totalOverallCount) * 100) : 0;

        // Update the progress data to be written to the file
        const overallProgressData = {
            Overall: {
                updatedCount: totalUpdatedOverall,
                skippedCount: totalSkippedOverall,
                failedCount: totalFailedOverall,
                totalCount: parseInt(totalOverallCount, 10),
                progress: overallProgress,
            }
        };

        // Write overall progress to update-progress.txt
        writeProgressFile(overallProgressData);

        // Log the overall progress
        logUpdatesToFile(`[${getPSTDate()}] Overall progress: ${totalUpdatedOverall} updated, ${totalSkippedOverall} skipped, ${totalFailedOverall} failed, out of ${totalOverallCount} rows (${overallProgress}%).`);
        
    } catch (error) {
        logErrorToFile(`Error logging overall progress: ${error.message}`);
    }
};

// Call this function periodically, e.g., every 2 minutes
setInterval(logOverallProgress, 2 * 60 * 1000); // 2 minutes in milliseconds

module.exports = {
    logger,
    logUpdatesToFile,
    logErrorToFile,
    logDetailedErrorToFile,
    logInfoToFile,
    logFileProgress
};