const fs = require("fs");
const path = require("path");
const pino = require("pino");
const pinoPretty = require("pino-pretty");
const dayjs = require('dayjs');
const utc = require('dayjs/plugin/utc');
const timezone = require('dayjs/plugin/timezone');
const { redisClient } = require('./queue');

// Generate the unique filename with date and increment version dynamically if file exists
const date = new Date().toISOString().split("T")[0]; // YYYY-MM-DD format

// Helper function to generate the current date and time in PST
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

let updatedProductsFileName;
const maxSize = 5 * 1024 * 1024; // Max 5 MB per log file


// Function to generate a unique file name by checking for existing files
const generateFileName = (fileName = "log-info") => {
    const creationTimestamp = getFileSafePSTDate(); // Capture creation timestamp
    let version = 1;
    let generatedFileName;

    do {
        generatedFileName = `${fileName}-${creationTimestamp}-v${version}.txt`;
        version += 1;
    } while (fs.existsSync(generatedFileName));
    
    return generatedFileName;
};

// Set the updatedProductsFileName to the unique file name
updatedProductsFileName = generateFileName("updated-products");

const rotateLogFile = (currentFileName) => {
    if (fs.existsSync(currentFileName) && fs.statSync(currentFileName).size > maxSize) {
        const endingTimestamp = getFileSafePSTDate(); // Capture ending timestamp
        const rotatedFileName = currentFileName.replace(".txt", `-end-${endingTimestamp}.txt`);
        
        // Rename the existing file with the ending timestamp
        fs.renameSync(currentFileName, rotatedFileName);

        // Generate a new file name for future logs
        updatedProductsFileName = generateFileName(currentFileName.split('-')[0]); // Pass base name without timestamps
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
    fs.appendFileSync("error-log.txt", formattedMessage);
};

  
// Function to log skipped items or errors directly to the file
const logErrorToFile = (message, error = null) => { // Add an optional error parameter
    rotateLogFile("error-log");
    let formattedMessage = `[${getPSTDate()}] ${message}\n`;

    if (error) { // Add the error details if provided
        formattedMessage += `- Error Stack Trace: ${error.stack}\n`; 
    }

    logger.error(`Error occurred: ${formattedMessage}`);
    fs.appendFileSync("error-log.txt", formattedMessage);
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
    fs.appendFileSync("log-info.txt", formattedMessage);
};

// Function to read existing progress from the file
const readProgressFile = (filePath) => {
    if (fs.existsSync(filePath)) {
        const content = fs.readFileSync(filePath, "utf-8");
        return content.split("\n").reduce((acc, line) => {
            const match = line.match(/Progress for file "(.+?)": (\d+) out of (\d+) rows updated \((\d+)%\)\./);
            if (match) {
                acc[match[1]] = {
                    updatedCount: parseInt(match[2], 10),
                    totalCount: parseInt(match[3], 10),
                    progress: parseInt(match[4], 10),
                };
            }
            return acc;
        }, {});
    }
    return {};
};

// Function to write updated progress to the file
const writeProgressFile = (filePath, progressData) => {
    const content = Object.keys(progressData).map((fileKey) => {
        const { updatedCount, totalCount, progress } = progressData[fileKey];
        return `[${getPSTDate()}] Progress for file "${fileKey}": ${updatedCount} out of ${totalCount} rows updated (${progress}%).`;
    }).join("\n");
    
    fs.writeFileSync(filePath, content);
};

// Main function to log file progress
const logFileProgress = async (fileKey) => {
    try {
        logUpdatesToFile(`[DEBUG] logFileProgress called for file: ${fileKey}`);
        const progressFilePath = path.join(__dirname, "update-progress.txt");
        logUpdatesToFile(`[DEBUG] Progress file path: ${progressFilePath}`);
        const existingProgress = readProgressFile(progressFilePath);

        // Get total rows in the CSV file from Redis
        const totalRows = await redisClient.get(`total-rows:${fileKey}`);
        const updatedProducts = await redisClient.get(`updated-products:${fileKey}`);
        
        const totalRowsCount = totalRows ? parseInt(totalRows, 10) : 0;
        const updatedProductsCount = updatedProducts ? parseInt(updatedProducts, 10) : 0;

        // Calculate progress
        const progress = totalRowsCount > 0 ? Math.round((updatedProductsCount / totalRowsCount) * 100) : 0;

        // Update the progress data
        existingProgress[fileKey] = {
            updatedCount: updatedProductsCount,
            totalCount: totalRowsCount,
            progress,
        };

        // Calculate overall progress
        const totalOverallCount = Object.values(existingProgress).reduce((sum, file) => sum + file.totalCount, 0);
        const totalUpdatedOverall = Object.values(existingProgress).reduce((sum, file) => sum + file.updatedCount, 0);
        const overallProgress = totalOverallCount > 0 ? Math.round((totalUpdatedOverall / totalOverallCount) * 100) : 0;

        // Add overall progress to the data
        existingProgress["Overall"] = {
            updatedCount: totalUpdatedOverall,
            totalCount: totalOverallCount,
            progress: overallProgress,
        };

        // Write updated progress to the file
        writeProgressFile(progressFilePath, existingProgress);

        // Log the progress to console
        logUpdatesToFile(`[${getPSTDate()}] Progress for file "${fileKey}": ${updatedProductsCount} out of ${totalRowsCount} rows updated (${progress}%).`);
        logUpdatesToFile(`[${getPSTDate()}] Overall progress: ${totalUpdatedOverall} out of ${totalOverallCount} rows updated (${overallProgress}%).`);
    } catch (error) {
        logErrorToFile(`Error logging progress for file "${fileKey}": ${error.message}`);
    }
};


module.exports = {
    logger,
    logUpdatesToFile,
    logErrorToFile,
    logDetailedErrorToFile,
    logInfoToFile,
    logFileProgress
};