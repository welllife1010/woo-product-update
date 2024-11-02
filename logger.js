// TODO: 
// 1. update the Date from (UTC) to human readable (PST) in the logger
 
const fs = require("fs");
const path = require("path");
const pino = require("pino");
const pinoPretty = require("pino-pretty");

// Generate the unique filename with date and increment version dynamically if file exists
const date = new Date().toISOString().split("T")[0]; // YYYY-MM-DD format
const pstDate = new Date().toLocaleString("en-US", {
    timeZone: "America/Los_Angeles",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: true,  // Optional: change to false for 24-hour format
});

let version = 1; 
let updatedProductsFile;


// Function to generate a unique file name by checking for existing files
const generateFileName = () => {
    let fileName;
    do {
        fileName = `updated-products-${date}-v${version}.txt`;
        version += 1;
    } while (fs.existsSync(fileName));
    return fileName;
};

const logFile = generateFileName();
const maxSize = 5 * 1024 * 1024; // Max 5 MB per log file

// Set the updatedProductsFile to the unique file name
updatedProductsFile = generateFileName();

const rotateLogFile = () => {
    if (fs.existsSync(logFile) && fs.statSync(logFile).size > maxSize) {
        fs.renameSync(logFile, logFile.replace(".txt", `-${Date.now()}.txt`));
    }
};

// Logger setup with pino-pretty
const logger = pino(
    pinoPretty({
        levelFirst: true,
        colorize: true,
        translateTime: "SYS:standard",
    })
);
  
// Function to log skipped items or errors directly to the file
const logErrorToFile = (message) => {
    rotateLogFile();
    const formattedMessage = `[${pstDate}] ${message}\n`;
    fs.appendFileSync("error-log.txt", formattedMessage);
};

const logUpdatesToFile = (message) => {
    rotateLogFile();
    const formattedMessage = `[${pstDate}] ${message}\n`;
    fs.appendFileSync(updatedProductsFile, formattedMessage);
};

module.exports = {
    logger,
    logUpdatesToFile,
    logErrorToFile,
};