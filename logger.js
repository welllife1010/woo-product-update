const fs = require("fs");
const path = require("path");
const pino = require("pino");
const pinoPretty = require("pino-pretty");

// Generate the unique filename with date and version
const date = new Date().toISOString().split("T")[0]; // YYYY-MM-DD format
const version = "v3"; // Change this if need to track different versions
const updatedProductsFile = path.join(__dirname, `updated-products-${date}-${version}.txt`);

// Logger setup with pino-pretty
const logger = pino(
    pinoPretty({
        levelFirst: true,
        colorize: true,
        translateTime: "SYS:standard",
    })
);

const logUpdatesToFile = (message) => {
    const formattedMessage = `[${new Date().toISOString()}] ${message}\n`;
    fs.appendFileSync(updatedProductsFile, formattedMessage);
};
  
// Function to log skipped items or errors directly to the file
const logErrorToFile = (message) => {
    const formattedMessage = `[${new Date().toISOString()}] ERROR: ${message}\n`;
    fs.appendFileSync("error-log.txt", formattedMessage);
};

module.exports = {
    logger,
    logUpdatesToFile,
    logErrorToFile,
};