const fs = require("fs");
const path = require("path");
const pino = require("pino");
const pinoPretty = require("pino-pretty");

// Generate the unique filename with date and increment version dynamically if file exists
const date = new Date().toISOString().split("T")[0]; // YYYY-MM-DD format
let version = "1"; 
let updatedProductsFile;


// Function to generate a unique file name by checking for existing files
const generateFileName = () => {
    let fileName;
    do {
        fileName = path.join(__dirname, `updated-products-${date}-v${version}.txt`);
        version += 1;
    } while (fs.existsSync(fileName));
    return fileName;
};

// Set the updatedProductsFile to the unique file name
updatedProductsFile = generateFileName();

const logUpdatesToFile = (message) => {
    const formattedMessage = `[${new Date().toISOString()}] ${message}\n`;
    fs.appendFileSync(updatedProductsFile, formattedMessage);
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
    const formattedMessage = `[${new Date().toISOString()}] ERROR: ${message}\n`;
    fs.appendFileSync("error-log.txt", formattedMessage);
};

module.exports = {
    logger,
    logUpdatesToFile,
    logErrorToFile,
};