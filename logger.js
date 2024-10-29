// logger.js
const fs = require("fs");
const pino = require("pino");
const pinoPretty = require("pino-pretty");

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
    logErrorToFile,
};
