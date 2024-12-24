const fs = require("fs");
const path = require("path");
const pino = require("pino");
const pinoPretty = require("pino-pretty");
const dayjs = require('dayjs');
const utc = require('dayjs/plugin/utc');
const timezone = require('dayjs/plugin/timezone');
const { redisClient } = require('./queue');

// Extend dayjs with UTC and timezone plugins
dayjs.extend(utc);
dayjs.extend(timezone);

// File paths
const progressFilePath = path.join(__dirname, "output-files", "update-progress.txt");
const errorFilePath = path.join(__dirname, "output-files", "error-log.txt");
const infoFilePath = path.join(__dirname, "output-files", "info-log.txt");
const updatesFilePath = path.join(__dirname, "output-files", "updates-log.txt");

// Log file size limit (5 MB)
const maxSize = 5 * 1024 * 1024; // 5 MB

// Utility function for consistent timestamps
const getPSTTimestamp = () => dayjs().tz("America/Los_Angeles").format("YYYY-MM-DD HH:mm:ss");

// Utility function for safe filenames
const getSafePSTTimestamp = () => dayjs().tz("America/Los_Angeles").format("YYYY-MM-DD_HH-mm-ss");

// Rotate log files if size exceeds the limit
const rotateLogFile = (filePath) => {
    if (fs.existsSync(filePath) && fs.statSync(filePath).size > maxSize) {
      const rotatedFileName = filePath.replace(".txt", `-archived-${getSafePSTTimestamp()}.txt`);
      fs.renameSync(filePath, rotatedFileName);
      console.log(`[${getPSTTimestamp()}] Rotated log file: ${rotatedFileName}`);
    }
  };

// Function to write content to log files with rotation
const writeToFile = (filePath, content) => {
    rotateLogFile(filePath);
    fs.appendFileSync(filePath, content, "utf-8");
  };
  
  // Write the progress log file
  const writeProgressToFile = (content) => {
    writeToFile(progressFilePath, content);
  };
  
  // Fetch progress from Redis and log to file
  const logProgressToFile = async () => {
    try {
      const fileKeys = await redisClient.keys("total-rows:*");
  
      if (fileKeys.length === 0) {
        console.log(`[${getPSTTimestamp()}] No progress to log.`);
        return;
      }
  
      let progressLogs = "";
  
      for (const key of fileKeys) {
        const fileKey = key.split(":")[1];
        const totalRows = parseInt(await redisClient.get(`total-rows:${fileKey}`) || 0, 10);
        const updated = parseInt(await redisClient.get(`updated-products:${fileKey}`) || 0, 10);
        const skipped = parseInt(await redisClient.get(`skipped-products:${fileKey}`) || 0, 10);
        const failed = parseInt(await redisClient.get(`failed-products:${fileKey}`) || 0, 10);
  
        const completed = updated + skipped + failed;
        const progress = totalRows > 0 ? Math.round((completed / totalRows) * 100) : 0;
  
        progressLogs += `[${getPSTTimestamp()}] File: ${fileKey}\n`;
        progressLogs += `Updated: ${updated}, Skipped: ${skipped}, Failed: ${failed}, Total: ${totalRows} (${progress}% completed)\n\n`;
      }
  
      writeProgressToFile(progressLogs);
    } catch (error) {
      logErrorToFile(`Error logging progress: ${error.message}`);
    }
  };

  // Error logging function
const logErrorToFile = (message, error = null) => {
    const timestamp = getPSTTimestamp();
    let errorContent = `[${timestamp}] ${message}\n`;
    if (error) errorContent += `Error: ${error.stack || error.message}\n`;
    writeToFile(errorFilePath, errorContent);
};
  
// Info logging function
const logInfoToFile = (message) => {
    const content = `[${getPSTTimestamp()}] ${message}\n`;
    writeToFile(infoFilePath, content);
};

// Updates logging function
const logUpdatesToFile = (message) => {
    const content = `[${getPSTTimestamp()}] ${message}\n`;
    writeToFile(updatesFilePath, content);
};

// Pino logger setup
const pinoLogger = pino(
    {
        base: null, // Removes default 'pid' and 'hostname' fields
        timestamp: () => `,"time":"${getPSTTimestamp()}"`, // Use the custom timestamp function
    },
    pinoPretty({
        levelFirst: true,
        colorize: true,
        translateTime: false, // Disable default time translation
    })
);

module.exports = {
    logger: pinoLogger, // Pino logger for external use
    logUpdatesToFile,
    logErrorToFile,
    logInfoToFile,
    logProgressToFile
};