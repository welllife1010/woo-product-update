const fs = require("fs");
const path = require("path");
const { redisClient } = require('./queue');

// Define the path to the checkpoint file
const checkpointFilePath = path.join(__dirname, "process_checkpoint.json");

// Save progress to the checkpoint file
const saveCheckpoint = async (fileKey, lastProcessedRow, totalProductsInFile, batch) => {
    if (!batch || batch.length === 0) {
        logErrorToFile(`Invalid batch provided for checkpoint saving. File: ${fileKey}, Last Processed Row: ${lastProcessedRow}`);
        return;
    }

    const checkpoints = fs.existsSync(checkpointFilePath)
        ? JSON.parse(fs.readFileSync(checkpointFilePath, "utf-8"))
        : {};

    // Calculate the updated last processed row but limit it to totalProductsInFile
    const updatedLastProcessedRow = Math.min(lastProcessedRow + batch.length, totalProductsInFile);

    // Save checkpoint only if within totalProductsInFile bounds
    if (updatedLastProcessedRow <= totalProductsInFile) {
        await redisClient.set(`lastProcessedRow:${fileKey}`, updatedLastProcessedRow);
        checkpoints[fileKey] = {
            lastProcessedRow: updatedLastProcessedRow,
            timestamp: new Date().toISOString(),
        };
        fs.writeFileSync(checkpointFilePath, JSON.stringify(checkpoints, null, 2));
    } else {
        logErrorToFile(`Attempted to save a checkpoint with lastProcessedRow (${updatedLastProcessedRow}) exceeding totalProductsInFile (${totalProductsInFile}) for file: ${fileKey}`);
    }
};

// Get the last checkpoint for a given file
const getCheckpoint = (fileKey) => {
    if (!fs.existsSync(checkpointFilePath)) return 0;
    const checkpoints = JSON.parse(fs.readFileSync(checkpointFilePath, "utf-8"));
    return checkpoints[fileKey]?.lastProcessedRow || 0;
};

module.exports = {
    saveCheckpoint,
    getCheckpoint,
  };