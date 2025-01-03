const { batchQueue } = require('./queue'); // Bull Queue instance
const { limiter } = require('./woo-helpers'); // Bottleneck instance
const { logErrorToFile, logInfoToFile } = require('./logger');

// Add a batch job to Bull Queue
const addBatchJob = async (jobData, jobId) => {
    try {
        const job = await batchQueue.add(jobData, {
            jobId,
            attempts: 5,
            backoff: { type: 'exponential', delay: 5000 },
            timeout: 300000 // 5 minutes
        });
        logInfoToFile(`Successfully added batch job with ID: ${job.id}`);
        return job;
    } catch (error) {
        logErrorToFile(`Failed to add batch job with ID: ${jobId}`, error);
        throw error;
    }
};

// Schedule an API request using Bottleneck
const scheduleApiRequest = async (task, options) => {
    try {
        const response = await limiter.schedule(options, task);
        logInfoToFile(`Successfully scheduled API request: ${options.id}`);
        return response;
    } catch (error) {
        logErrorToFile(`Failed to schedule API request: ${options.id}`, error);
        throw error;
    }
};

module.exports = {
    addBatchJob,
    scheduleApiRequest
};