const createUniqueJobId = (fileKey, action, rowIndex, retryCount = 0) => {
    return `${fileKey}-${action}-row-${rowIndex}-retry-${retryCount}`;
};

module.exports = { createUniqueJobId };