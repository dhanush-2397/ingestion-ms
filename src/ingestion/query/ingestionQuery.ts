export const IngestionDatasetQuery = {
    async getDataset(datasetName) {
        const queryStr = `SELECT schema FROM spec."DatasetGrammar" WHERE cname = $1`;
        return {query: queryStr, values: [datasetName]};
    },
    async getDimension(dimensionName) {
        const queryStr = `SELECT schema FROM spec."DimensionGrammar" WHERE cname = $1`;
        return {query: queryStr, values: [dimensionName]};
    },
    async getEvents(eventName) {
        const queryStr = `SELECT schema FROM spec."EventGrammar" WHERE cname = $1`;
        return {query: queryStr, values: [eventName]};
    },
    async createFileTracker(uploadedFileName, ingestionType, ingestionName, fileSize) {
        const queryStr = `INSERT INTO ingestion."FileTracker"(uploaded_file_name, ingestion_type, ingestion_name, file_status, filesize)
	                       VALUES ($1, $2, $3, $4, $5) RETURNING pid`;
        return {
            query: queryStr,
            values: [uploadedFileName, ingestionType, ingestionName, 'Upload_in_progress', fileSize]
        };
    },
    async updateFileTracker(pid, fileStatus, ingestionName?: string) {
        let whereStr = "";
        if (ingestionName) {
            whereStr = `, system_file_name = '${ingestionName}.csv'`
        }
        const queryStr = `UPDATE ingestion."FileTracker"
            SET file_status = $2,
            updated_at = CURRENT_TIMESTAMP
            ${whereStr}
            WHERE pid = $1`;
        return {query: queryStr, values: [pid, fileStatus]};
    },
    async getFileStatus(fileName, ingestionType, ingestionName) {
        const queryStr = `SELECT pid,file_status,created_at,processed_data_count,error_data_count
        FROM ingestion."FileTracker" 
        WHERE uploaded_file_name = $1 
        AND ingestion_type=$2 
        AND ingestion_name = $3 
        ORDER BY pid DESC`;
        return {query: queryStr, values: [fileName, ingestionType, ingestionName]};
    },
    async getFile(fileName, ingestionType, ingestionName) {
        const queryStr = `SELECT pid, uploaded_file_name, system_file_name, file_status 
        FROM ingestion."FileTracker"
        WHERE system_file_name = $1
        AND ingestion_type = $2
        AND ingestion_name = $3
        AND is_deleted = false;`;
        return {query: queryStr, values: [fileName, ingestionType, ingestionName]};
    },
    async updateFileStatus(pid, fileStatus) {
        const queryStr = `UPDATE ingestion."FileTracker"
            SET file_status = $2,
            updated_at = CURRENT_TIMESTAMP
            WHERE pid = $1 RETURNING pid;`;
        return {query: queryStr, values: [pid, fileStatus]};
    },
    async updateCounter(fileTrackerPid, validCounter, errorCounter) {
        let query = "";
        if (validCounter) {
            query = `SET processed_data_count = ${+validCounter}`
        } else if (errorCounter) {
            query = `SET error_data_count = ${+errorCounter}`
        }
        const queryStr = `UPDATE ingestion."FileTracker"
        ${query}
        WHERE pid = $1;`;
        console.log('ingestionQuery.updateCounter: ', queryStr);
        return {query: queryStr, values: [fileTrackerPid]};
    }
};