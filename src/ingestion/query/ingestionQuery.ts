export const IngestionDatasetQuery = {
    async getDataset(datasetName) {
        const queryStr = `SELECT dataset_data FROM spec.dataset WHERE dataset_name = $1`;
        return {query: queryStr, values: [datasetName]};
    },
    async getDimension(dimensionName) {
        const queryStr = `SELECT dimension_data FROM spec.dimension WHERE dimension_name = $1`;
        return {query: queryStr, values: [dimensionName]};
    },
    async getEvents(eventName) {
        const queryStr = `SELECT event_data FROM spec.event WHERE event_name = $1`;
        return {query: queryStr, values: [eventName]};
    },
    async getPipelineSpec(pipelineName) {
        const queryStr = `SELECT transformer_file, event_name, dataset_name
        FROM spec.pipeline
        LEFT JOIN spec.event ON event.pid = pipeline.event_pid
        LEFT JOIN spec.dataset ON dataset.pid  = pipeline.dataset_pid
        LEFT JOIN spec.transformer ON transformer.pid = pipeline.transformer_pid
        WHERE pipeline_name = $1`;
        return {query: queryStr, values: [pipelineName]};
    },
    async createFileTracker(uploadedFileName, ingestionType, ingestionName, fileSize) {
        const queryStr = `INSERT INTO ingestion.file_tracker(uploaded_file_name, ingestion_type, ingestion_name, file_status, filesize)
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
        const queryStr = `UPDATE ingestion.file_tracker
            SET file_status = $2,
            updated_at = CURRENT_TIMESTAMP
            ${whereStr}
            WHERE pid = $1`;
        return {query: queryStr, values: [pid, fileStatus]};
    },
    async getFileStatus(fileName, ingestionType, ingestionName) {
        const queryStr = `SELECT pid,file_status,created_at FROM ingestion.file_tracker WHERE uploaded_file_name = $1 AND ingestion_type=$2 AND ingestion_name = $3 `;
        return {query: queryStr, values: [fileName, ingestionType, ingestionName]};
    },
    async getFile(fileName, ingestionType, ingestionName) {
        const queryStr = `SELECT pid, uploaded_file_name, system_file_name, file_status 
        FROM ingestion.file_tracker
        WHERE system_file_name = $1
        AND ingestion_type = $2
        AND ingestion_name = $3
        AND is_deleted = false;`;
        return {query: queryStr, values: [fileName, ingestionType, ingestionName]};
    },
    async updateFileStatus(pid, fileStatus) {
        const queryStr = `UPDATE ingestion.file_tracker
            SET file_status = $2,
            updated_at = CURRENT_TIMESTAMP
            WHERE pid = $1;`;
        return {query: queryStr, values: [pid, fileStatus]};
    },
    async updateFileProcessedCount(pid) {
        const queryStr = `UPDATE ingestion.file_tracker AS ft
            SET processed_count = ft.processed_count::integer + 1::integer,
            updated_at = CURRENT_TIMESTAMP
            WHERE pid = $1 
            RETURNING *;`;
        return {query: queryStr, values: [pid]};
    },
    async getDatasetCount(ingestionName) {
        const queryStr = `SELECT COUNT(pid) AS dataset_count
        FROM spec.pipeline
        WHERE event_pid = (SELECT pid FROM spec.event WHERE event_name = $1)
        AND dataset_pid IS NOT NULL
        AND is_deleted = false;`;
        return {query: queryStr, values: [ingestionName]};
    }
};