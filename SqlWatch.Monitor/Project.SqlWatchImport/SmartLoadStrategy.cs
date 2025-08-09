using System;
using System.Data;
using System.Data.SqlClient;
using System.Threading.Tasks;

namespace SqlWatchImport
{
    /// <summary>
    /// Smart load strategy manager - inspired by SQLWATCH's load type logic
    /// </summary>
    public class SmartLoadStrategy
    {
        private readonly string _connectionString;

        public SmartLoadStrategy(string connectionString)
        {
            _connectionString = connectionString;
        }

        /// <summary>
        /// Determine the best load strategy for a table - inspired by SQLWATCH's logic
        /// </summary>
        public async Task<LoadType> DetermineLoadStrategy(string tableName, string sqlInstance, LoadType requestedType)
        {
            try
            {
                // If smart strategy is disabled, use requested type
                if (!Config.UseSmartLoadStrategy)
                    return requestedType;

                // Meta tables always full load (like SQLWATCH)
                if (tableName.Contains("sqlwatch_meta") && Config.MetaTablesFullLoadOnly)
                {
                    Logger.LogVerbose($"Meta table {tableName} forced to FULL load strategy");
                    return LoadType.Full;
                }

                // For logger tables, check if delta is beneficial
                if (tableName.Contains("sqlwatch_logger") && Config.LoggerTablesDeltaLoad)
                {
                    var deltaViability = await AssessDeltaViability(tableName, sqlInstance);
                    
                    if (deltaViability.IsViable && requestedType == LoadType.Delta)
                    {
                        Logger.LogVerbose($"Logger table {tableName} using DELTA load strategy (estimated {deltaViability.EstimatedRows} new rows)");
                        return LoadType.Delta;
                    }
                    else if (!deltaViability.IsViable)
                    {
                        Logger.LogVerbose($"Logger table {tableName} switching to FULL load strategy - {deltaViability.Reason}");
                        return LoadType.Full;
                    }
                }

                return requestedType;
            }
            catch (Exception ex)
            {
                Logger.LogError($"Error determining load strategy for {tableName}", ex.Message);
                return LoadType.Full; // Safe fallback
            }
        }

        /// <summary>
        /// Assess if delta load is viable - inspired by SQLWATCH's delta logic
        /// </summary>
        private async Task<DeltaViabilityResult> AssessDeltaViability(string tableName, string sqlInstance)
        {
            try
            {
                using (var connection = new SqlConnection(_connectionString))
                {
                    await connection.OpenAsync();

                    // Check if table has snapshot_time column (required for delta)
                    var hasSnapshotTime = await CheckSnapshotTimeColumn(connection, tableName);
                    if (!hasSnapshotTime)
                    {
                        return new DeltaViabilityResult 
                        { 
                            IsViable = false, 
                            Reason = "Table lacks snapshot_time column for delta load" 
                        };
                    }

                    // Get current max snapshot_time for this instance (like SQLWATCH's logic)
                    var lastSnapshot = await GetLastSnapshotTime(connection, tableName, sqlInstance);
                    
                    // Estimate rows that would be imported in delta
                    var estimatedRows = await EstimateDeltaRows(connection, tableName, sqlInstance, lastSnapshot);

                    // If delta would import more than 50% of total table, full load might be better
                    var totalRows = await GetTotalRowCount(connection, tableName, sqlInstance);
                    var deltaPercentage = totalRows > 0 ? (estimatedRows * 100.0 / totalRows) : 0;

                    if (deltaPercentage > 50)
                    {
                        return new DeltaViabilityResult 
                        { 
                            IsViable = false, 
                            Reason = $"Delta would import {deltaPercentage:F1}% of table data, full load more efficient",
                            EstimatedRows = estimatedRows
                        };
                    }

                    return new DeltaViabilityResult 
                    { 
                        IsViable = true, 
                        EstimatedRows = estimatedRows 
                    };
                }
            }
            catch (Exception ex)
            {
                Logger.LogError($"Error assessing delta viability for {tableName}", ex.Message);
                return new DeltaViabilityResult 
                { 
                    IsViable = false, 
                    Reason = $"Error during assessment: {ex.Message}" 
                };
            }
        }

        /// <summary>
        /// Check if table has snapshot_time column
        /// </summary>
        private async Task<bool> CheckSnapshotTimeColumn(SqlConnection connection, string tableName)
        {
            var sql = @"
                SELECT COUNT(*)
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA + '.' + TABLE_NAME = @tableName
                AND COLUMN_NAME = 'snapshot_time'";

            using (var cmd = new SqlCommand(sql, connection))
            {
                cmd.Parameters.AddWithValue("@tableName", tableName);
                var result = await cmd.ExecuteScalarAsync();
                return Convert.ToInt32(result) > 0;
            }
        }

        /// <summary>
        /// Get last snapshot time for delta calculation - like SQLWATCH's logic
        /// </summary>
        private async Task<DateTime?> GetLastSnapshotTime(SqlConnection connection, string tableName, string sqlInstance)
        {
            var sql = $@"
                SELECT MAX(snapshot_time) 
                FROM {tableName} 
                WHERE sql_instance = @sqlInstance";

            using (var cmd = new SqlCommand(sql, connection))
            {
                cmd.Parameters.AddWithValue("@sqlInstance", sqlInstance);
                var result = await cmd.ExecuteScalarAsync();
                return result == DBNull.Value ? null : (DateTime?)result;
            }
        }

        /// <summary>
        /// Estimate rows that would be imported in delta load
        /// </summary>
        private async Task<long> EstimateDeltaRows(SqlConnection connection, string tableName, string sqlInstance, DateTime? lastSnapshot)
        {
            if (!lastSnapshot.HasValue)
                return await GetTotalRowCount(connection, tableName, sqlInstance);

            // This would need to query the remote instance to get accurate estimate
            // For now, return a conservative estimate
            return 1000; // Placeholder - in real implementation, query remote instance
        }

        /// <summary>
        /// Get total row count for comparison
        /// </summary>
        private async Task<long> GetTotalRowCount(SqlConnection connection, string tableName, string sqlInstance)
        {
            var sql = $@"
                SELECT COUNT(*) 
                FROM {tableName} 
                WHERE sql_instance = @sqlInstance";

            using (var cmd = new SqlCommand(sql, connection))
            {
                cmd.Parameters.AddWithValue("@sqlInstance", sqlInstance);
                var result = await cmd.ExecuteScalarAsync();
                return Convert.ToInt64(result);
            }
        }

        /// <summary>
        /// Create delta load query - inspired by SQLWATCH's delta logic
        /// </summary>
        public string CreateDeltaQuery(string tableName, string sqlInstance, DateTime? lastSnapshot)
        {
            var allColumns = "*"; // In real implementation, get from table metadata
            
            var sql = $@"
                SELECT {allColumns}
                FROM {tableName}
                WHERE sql_instance = '{sqlInstance}'";

            if (lastSnapshot.HasValue)
            {
                sql += $" AND snapshot_time > '{lastSnapshot.Value:yyyy-MM-dd HH:mm:ss}'";
            }

            // For logger tables other than snapshot_header, limit to available header data
            if (tableName.Contains("sqlwatch_logger") && !tableName.Contains("snapshot_header"))
            {
                sql += $@" 
                    AND snapshot_time <= (
                        SELECT MAX(snapshot_time) 
                        FROM dbo.sqlwatch_logger_snapshot_header 
                        WHERE sql_instance = '{sqlInstance}'
                    )";
            }

            return sql;
        }
    }

    /// <summary>
    /// Result of delta viability assessment
    /// </summary>
    public class DeltaViabilityResult
    {
        public bool IsViable { get; set; }
        public string Reason { get; set; }
        public long EstimatedRows { get; set; }
    }
}
