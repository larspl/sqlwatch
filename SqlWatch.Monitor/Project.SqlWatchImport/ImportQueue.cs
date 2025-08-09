using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Threading.Tasks;

namespace SqlWatchImport
{
    /// <summary>
    /// Import queue item - inspired by SQLWATCH's sqlwatch_meta_repository_import_queue
    /// </summary>
    public class ImportQueueItem
    {
        public string SqlInstance { get; set; }
        public string ObjectName { get; set; }
        public string ParentObjectName { get; set; } // For dependency management
        public int Priority { get; set; } // 1=Meta tables, 2=Logger tables
        public LoadType LoadType { get; set; }
        public DateTime TimeQueued { get; set; }
        public Guid BatchId { get; set; }
        public ImportStatus Status { get; set; }
        public DateTime? StartTime { get; set; }
        public DateTime? EndTime { get; set; }
        public string ThreadName { get; set; }
        public int RetryCount { get; set; }
        public string LastError { get; set; }
    }

    /// <summary>
    /// Load type enum - inspired by SQLWATCH's load_type
    /// </summary>
    public enum LoadType
    {
        Full,    // Complete table refresh
        Delta    // Incremental load
    }

    /// <summary>
    /// Import status enum
    /// </summary>
    public enum ImportStatus
    {
        Queued,
        Running,
        Success,
        Error,
        Retry
    }

    /// <summary>
    /// Queue-based import manager - inspired by SQLWATCH's T-SQL repository architecture
    /// </summary>
    public class ImportQueueManager
    {
        private readonly string _connectionString;
        private readonly List<ImportQueueItem> _queue;
        private readonly object _queueLock = new object();

        public ImportQueueManager(string connectionString)
        {
            _connectionString = connectionString;
            _queue = new List<ImportQueueItem>();
        }

        /// <summary>
        /// Populate queue with tables to import - inspired by usp_sqlwatch_repository_remote_table_enqueue
        /// </summary>
        public Task<bool> PopulateQueue(List<SqlWatchInstance> instances, DataTable tables)
        {
            try
            {
                Logger.LogVerbose("Starting queue population for import processing");
                
                var batchId = Guid.NewGuid();
                var queueItems = new List<ImportQueueItem>();

                foreach (var instance in instances)
                {
                    foreach (DataRow table in tables.Rows)
                    {
                        var tableName = table["table_name"].ToString();
                        var dependencyLevel = Convert.ToInt32(table["dependency_level"]);

                        // Determine parent object based on SQLWATCH's dependency logic
                        var parentObject = DetermineParentObject(tableName, instance.SqlInstance);
                        
                        // Determine load type based on SQLWATCH's smart strategy
                        var loadType = DetermineLoadType(tableName);
                        
                        // Set priority based on table type (like SQLWATCH's priority system)
                        var priority = tableName.Contains("sqlwatch_meta") ? Config.MetaTablePriority : Config.LoggerTablePriority;

                        var queueItem = new ImportQueueItem
                        {
                            SqlInstance = instance.SqlInstance,
                            ObjectName = tableName,
                            ParentObjectName = parentObject,
                            Priority = priority,
                            LoadType = loadType,
                            TimeQueued = DateTime.UtcNow,
                            BatchId = batchId,
                            Status = ImportStatus.Queued,
                            RetryCount = 0
                        };

                        queueItems.Add(queueItem);
                    }
                }

                // Sort by priority and dependency (like SQLWATCH's ordering)
                var orderedItems = queueItems
                    .OrderBy(x => x.Priority)
                    .ThenBy(x => HasDependency(x.ParentObjectName))
                    .ThenBy(x => x.ObjectName);

                lock (_queueLock)
                {
                    _queue.Clear();
                    _queue.AddRange(orderedItems);
                }

                Logger.LogVerbose($"Queue populated with {queueItems.Count} items across {instances.Count} instances");
                return Task.FromResult(true);
            }
            catch (Exception ex)
            {
                Logger.LogError("Failed to populate import queue", ex.Message);
                return Task.FromResult(false);
            }
        }

        /// <summary>
        /// Dequeue next available item - inspired by usp_sqlwatch_repository_remote_table_dequeue
        /// </summary>
        public ImportQueueItem DequeueNext(string threadName)
        {
            lock (_queueLock)
            {
                // Find next available item (similar to SQLWATCH's readpast logic)
                var nextItem = _queue
                    .Where(x => x.Status == ImportStatus.Queued || x.Status == ImportStatus.Retry)
                    .Where(x => string.IsNullOrEmpty(x.ParentObjectName) || 
                               _queue.Any(p => p.ObjectName == x.ParentObjectName && p.Status == ImportStatus.Success))
                    .OrderBy(x => x.Priority)
                    .ThenBy(x => x.TimeQueued)
                    .FirstOrDefault();

                if (nextItem != null)
                {
                    nextItem.Status = ImportStatus.Running;
                    nextItem.StartTime = DateTime.UtcNow;
                    nextItem.ThreadName = threadName;
                    
                    Logger.LogVerbose($"Dequeued: {nextItem.ObjectName} for {nextItem.SqlInstance} (Thread: {threadName})");
                }

                return nextItem;
            }
        }

        /// <summary>
        /// Mark item as completed
        /// </summary>
        public void MarkCompleted(ImportQueueItem item, bool success, string errorMessage = null)
        {
            lock (_queueLock)
            {
                item.Status = success ? ImportStatus.Success : ImportStatus.Error;
                item.EndTime = DateTime.UtcNow;
                item.LastError = errorMessage;

                if (!success && item.RetryCount < Config.QueueRetryAttempts)
                {
                    // Schedule for retry (like SQLWATCH's retry logic)
                    item.RetryCount++;
                    item.Status = ImportStatus.Retry;
                    item.TimeQueued = DateTime.UtcNow.AddMilliseconds(Config.QueueRetryDelayMs);
                    
                    Logger.LogVerbose($"Scheduling retry {item.RetryCount}/{Config.QueueRetryAttempts} for {item.ObjectName}");
                }
                else if (!success)
                {
                    // Remove dependent items if parent failed (like SQLWATCH's cleanup)
                    var dependentItems = _queue.Where(x => x.ParentObjectName == item.ObjectName).ToList();
                    foreach (var dependent in dependentItems)
                    {
                        dependent.Status = ImportStatus.Error;
                        dependent.LastError = $"Parent table {item.ObjectName} failed";
                    }
                    
                    Logger.LogError($"Import failed for {item.ObjectName} after {item.RetryCount} retries. Removed {dependentItems.Count} dependent items.");
                }
            }
        }

        /// <summary>
        /// Get queue statistics
        /// </summary>
        public QueueStatistics GetStatistics()
        {
            lock (_queueLock)
            {
                return new QueueStatistics
                {
                    TotalItems = _queue.Count,
                    QueuedItems = _queue.Count(x => x.Status == ImportStatus.Queued),
                    RunningItems = _queue.Count(x => x.Status == ImportStatus.Running),
                    CompletedItems = _queue.Count(x => x.Status == ImportStatus.Success),
                    ErrorItems = _queue.Count(x => x.Status == ImportStatus.Error),
                    RetryItems = _queue.Count(x => x.Status == ImportStatus.Retry)
                };
            }
        }

        /// <summary>
        /// Check if queue is empty (no more work)
        /// </summary>
        public bool IsEmpty()
        {
            lock (_queueLock)
            {
                return !_queue.Any(x => x.Status == ImportStatus.Queued || x.Status == ImportStatus.Retry);
            }
        }

        /// <summary>
        /// Determine load type based on table name - inspired by SQLWATCH's logic
        /// </summary>
        private LoadType DetermineLoadType(string tableName)
        {
            if (!Config.UseSmartLoadStrategy)
                return Config.fullLoad ? LoadType.Full : LoadType.Delta;

            // Meta tables always full load (small, structural data)
            if (tableName.Contains("sqlwatch_meta") && Config.MetaTablesFullLoadOnly)
                return LoadType.Full;

            // Logger tables can use delta load (large, time-series data)
            if (tableName.Contains("sqlwatch_logger") && Config.LoggerTablesDeltaLoad && !Config.fullLoad)
                return LoadType.Delta;

            return LoadType.Full;
        }

        /// <summary>
        /// Determine parent object based on SQLWATCH's dependency logic
        /// </summary>
        private string DetermineParentObject(string tableName, string sqlInstance)
        {
            if (!Config.UseDependencyBasedOrdering)
                return null;

            // Simplified dependency mapping (based on SQLWATCH's logic)
            switch (tableName)
            {
                case "dbo.sqlwatch_meta_server":
                    return null; // Root object
                case "dbo.sqlwatch_meta_database":
                    return "dbo.sqlwatch_meta_server";
                case "dbo.sqlwatch_meta_table":
                    return "dbo.sqlwatch_meta_database";
                case "dbo.sqlwatch_logger_snapshot_header":
                    return "dbo.sqlwatch_meta_server";
                default:
                    if (tableName.Contains("sqlwatch_meta"))
                        return "dbo.sqlwatch_meta_server";
                    if (tableName.Contains("sqlwatch_logger"))
                        return "dbo.sqlwatch_logger_snapshot_header";
                    return null;
            }
        }

        /// <summary>
        /// Check if object has dependency
        /// </summary>
        private int HasDependency(string parentObjectName)
        {
            return string.IsNullOrEmpty(parentObjectName) ? 0 : 1;
        }
    }

    /// <summary>
    /// Queue statistics
    /// </summary>
    public class QueueStatistics
    {
        public int TotalItems { get; set; }
        public int QueuedItems { get; set; }
        public int RunningItems { get; set; }
        public int CompletedItems { get; set; }
        public int ErrorItems { get; set; }
        public int RetryItems { get; set; }

        public double CompletionPercentage => TotalItems > 0 ? (CompletedItems * 100.0 / TotalItems) : 0;
        public bool HasErrors => ErrorItems > 0;
        public bool IsComplete => QueuedItems == 0 && RunningItems == 0 && RetryItems == 0;
    }
}
