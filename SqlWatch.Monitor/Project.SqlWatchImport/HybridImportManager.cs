using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;

namespace SqlWatchImport
{
    /// <summary>
    /// Manages high-concurrency imports with table-specific throttling and global coordination
    /// </summary>
    public static class HybridImportManager
    {
        private static readonly SemaphoreSlim GlobalSemaphore = new SemaphoreSlim(Config.GlobalConcurrencyLimit);
        private static readonly ConcurrentDictionary<string, SemaphoreSlim> TableSemaphores = new ConcurrentDictionary<string, SemaphoreSlim>();
        private static readonly Random StaggerRandom = new Random();

        /// <summary>
        /// Gets or creates a semaphore for table-specific concurrency control
        /// </summary>
        public static SemaphoreSlim GetTableSemaphore(string tableName)
        {
            return TableSemaphores.GetOrAdd(tableName, _ => 
                new SemaphoreSlim(GetTableConcurrency(tableName)));
        }

        /// <summary>
        /// Determines optimal concurrency based on table type
        /// </summary>
        private static int GetTableConcurrency(string tableName)
        {
            if (tableName.Contains("sqlwatch_logger"))
            {
                // Logger tables are append-only, can handle higher concurrency
                return Config.LoggerTableConcurrency;
            }
            else if (tableName.Contains("sqlwatch_meta") || tableName.Contains("sqlwatch_config"))
            {
                // Meta/config tables require upserts, need lower concurrency to reduce lock contention
                return Config.MetaTableConcurrency;
            }
            
            return Config.DefaultTableConcurrency;
        }

        /// <summary>
        /// Executes an import operation with hybrid concurrency control
        /// </summary>
        public static async Task<bool> ExecuteWithHybridControl(string tableName, string instanceName, Func<Task<bool>> operation)
        {
            // Global throttling to prevent overwhelming the central database
            await GlobalSemaphore.WaitAsync();
            
            try 
            {
                // Table-specific throttling to reduce lock contention per table
                var tableSemaphore = GetTableSemaphore(tableName);
                await tableSemaphore.WaitAsync();
                
                try 
                {
                    // Stagger operations to reduce peak load
                    if (Config.StaggerDelayMaxMs > 0)
                    {
                        var delay = StaggerRandom.Next(Config.StaggerDelayMinMs, Config.StaggerDelayMaxMs);
                        await Task.Delay(delay);
                        
                        Logger.LogVerbose($"Staggered import for \"{instanceName}.{tableName}\" by {delay}ms");
                    }
                    
                    Logger.LogVerbose($"Starting controlled import: \"{instanceName}.{tableName}\" " +
                        $"(Global: {Config.GlobalConcurrencyLimit - GlobalSemaphore.CurrentCount}/{Config.GlobalConcurrencyLimit}, " +
                        $"Table: {GetTableConcurrency(tableName) - tableSemaphore.CurrentCount}/{GetTableConcurrency(tableName)})");
                    
                    return await operation();
                }
                finally 
                {
                    tableSemaphore.Release();
                }
            }
            finally 
            {
                GlobalSemaphore.Release();
            }
        }

        /// <summary>
        /// Determines if staging approach should be used for a table
        /// </summary>
        public static bool ShouldUseStaging(string tableName)
        {
            return Config.UseStaginForMetaTables && 
                   (tableName.Contains("sqlwatch_meta") || tableName.Contains("sqlwatch_config"));
        }

        /// <summary>
        /// Gets current concurrency statistics for monitoring
        /// </summary>
        public static string GetConcurrencyStats()
        {
            var stats = $"Global: {Config.GlobalConcurrencyLimit - GlobalSemaphore.CurrentCount}/{Config.GlobalConcurrencyLimit}";
            
            foreach (var kvp in TableSemaphores)
            {
                var tableName = kvp.Key;
                var semaphore = kvp.Value;
                var maxConcurrency = GetTableConcurrency(tableName);
                var currentUsage = maxConcurrency - semaphore.CurrentCount;
                
                stats += $", {tableName}: {currentUsage}/{maxConcurrency}";
            }
            
            return stats;
        }

        /// <summary>
        /// Cleanup resources when application shuts down
        /// </summary>
        public static void Dispose()
        {
            GlobalSemaphore?.Dispose();
            
            foreach (var semaphore in TableSemaphores.Values)
            {
                semaphore?.Dispose();
            }
            
            TableSemaphores.Clear();
        }
    }
}
