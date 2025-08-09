// Example integration for Program.cs - showing how to use the new queue-based architecture

/*
 * Add this integration code to your existing Program.cs to leverage SQLWATCH's T-SQL architecture concepts
 * This file demonstrates the concepts but should be integrated into the actual Program.cs
 */

using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;

namespace SqlWatchImport
{
    /// <summary>
    /// Example integration methods for queue-based architecture
    /// These methods show how to integrate SQLWATCH T-SQL concepts but should be merged into actual Program.cs
    /// </summary>
    public static class QueueIntegrationExample
    {
        /// <summary>
        /// Enhanced main processing method with queue-based architecture option
        /// </summary>
        public static async Task ProcessWithQueueArchitecture()
        {
            try
            {
                Logger.LogVerbose("Starting import process with queue-based architecture (inspired by SQLWATCH T-SQL)");

                // Initialize components
                var connectionStringRepository = GetRepositoryConnectionString();
                var queueManager = new ImportQueueManager(connectionStringRepository);
                var smartLoadStrategy = new SmartLoadStrategy(connectionStringRepository);

                // Get remote instances and tables (existing logic)
                var remoteInstances = GetRemoteInstances();
                var tables = GetTablesToImport();

                if (Config.UseQueueBasedProcessing)
                {
                    await ProcessWithQueue(queueManager, smartLoadStrategy, remoteInstances, tables);
                }
                else
                {
                    // Use existing parallel processing approach
                    await ProcessWithExistingApproach(remoteInstances, tables);
                }

                Logger.LogVerbose("Import process completed successfully");
            }
            catch (Exception ex)
            {
                Logger.LogError("Fatal error in queue-based processing", ex.Message);
                Config.hasErrors = true;
                throw;
            }
        }

        /// <summary>
        /// Process using queue-based architecture (inspired by SQLWATCH)
        /// </summary>
        private static async Task ProcessWithQueue(ImportQueueManager queueManager, SmartLoadStrategy smartLoadStrategy, 
            List<SqlWatchInstance> instances, DataTable tables)
        {
            Logger.LogVerbose("Using queue-based processing architecture");

            // Step 1: Populate queue (like usp_sqlwatch_repository_remote_table_enqueue)
            Logger.LogVerbose("Populating import queue...");
            var queuePopulated = await queueManager.PopulateQueue(instances, tables);
            
            if (!queuePopulated)
            {
                Logger.LogError("Failed to populate import queue");
                return;
            }

            var initialStats = queueManager.GetStatistics();
            Logger.LogInformation($"Queue populated with {initialStats.TotalItems} items for processing");

            // Step 2: Create worker threads (like SQLWATCH's 50 worker jobs)
            var maxWorkers = Math.Min(Config.GlobalConcurrencyLimit, Environment.ProcessorCount * 2);
            var workers = new List<Task>();

            Logger.LogVerbose($"Starting {maxWorkers} worker threads for queue processing");

            for (int i = 0; i < maxWorkers; i++)
            {
                var threadName = $"QueueWorker-{i + 1}";
                var worker = ProcessQueueWorker(queueManager, smartLoadStrategy, threadName);
                workers.Add(worker);
            }

            // Step 3: Monitor progress and wait for completion
            await MonitorQueueProgress(queueManager, workers);

            // Step 4: Report final statistics
            var finalStats = queueManager.GetStatistics();
            Logger.LogInformation($"Queue processing completed: {finalStats.CompletedItems} successful, {finalStats.ErrorItems} errors");

            if (finalStats.HasErrors)
            {
                Config.hasErrors = true;
                Logger.LogError($"Import completed with {finalStats.ErrorItems} errors");
            }
        }

        /// <summary>
        /// Queue worker thread (inspired by usp_sqlwatch_repository_remote_table_import)
        /// </summary>
        private static async Task ProcessQueueWorker(ImportQueueManager queueManager, SmartLoadStrategy smartLoadStrategy, string threadName)
        {
            Logger.LogVerbose($"Queue worker {threadName} started");

            try
            {
                while (!queueManager.IsEmpty())
                {
                    // Dequeue next item (like usp_sqlwatch_repository_remote_table_dequeue)
                    var queueItem = queueManager.DequeueNext(threadName);
                    
                    if (queueItem == null)
                    {
                        // No work available, wait briefly and retry
                        await Task.Delay(1000);
                        continue;
                    }

                    Logger.LogVerbose($"Processing {queueItem.ObjectName} for {queueItem.SqlInstance} (Thread: {threadName})");

                    try
                    {
                        // Determine optimal load strategy
                        var loadType = await smartLoadStrategy.DetermineLoadStrategy(
                            queueItem.ObjectName, 
                            queueItem.SqlInstance, 
                            queueItem.LoadType);

                        // Process the table import (use existing logic)
                        var success = await ProcessTableImport(queueItem, loadType);

                        // Mark as completed
                        queueManager.MarkCompleted(queueItem, success);

                        if (success)
                        {
                            Logger.LogVerbose($"Successfully processed {queueItem.ObjectName} for {queueItem.SqlInstance}");
                        }
                    }
                    catch (Exception ex)
                    {
                        Logger.LogError($"Error processing {queueItem.ObjectName} for {queueItem.SqlInstance}", ex.Message);
                        queueManager.MarkCompleted(queueItem, false, ex.Message);
                    }
                }
            }
            catch (Exception ex)
            {
                Logger.LogError($"Queue worker {threadName} failed", ex.Message);
            }
            finally
            {
                Logger.LogVerbose($"Queue worker {threadName} completed");
            }
        }

        /// <summary>
        /// Monitor queue progress and provide status updates
        /// </summary>
        private static async Task MonitorQueueProgress(ImportQueueManager queueManager, List<Task> workers)
        {
            var lastReported = DateTime.MinValue;
            
            while (!queueManager.IsEmpty() || workers.Any(w => !w.IsCompleted))
            {
                var stats = queueManager.GetStatistics();
                
                // Report progress every 30 seconds
                if (DateTime.Now.Subtract(lastReported).TotalSeconds >= 30)
                {
                    Logger.LogInformation($"Queue Progress: {stats.CompletionPercentage:F1}% " +
                                      $"({stats.CompletedItems}/{stats.TotalItems} completed, " +
                                      $"{stats.RunningItems} running, " +
                                      $"{stats.QueuedItems} queued, " +
                                      $"{stats.ErrorItems} errors)");
                    
                    lastReported = DateTime.Now;
                }

                await Task.Delay(5000); // Check every 5 seconds
            }

            // Wait for all workers to complete
            await Task.WhenAll(workers);
        }

        /// <summary>
        /// Process individual table import (integrate with existing SqlWatchInstance logic)
        /// </summary>
        private static async Task<bool> ProcessTableImport(ImportQueueItem queueItem, LoadType loadType)
        {
            try
            {
                // Find the SqlWatchInstance for this queue item
                var instance = GetSqlWatchInstance(queueItem.SqlInstance);
                if (instance == null)
                {
                    Logger.LogError($"Could not find SqlWatchInstance for {queueItem.SqlInstance}");
                    return false;
                }

                // For this example implementation, we'll use placeholder values
                // In a real implementation, these would come from the table metadata
                var primaryKeys = "[sql_instance]"; // Placeholder - should come from table metadata
                var hasIdentity = false; // Placeholder - should come from table metadata
                var hasLastSeen = false; // Placeholder - should come from table metadata
                var hasLastUpdated = false; // Placeholder - should come from table metadata
                var joins = "source.[sql_instance] = target.[sql_instance]"; // Placeholder
                var updateColumns = ""; // Placeholder - should come from table metadata
                var allColumns = "*"; // Placeholder - should come from table metadata

                // Use existing import logic with load type override
                var originalLoadType = Config.fullLoad;
                Config.fullLoad = (loadType == LoadType.Full);

                try
                {
                    var result = await instance.ImportTableAsync(
                        queueItem.ObjectName,
                        primaryKeys,
                        hasIdentity,
                        hasLastSeen,
                        hasLastUpdated,
                        joins,
                        updateColumns,
                        allColumns);
                    return result;
                }
                finally
                {
                    Config.fullLoad = originalLoadType; // Restore original setting
                }
            }
            catch (Exception ex)
            {
                Logger.LogError($"Error importing table {queueItem.ObjectName}", ex.Message);
                return false;
            }
        }

        // Helper methods would go here...
        private static SqlWatchInstance GetSqlWatchInstance(string sqlInstance) 
        { 
            // Implementation would find and return the SqlWatchInstance for the given SQL instance
            // This is a placeholder - in real implementation, maintain a dictionary or collection of instances
            return null; 
        }
        
        private static List<SqlWatchInstance> GetRemoteInstances() 
        { 
            // Existing implementation - returns list of remote SQL instances
            return new List<SqlWatchInstance>(); 
        }
        
        private static DataTable GetTablesToImport() 
        { 
            // Existing implementation - returns table metadata
            return new DataTable(); 
        }
        
        private static Task ProcessWithExistingApproach(List<SqlWatchInstance> instances, DataTable tables) 
        { 
            // Existing implementation - current parallel processing approach
            return Task.CompletedTask;
        }
        
        private static string GetRepositoryConnectionString() 
        { 
            // Existing implementation - builds connection string for repository
            return ""; 
        }
    }
}
