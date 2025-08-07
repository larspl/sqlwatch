using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Web.Caching;

namespace SqlWatchImport
{
	class SqlWatchInstance : IDisposable
	{

		public static double t1 = 0; // Total time ms spent on bulk copy full load;
		public static double t2 = 0; // Total time ms spent on merge;
		public static double t3 = 0; // Total time ms spent on getting last snapshot from the central repository;
		public static double t4 = 0; // Total time ms spent on bulk copy delta load;

		public string SqlInstance { get; set; }
		public string SqlDatabase { get; set; }
		public string SqlUser { get; set; }
		public string SqlSecret { get; set; }
		public int ConnectTimeout { get; set; } = 60;
		public string Hostname { get; set; } = "";
		public string ConnectionString
		{
			get
			{
				SqlConnectionStringBuilder conn = new SqlConnectionStringBuilder
				{
					DataSource = this.Hostname != "" ? this.Hostname : this.SqlInstance,
					InitialCatalog = this.SqlDatabase,
					ConnectTimeout = Config.RemoteInstanceConnectTimeOut,
					MinPoolSize = 0,
					Pooling = true,
					ApplicationName = Config.ApplicationName,
					IntegratedSecurity = this.SqlUser == "" ? true : false,
					UserID = this.SqlUser == "" ? "" : this.SqlUser,
					Password = this.SqlSecret == "" ? "" : Tools.Decrypt(this.SqlSecret)
				};

				return conn.ConnectionString;
			}
		}
		public string ConnectionStringRepository { get; set; }
		public string Vesion { get; set; }

		void IDisposable.Dispose() { }

		public class RemoteInstance
		{
			public string SqlInstance { get; set; }
			public string SqlDatabase { get; set; }
			public string Hostname { get; set; }
			public string SqlUser { get; set; }
			public string SqlSecret { get; set; }
			public string Environment { get; set; }
		}

		public class SqlWatchTable
		{
			public string Name { get; set; }
			public int DependencyLevel { get; set; }
			public bool HasLastSeen { get; set; }
			public string PrimaryKey { get; set; }
			public bool HasIdentity { get; set; }
			public bool HasLastUpdated { get; set; }
			public string Joins { get; set; }
			public string UpdateColumns { get; set; }
			public string AllColumns { get; set; }
		}

		public class SqlWatchHeader
		{
			public string SqlInstance { get; set; }
			public int SnapshotTypeId { get; set; }
			public string SnapshotTime { get; set; } // storing time in ISO string as we're passing it straight back to SQL without doing anything with it in C#

		}

		public class SqlWatchTableSnapshotTypeId
		{
			public string TableName { get; set; }
			public int SnapshotTypeId { get; set; }
		}

		public async Task<List<SqlWatchTableSnapshotTypeId>> GetSqlWatchTableSnapshotTypeIdAsync()
		{
			// Get list of tables first:
			string sql = @"select stuff(( 
						select ' union all select top 1 TABLE_NAME=''' + TABLE_NAME + ''', snapshot_type_id from ' + TABLE_NAME
						from (
							select distinct TABLE_NAME=TABLE_SCHEMA + '.' + TABLE_NAME
							from INFORMATION_SCHEMA.COLUMNS
							where TABLE_NAME like 'sqlwatch_logger%'
							and COLUMN_NAME = 'snapshot_type_id'
							) t
						for xml path('')),1,10,'')";

			List<SqlWatchTableSnapshotTypeId> SqlWatchTableSnapshotType = new List<SqlWatchTableSnapshotTypeId>();

			using (SqlConnection connection = new SqlConnection(this.ConnectionString))
			{
				using (SqlCommand command = new SqlCommand(sql, connection))
				{
					await connection.OpenAsync();

					// Execute the first query to build sql string 
					sql = (await command.ExecuteScalarAsync()).ToString();
					command.CommandText = sql;

					SqlDataReader reader = await command.ExecuteReaderAsync();

					if (reader.HasRows)
					{
						while (reader.Read())
						{
							SqlWatchTableSnapshotTypeId Type = new SqlWatchTableSnapshotTypeId
							{
								TableName = reader["TABLE_NAME"].ToString(),
								SnapshotTypeId = int.Parse(reader["snapshot_type_id"].ToString())
							};

							SqlWatchTableSnapshotType.Add(Type);
						}
					}

					connection.Close();
				}
			}

			return SqlWatchTableSnapshotType;
		}

		public async Task<List<RemoteInstance>> GetRemoteInstancesAsync()
		{
			// Gets the list of remote servers to import data from

			string sql = @"select RemoteSqlInstance = sql_instance
									, Hostname=isnull(hostname,sql_instance) + isnull(','+convert(varchar(10),[sql_port]),'')
									, SqlSecret=isnull([sql_secret],'')
									, SqlUser=isnull([sql_user],'')
									, SqlWatchDatabase = [sqlwatch_database_name]
									, Environment = isnull([environment],'')
							from [dbo].[sqlwatch_config_sql_instance]
							where repo_collector_is_active = 1";

			// Add environment filter if not "ALL"
			if (Config.EnvironmentToProcess != "ALL")
			{
				sql += " and isnull([environment],'') = @EnvironmentToProcess";
			}

			List<RemoteInstance> RemoteSqlInstance = new List<RemoteInstance>();

			using (SqlConnection connection = new SqlConnection(this.ConnectionString))
			{
				using (SqlCommand command = new SqlCommand(sql, connection))
				{
					// Add parameter if filtering by specific environment
					if (Config.EnvironmentToProcess != "ALL")
					{
						command.Parameters.AddWithValue("@EnvironmentToProcess", Config.EnvironmentToProcess);
					}

					await connection.OpenAsync();
					SqlDataReader reader = await command.ExecuteReaderAsync();

					if (reader.HasRows)
					{
						while (reader.Read())
						{
							RemoteInstance RemoteInstance = new RemoteInstance
							{
								SqlInstance = reader["RemoteSqlInstance"].ToString(),
								SqlDatabase = reader["SqlWatchDatabase"].ToString(),
								Hostname = reader["Hostname"].ToString(),
								SqlUser = reader["SqlUser"].ToString(),
								SqlSecret = reader["SqlSecret"].ToString(),
								Environment = reader["Environment"].ToString()
							};

							RemoteSqlInstance.Add(RemoteInstance);
						}
					}

					connection.Close();
				}
			}

			return RemoteSqlInstance;
		}

		public async Task<List<SqlWatchTable>> GetTablesToImportAsync()
		{
			// Gets the list of remote servers to import data from

			string sql = @"SELECT TableName=[table_name]
								  ,DependencyLevel=[dependency_level]
								  ,HasLastSeen=[has_last_seen]
								  ,PrimaryKey=[primary_key]
								  ,HasIdentity=[has_identity]
								  ,HasLastUpdated=[has_last_updated]
								  ,Joins=[joins]
								  ,UpdateColumns=[updatecolumns]
								  ,AllColumns=[allcolumns]
							  FROM [dbo].[sqlwatch_stage_repository_tables_to_import]
							  ORDER BY dependency_level";

			List<SqlWatchTable> SqlWatchTable = new List<SqlWatchTable>();

			using (SqlConnection connection = new SqlConnection(this.ConnectionString))
			{
				using (SqlCommand command = new SqlCommand(sql, connection))
				{
					await connection.OpenAsync();

					SqlDataReader reader = await command.ExecuteReaderAsync();

					if (reader.HasRows)
					{
						while (reader.Read())
						{
							SqlWatchTable Table = new SqlWatchTable
							{
								Name = reader["TableName"].ToString(),
								DependencyLevel = int.Parse(reader["DependencyLevel"].ToString()),
								HasLastSeen = bool.Parse(reader["HasLastSeen"].ToString()),
								PrimaryKey = reader["PrimaryKey"].ToString(),
								HasIdentity = bool.Parse(reader["HasIdentity"].ToString()),
								HasLastUpdated = bool.Parse(reader["HasLastUpdated"].ToString()),
								Joins = reader["Joins"].ToString(),
								UpdateColumns = reader["UpdateColumns"].ToString(),
								AllColumns = reader["AllColumns"].ToString()
							};

							SqlWatchTable.Add(Table);
						}
					}

					connection.Close();
				}
			}

			return SqlWatchTable;
		}

		public async Task<List<SqlWatchHeader>> GetSqlWatchHeaderAsync()
		{
			// Gets the list of remote servers to import data from

			string sql = $@"select sql_instance, snapshot_type_id, 
								snapshot_time=convert(varchar(23),max(snapshot_time),121)
								from dbo.sqlwatch_logger_snapshot_header
								where sql_instance = '{ this.SqlInstance }'
								group by sql_instance, snapshot_type_id";

			List<SqlWatchHeader> SqlWatchHeader = new List<SqlWatchHeader>();

			using (SqlConnection connection = new SqlConnection(this.ConnectionStringRepository))
			{
				using (SqlCommand command = new SqlCommand(sql, connection))
				{
					await connection.OpenAsync();
					SqlDataReader reader = await command.ExecuteReaderAsync();

					if (reader.HasRows)
					{
						while (reader.Read())
						{
							SqlWatchHeader Header = new SqlWatchHeader
							{
								SqlInstance = reader["sql_instance"].ToString(),
								SnapshotTypeId = int.Parse(reader["snapshot_type_id"].ToString()),
								SnapshotTime = reader["snapshot_time"].ToString()
							};

							SqlWatchHeader.Add(Header);
						}
					}

					connection.Close();
				}
			}

			return SqlWatchHeader;
		}

		public async Task<bool> ImportAsync(List<SqlWatchTable> SqlWatchTables)
		{

			Stopwatch sw = Stopwatch.StartNew();

			string SqlInstance = this.SqlInstance;

			Logger.LogMessage($"Importing: \"{ SqlInstance }\" using Hybrid Approach");

			if (await IsOnline() == false)
			{
				return false;
			}

			// Use dependency-level processing but with hybrid concurrency control
			int depLevels = (SqlWatchTables.Max(s => s.DependencyLevel));
			for (int i = 1; i <= depLevels; i++)
			{
				var tablesAtLevel = SqlWatchTables.FindAll(s => s.DependencyLevel == i);
				Logger.LogVerbose($"Processing dependency level {i} with {tablesAtLevel.Count} tables for \"{SqlInstance}\"");

				// Process tables at same dependency level in parallel but with hybrid control
				var levelTasks = tablesAtLevel.Select(async table =>
				{
					return await HybridImportManager.ExecuteWithHybridControl(
						table.Name,
						SqlInstance,
						() => ImportTableAsync(
							table.Name,
							table.PrimaryKey,
							table.HasIdentity,
							table.HasLastSeen,
							table.HasLastUpdated,
							table.Joins,
							table.UpdateColumns,
							table.AllColumns
						)
					);
				}).ToArray();

				var results = await Task.WhenAll(levelTasks);

				// Check if any table import failed - if so, break the dependency chain
				if (results.Any(result => result == false))
				{
					Logger.LogWarning($"One or more table imports failed at dependency level {i} for \"{SqlInstance}\". Breaking dependency chain.");
					break;
				}

				Logger.LogVerbose($"Completed dependency level {i} for \"{SqlInstance}\". Concurrency stats: {HybridImportManager.GetConcurrencyStats()}");
			}

			Logger.LogMessage($"Finished: \"{ SqlInstance }\". Time taken: { sw.Elapsed.TotalMilliseconds }ms");
			return true;
		}

		public async Task<bool> ImportTableAsync(
					string tableName,
					string primaryKeys,
					bool HasIdentity,
					bool HasLastSeen,
					bool HasLastUpdated,
					string Joins,
					string UpdateColumns,
					string AllColumns
			)
		{
			string SqlInstance = this.SqlInstance;

			Stopwatch tt = Stopwatch.StartNew();

			// Determine if we should use staging approach
			bool useStaging = HybridImportManager.ShouldUseStaging(tableName);
			
			// Create truly unique staging table names to avoid collisions
			string uniqueId = $"{Environment.MachineName}_{Thread.CurrentThread.ManagedThreadId}_{DateTime.Now:yyyyMMddHHmmssfff}_{Guid.NewGuid().ToString("N").Substring(0, 8)}";
			// Clean up special characters that could cause SQL issues
			string safeInstanceName = SqlInstance.Replace("\\", "_").Replace(".", "_").Replace("-", "_").Replace(" ", "_").Replace("[", "").Replace("]", "");
			string safeTableName = tableName.Replace("dbo.", "").Replace("[", "").Replace("]", "");
			
			string workingTableName = useStaging ? 
				$"[#stg_{safeTableName}_{safeInstanceName}_{DateTime.Now:yyyyMMddHHmmss}_{Thread.CurrentThread.ManagedThreadId}]" : 
				$"[#{ tableName }]";

			Logger.LogVerbose($"Starting import of \"{tableName}\" for \"{SqlInstance}\" using {(useStaging ? "staging" : "direct")} approach");

			using (SqlConnection connectionRepository = new SqlConnection(this.ConnectionStringRepository))
			{
				Stopwatch tc = Stopwatch.StartNew();
				await connectionRepository.OpenAsync();
				//Logger.LogVerbose($"Opened connection to the Central Repository in { tc.Elapsed.TotalMilliseconds }ms.");

				string sql = "";

				using (SqlConnection connectionRemote = new SqlConnection(this.ConnectionString))
				{
					try
					{
						Stopwatch t = Stopwatch.StartNew();
						await connectionRemote.OpenAsync();
						//Logger.LogVerbose($"Opened connection to \"{ SqlInstance }\" to import \"{tableName}\" in { t.Elapsed.TotalMilliseconds }ms.");
					}
					catch (SqlException e)
					{
						Logger.LogError($"Failed to open connection to { this.SqlInstance }", e.Errors[0].Message);
						return false;
					}

					string lastSeenInRepo = Config.fullLoad == true ? "1970-01-01" : "";
					string lastUpdatedInRepo = Config.fullLoad == true ? "1970-01-01" : "";

					if (HasLastSeen == true && lastSeenInRepo == "")
					{
						// The nolock here is safe as nothing is modifying or writing data for specific instance but it does not block other threads modifying their own instances
						sql = $"select convert(varchar(30),isnull(max(date_last_seen),'1970-01-01'),121) " +
							$"from { tableName } with (nolock) " +
							$"where sql_instance = '{ this.SqlInstance }' ";

						using (SqlCommand cmdGetRepoLastSeen = new SqlCommand(sql, connectionRepository))
						{
							Stopwatch t = Stopwatch.StartNew();
							lastSeenInRepo = (await cmdGetRepoLastSeen.ExecuteScalarAsync()).ToString();
							Logger.LogVerbose($"Fetched \"Last Seen\" (\"{lastSeenInRepo}\") from \"{ tableName }\" for \"[{ SqlInstance }]\" in { t.Elapsed.TotalMilliseconds }ms.");
						}
					}
					else if (HasLastUpdated == true && lastUpdatedInRepo == "")
					{
						// The nolock here is safe as nothing is modifying or writing data for specific instance but it does not block other threads modifying their own instances
						sql = $"select convert(varchar(30),isnull(max(date_updated),'1970-01-01'),121) " +
							$"from { tableName } with (nolock) " +
							$"where sql_instance = '{ this.SqlInstance }' ";

						using (SqlCommand cmdGetRepoLastUpdated = new SqlCommand(sql, connectionRepository))
						{
							Stopwatch t = Stopwatch.StartNew();
							lastUpdatedInRepo = (await cmdGetRepoLastUpdated.ExecuteScalarAsync()).ToString();
							Logger.LogVerbose($"Fetched \"Last Updated\" (\"{lastUpdatedInRepo}\") from \"{ tableName }\" for \"[{ SqlInstance }]\" in { t.Elapsed.TotalMilliseconds }ms.");
						}
					}

					// For the header table, we have to pull new headers for each snapshot
					// We are going to build a where clause dynamically
					if (tableName == "dbo.sqlwatch_logger_snapshot_header")
					{
						// If we are doing a full load, we are going to skip the below and default to 1970-01-01 further below:
						var snapshotTimes = "";
						if (!Config.fullLoad)
						{
							snapshotTimes = await SnapshotTimeForInstance(connectionRepository);
						}

						sql = $@"select * 
								from [dbo].[sqlwatch_logger_snapshot_header] with (readpast) 
								where [snapshot_time] > { (snapshotTimes == "" ? "'1970-01-01'" : snapshotTimes) } ";
					}
					// For logger tables excluding the snapshot header, check the last snapshot_time we have in the central respository
					// and only import new records from remote
					else if (tableName.Contains("sqlwatch_logger"))
					{

						// Check if we are running fullload and set the SnapshotTime way in the past if yes to force load all data from the remote:
						string snapshotTime = Config.fullLoad == true ? "1970-01-01" : "";

						if (snapshotTime == "")
						{
							// The nolock here is safe as nothing is modifying or writing data for specific instance but it does not block other threads modifying their own instances
							sql = $"select [snapshot_time]=isnull(convert(varchar(23),max([snapshot_time]),121),'1970-01-01') " +
								$"from { tableName } with (nolock)" +
								$"where sql_instance = '{ this.SqlInstance }'";

							using (SqlCommand cmdGetLastSnapshotTime = new SqlCommand(sql, connectionRepository))
							{
								Stopwatch t = Stopwatch.StartNew();
								snapshotTime = (await cmdGetLastSnapshotTime.ExecuteScalarAsync()).ToString();
								Logger.LogVerbose($"Fetched \"Last Snapshot Time\" (\"{ (snapshotTime == "" ? "1970-01-01" : snapshotTime) }\") from \"{ tableName }\" for \"[{ SqlInstance }]\" in { t.Elapsed.TotalMilliseconds }ms.");
							}
						}

						// If we are not running full load, we can genuinely not have snapshottime if the tables are empty. In which case, again, we have to pull all data:
						sql = $"select * from { tableName } with (readpast) where snapshot_time > '{ (snapshotTime == "" ? "1970-01-01" : snapshotTime) }'";
					}
					// For any other table, we are assuming they are meta tables:
					else
					{
						sql = $"select * from { tableName } with (readpast)";

						// Some tables may have both, the last seen and last updated fields.
						// The last seen field takes precedence over the last udpated.

						if (lastSeenInRepo != "" && HasLastSeen == true)
						{
							sql += $" where date_last_seen > '{ lastSeenInRepo }'";
						}
						else if (lastUpdatedInRepo != "" && HasLastUpdated == true)
						{
							sql += $" where date_updated > '{ lastUpdatedInRepo }'";
						}
					}

					// ------------------------------------------------------------------------------------------------------------------------------
					// BULK COPY
					// ------------------------------------------------------------------------------------------------------------------------------
					int rowsCopied = 0;
					string originalDataQuery = ""; // Store original query for retries

					using (SqlCommand cmdGetData = new SqlCommand(sql, connectionRemote))
					{
						originalDataQuery = sql; // Preserve the original data query for retries
						cmdGetData.CommandTimeout = Config.BulkCopyTimeout;

						//import data into #t table
						try
						{
							Stopwatch bk1 = Stopwatch.StartNew();
							using (SqlDataReader reader = await cmdGetData.ExecuteReaderAsync())
							{

								if (reader.HasRows == false)
								{
									Logger.LogVerbose($"Nothing to import from \"[{ SqlInstance }].{ tableName }\"");

									if (tableName == "dbo.sqlwatch_logger_snapshot_header")
									{
										Logger.LogWarning($"No new records in the snapshot header from \"{SqlInstance}\".");
										// At this point we want to break importing of any child tables:
										return false;
									}
									else
									{
										return true;
									}
								}

								string PkColumns = primaryKeys;

								// Ensure staging table name doesn't exceed SQL Server limits (128 characters)
								if (workingTableName.Length > 128)
								{
									string shortId = $"{DateTime.Now:yyyyMMddHHmmss}_{Thread.CurrentThread.ManagedThreadId}";
									workingTableName = useStaging ? 
										$"[#stg_{safeTableName.Substring(0, Math.Min(20, safeTableName.Length))}_{shortId}]" : 
										$"[#{ tableName }]";
								}

								sql = $"select top 0 * into {workingTableName} from { tableName } with (nolock);";

								// For staging tables, don't add primary key constraint to allow duplicates during staging
								// We'll handle deduplication during the merge process
								if (PkColumns != "" && !useStaging)
								{
									sql += $"alter table {workingTableName} add primary key ({ PkColumns }); ";
								}

								using (SqlCommand commandRepository = new SqlCommand(sql, connectionRepository))
								{

									try
									{
										Stopwatch t = Stopwatch.StartNew();
										await commandRepository.ExecuteNonQueryAsync();
										Logger.LogVerbose($"Created landing table \"{workingTableName}\" for \"{ SqlInstance }\" in { t.Elapsed.TotalMilliseconds }ms.");
									}
									catch (SqlException e)
									{
										Logger.LogError($"Failed to prepare table for \"[{ SqlInstance }].{ tableName}\"", e.Errors[0].Message);
										return false;
									}
								}

								// Optimize bulk copy options based on table type and staging approach
								var options = SqlBulkCopyOptions.KeepIdentity;
								
								if (tableName.Contains("sqlwatch_logger"))
								{
									// Logger tables: Use faster options for append-only data
									options |= SqlBulkCopyOptions.CheckConstraints;
								}
								else if (useStaging)
								{
									// Staging tables: Optimize for speed since we'll merge later
									options |= SqlBulkCopyOptions.TableLock;
								}

								using (SqlBulkCopy sqlBulkCopy = new SqlBulkCopy(connectionRepository, options, null))
								{

									sqlBulkCopy.DestinationTableName = workingTableName;
									sqlBulkCopy.BulkCopyTimeout = Config.BulkCopyTimeout;
									sqlBulkCopy.EnableStreaming = Config.SqlBkEnableStreaming;
									
									// Adjust batch size based on table type
									if (useStaging && tableName.Contains("sqlwatch_meta"))
									{
										sqlBulkCopy.BatchSize = Config.MergeBatchSize;
									}
									else
									{
										sqlBulkCopy.BatchSize = Config.SqlBkBatchSize;
									}

									try
									{
										await sqlBulkCopy.WriteToServerAsync(reader);

										rowsCopied = SqlBulkCopyExtension.RowsCopiedCount(sqlBulkCopy);

										t1 += bk1.Elapsed.TotalMilliseconds;
										Logger.LogVerbose($"Copied { rowsCopied } { (rowsCopied == 1 ? "row" : "rows") } from \"[{ SqlInstance }].{ tableName }\" to \"{workingTableName}\" in { bk1.Elapsed.TotalMilliseconds }ms using {(useStaging ? "staging" : "direct")} approach.");
									}
									catch (SqlException e)
									{
										Logger.LogError($"Failed to Bulk Copy data from \"[{ SqlInstance }].{ tableName }\"", e.Errors[0].Message);
										return false;
									}
								}
							}
						}
						catch (SqlException e)
						{
							Logger.LogError($"Failed to populate DataReader with remote Data from \"[{ SqlInstance }].{ tableName }\"", e.Errors[0].Message, sql);
							return false;
						}
					}

					// ------------------------------------------------------------------------------------------------------------------------------
					// OPTIMIZED MERGE with Hybrid Approach
					// ------------------------------------------------------------------------------------------------------------------------------
					if (rowsCopied > 0) //&& tableName != "dbo.sqlwatch_logger_snapshot_header")
					{
						// Retry logic for the entire merge operation
						int retryCount = 0;
						int maxRetries = 3;
						
						while (retryCount <= maxRetries)
						{
							try
							{
								sql = "";

								if (HasIdentity == true)
								{
									sql += $"\nset identity_insert { tableName } on;";
								}

								string allColumns = AllColumns;

								// Use optimized merge with appropriate lock hints and conflict handling
								string lockHint = tableName.Contains("sqlwatch_logger") ? "with (ROWLOCK, READPAST)" : "with (UPDLOCK, SERIALIZABLE)";
								sql += $";merge { tableName } {lockHint} as target ";

								if (tableName.Contains("sqlwatch_logger") == true && tableName != "dbo.sqlwatch_logger_snapshot_header")
								{
									sql += $@"
										using (
										select s.* from {workingTableName} s
										inner join dbo.sqlwatch_logger_snapshot_header h
											on s.[snapshot_time] = h.[snapshot_time]
											and s.[snapshot_type_id] = h.[snapshot_type_id]
											and s.[sql_instance] = h.[sql_instance]) as source";
								}
								else if (useStaging)
								{
									// For staging tables, add deduplication to prevent constraint violations
									string primaryKeyColumns = primaryKeys;
									if (!string.IsNullOrEmpty(primaryKeyColumns))
									{
										sql += $@"using (
											select * from (
												select *, row_number() over (partition by {primaryKeyColumns} order by (select null)) as rn
												from {workingTableName}
											) deduped where rn = 1
										) as source";
									}
									else
									{
										sql += $"using {workingTableName} as source";
									}
								}
								else
								{
									sql += $"using {workingTableName} as source";
								}

								sql += $@"
									on ({ Joins })
									when not matched
									then insert ({ allColumns })
									values (source.{ allColumns.Replace(",", ",source.") })";

								string updateColumns = UpdateColumns;

								// we would never update existing logger tables
								if (updateColumns != "" && tableName.Contains("sqlwatch_logger") == false)
								{
									sql += $@"
									when matched
									then update set
									{ updateColumns }";
								}

								sql += ";";

								if (HasIdentity == true)
								{
									sql += $"\nset identity_insert { tableName } off;";
								}

								// Add cleanup for staging tables only when successful
								if (useStaging)
								{
									sql += $"\ndrop table {workingTableName};";
								}

								using (SqlCommand cmdMergeTable = new SqlCommand(sql, connectionRepository))
								{
									cmdMergeTable.CommandTimeout = Config.BulkCopyTimeout;
									
									Stopwatch mg = Stopwatch.StartNew();
									int nRows = await cmdMergeTable.ExecuteNonQueryAsync();
									t2 += mg.Elapsed.TotalMilliseconds;

									Logger.LogVerbose($"Merged { nRows } { (nRows == 1 ? "row" : "rows") } from \"{workingTableName}\" for \"{ SqlInstance }\" in { mg.Elapsed.TotalMilliseconds }ms using {(useStaging ? "staging" : "direct")} approach");
									Logger.LogSuccess($"Imported \"{ tableName }\" from \"{ SqlInstance }\" in { tt.Elapsed.TotalMilliseconds }ms");

									return true;
								}
							}
							catch (SqlException e) when (e.Number == 2627 || e.Number == 2601) // Unique constraint violations
							{
								retryCount++;
								if (retryCount <= maxRetries)
								{
									Logger.LogVerbose($"Unique constraint violation on \"{tableName}\" for \"{SqlInstance}\", retry {retryCount}/{maxRetries}. Waiting {retryCount * 200}ms...");
									
									// For meta tables, remove conflicting rows from destination before retry
									if (tableName.Contains("sqlwatch_meta") && !string.IsNullOrEmpty(primaryKeys))
									{
										try
										{
											Logger.LogVerbose($"Removing conflicting rows from destination table {tableName} for instance {SqlInstance}");
											
											// For meta tables, we should remove only the specific conflicting rows based on primary keys
											// This is more precise than removing all data for an instance
											string deleteSql;
											
											if (tableName == "dbo.sqlwatch_meta_table")
											{
												// For meta_table, remove only the specific conflicting tables/databases for this instance
												// Primary key is typically: sql_instance, sqlwatch_database_id, sqlwatch_table_id
												deleteSql = $@"
													DELETE dest FROM {tableName} dest
													INNER JOIN {workingTableName} staging 
													ON dest.sql_instance = staging.sql_instance 
													AND dest.sqlwatch_database_id = staging.sqlwatch_database_id 
													AND dest.sqlwatch_table_id = staging.sqlwatch_table_id
													WHERE dest.sql_instance = '{SqlInstance}'";
											}
											else
											{
												// For other meta tables, use precise matching based on primary keys
												deleteSql = $@"
													DELETE dest FROM {tableName} dest
													INNER JOIN {workingTableName} staging 
													ON {BuildJoinCondition(primaryKeys, "dest", "staging")}
													WHERE dest.sql_instance = '{SqlInstance}'";
											}
											
											using (SqlCommand deleteCmd = new SqlCommand(deleteSql, connectionRepository))
											{
												deleteCmd.CommandTimeout = Config.BulkCopyTimeout;
												int deletedRows = await deleteCmd.ExecuteNonQueryAsync();
												Logger.LogVerbose($"Removed {deletedRows} conflicting rows from {tableName} for {SqlInstance} (specific objects only)");
											}
										}
										catch (Exception deleteEx)
										{
											Logger.LogVerbose($"Warning: Could not remove conflicting rows from {tableName}: {deleteEx.Message}");
											
											// If precise deletion fails, try a fallback approach for meta_table only
											if (tableName == "dbo.sqlwatch_meta_table")
											{
												try
												{
													Logger.LogVerbose($"Attempting fallback deletion strategy for {tableName}");
													
													// Fallback: Get the specific database and table IDs from staging and delete only those
													string fallbackDeleteSql = $@"
														DELETE FROM {tableName} 
														WHERE sql_instance = '{SqlInstance}'
														AND (sqlwatch_database_id, sqlwatch_table_id) IN (
															SELECT DISTINCT sqlwatch_database_id, sqlwatch_table_id 
															FROM {workingTableName}
														)";
													
													using (SqlCommand fallbackCmd = new SqlCommand(fallbackDeleteSql, connectionRepository))
													{
														fallbackCmd.CommandTimeout = Config.BulkCopyTimeout;
														int fallbackDeleted = await fallbackCmd.ExecuteNonQueryAsync();
														Logger.LogVerbose($"Fallback deletion removed {fallbackDeleted} rows from {tableName} for {SqlInstance}");
													}
												}
												catch (Exception fallbackEx)
												{
													Logger.LogVerbose($"Fallback deletion also failed for {tableName}: {fallbackEx.Message}");
												}
											}
										}
									}
									
									// DON'T clean up staging table on retry - preserve for debugging
									// Only create new staging table if we're retrying
									if (retryCount > 1)
									{
										Logger.LogVerbose($"Preserving staging table {workingTableName} for debugging, creating new one for retry");
									}

									// Create new unique staging table name for retry
									string newUniqueId = $"{Environment.MachineName}_{Thread.CurrentThread.ManagedThreadId}_{DateTime.Now:yyyyMMddHHmmssfff}_{Guid.NewGuid().ToString("N").Substring(0, 8)}";
									string newSafeInstanceName = SqlInstance.Replace("\\", "_").Replace(".", "_").Replace("-", "_").Replace(" ", "_").Replace("[", "").Replace("]", "");
									string newSafeTableName = tableName.Replace("dbo.", "").Replace("[", "").Replace("]", "");
									
									workingTableName = useStaging ? 
										$"[#stg_{newSafeTableName}_{newSafeInstanceName}_{DateTime.Now:yyyyMMddHHmmss}_{Thread.CurrentThread.ManagedThreadId}_r{retryCount}]" : 
										$"[#{ tableName }_r{retryCount}]";

									// Recreate staging table with new name
									try
									{
										string recreateSql = $"select top 0 * into {workingTableName} from { tableName } with (nolock);";
										
										// Add primary key for direct approach only
										string PkColumns = primaryKeys;
										if (PkColumns != "" && !useStaging)
										{
											recreateSql += $"alter table {workingTableName} add primary key ({ PkColumns }); ";
										}

										using (SqlCommand recreateCmd = new SqlCommand(recreateSql, connectionRepository))
										{
											recreateCmd.CommandTimeout = Config.BulkCopyTimeout;
											await recreateCmd.ExecuteNonQueryAsync();
											Logger.LogVerbose($"Recreated staging table {workingTableName} for retry {retryCount}");
										}

										// Re-bulk copy the data to new staging table
										using (SqlCommand cmdGetRetryData = new SqlCommand(originalDataQuery, connectionRemote))
										{
											cmdGetRetryData.CommandTimeout = Config.BulkCopyTimeout;
											using (SqlDataReader retryReader = await cmdGetRetryData.ExecuteReaderAsync())
											{
												var options = SqlBulkCopyOptions.KeepIdentity;
												if (tableName.Contains("sqlwatch_logger"))
												{
													options |= SqlBulkCopyOptions.CheckConstraints;
												}
												else if (useStaging)
												{
													options |= SqlBulkCopyOptions.TableLock;
												}

												using (SqlBulkCopy retryBulkCopy = new SqlBulkCopy(connectionRepository, options, null))
												{
													retryBulkCopy.DestinationTableName = workingTableName;
													retryBulkCopy.BulkCopyTimeout = Config.BulkCopyTimeout;
													retryBulkCopy.EnableStreaming = Config.SqlBkEnableStreaming;
													
													if (useStaging && tableName.Contains("sqlwatch_meta"))
													{
														retryBulkCopy.BatchSize = Config.MergeBatchSize;
													}
													else
													{
														retryBulkCopy.BatchSize = Config.SqlBkBatchSize;
													}

													await retryBulkCopy.WriteToServerAsync(retryReader);
													Logger.LogVerbose($"Re-copied data to {workingTableName} for retry {retryCount}");
												}
											}
										}
									}
									catch (Exception recreateEx)
									{
										Logger.LogError($"Failed to recreate staging table for retry {retryCount}: {recreateEx.Message}");
										break; // Exit retry loop on recreation failure
									}
									
									await Task.Delay(retryCount * 200); // Progressive delay
									continue;
								}
								
								// If all retries failed, try one final full load approach for meta tables
								if (tableName.Contains("sqlwatch_meta"))
								{
									Logger.LogWarning($"All retries failed for {tableName}. Attempting emergency full load for {SqlInstance}...");
									
									try
									{
										// Save current fullLoad setting
										bool originalFullLoad = Config.fullLoad;
										
										// Temporarily enable full load
										Config.fullLoad = true;
										Logger.LogVerbose($"Enabled full load mode for emergency recovery of {tableName}");
										
										// Clear all existing data for this instance from the target table for meta tables only
										if (tableName.Contains("sqlwatch_meta"))
										{
											string clearSql = $"DELETE FROM {tableName} WHERE sql_instance = '{SqlInstance}'";
											using (SqlCommand clearCmd = new SqlCommand(clearSql, connectionRepository))
											{
												clearCmd.CommandTimeout = Config.BulkCopyTimeout;
												int clearedRows = await clearCmd.ExecuteNonQueryAsync();
												Logger.LogVerbose($"Emergency full load: Cleared {clearedRows} existing rows from {tableName} for {SqlInstance}");
											}
										}
										
										// Recursively call this method with full load enabled
										bool fullLoadResult = await ImportTableAsync(tableName, primaryKeys, HasIdentity, HasLastSeen, HasLastUpdated, Joins, UpdateColumns, AllColumns);
										
										// Restore original fullLoad setting
										Config.fullLoad = originalFullLoad;
										
										if (fullLoadResult)
										{
											Logger.LogWarning($"Emergency full load succeeded for {tableName} on {SqlInstance}");
											return true;
										}
										else
										{
											Logger.LogError($"Emergency full load also failed for {tableName} on {SqlInstance}");
										}
									}
									catch (Exception fullLoadEx)
									{
										Logger.LogError($"Emergency full load failed for {tableName} on {SqlInstance}: {fullLoadEx.Message}");
										// Restore original fullLoad setting even on exception
										Config.fullLoad = false;
									}
								}
								
								// If data doesn't exist, it's a real error
								string checkSql = $"SELECT COUNT(*) FROM {tableName} WHERE sql_instance = '{SqlInstance}'";
								using (SqlCommand checkCmd = new SqlCommand(checkSql, connectionRepository))
								{
									try
									{
										var existingCount = await checkCmd.ExecuteScalarAsync();
										if (Convert.ToInt32(existingCount) > 0)
										{
											Logger.LogVerbose($"Data already exists in \"{tableName}\" for \"{SqlInstance}\", treating as successful import");
											return true;
										}
									}
									catch (Exception checkEx)
									{
										Logger.LogVerbose($"Could not verify existing data in \"{tableName}\" for \"{SqlInstance}\": {checkEx.Message}");
									}
								}
								
								string message = $"Failed to merge table \"[{ SqlInstance }].{ tableName }\" after {maxRetries} retries";
								Logger.LogError(message, e.Errors[0].Message, sql);
								return false;
							}
							catch (SqlException e)
							{
								string message = $"Failed to merge table \"[{ SqlInstance }].{ tableName }\"";
								if (e.Errors[0].Message.Contains("Violation of PRIMARY KEY constraint") == true)
								{
									message += ". Perhaps you should try running a full load to try to resolve the issue.";
								}

								Logger.LogError(message, e.Errors[0].Message, sql);

								//dump # table to physical table to help debugging
								if (Config.dumpOnError == true)
								{
									try
									{
										string dumpTableName = $"[_DUMP_{ string.Format("{0:yyyyMMddHHmmssfff}", DateTime.Now) }_{SqlInstance.Replace("\\", "_").Replace(".", "_")}_{tableName.Replace("dbo.", "").Replace("[", "").Replace("]", "")}]";
										sql = $"select * into {dumpTableName} from {workingTableName}";
										using (SqlCommand cmdDumpData = new SqlCommand(sql, connectionRepository))
										{
											cmdDumpData.CommandTimeout = Config.BulkCopyTimeout;
											cmdDumpData.ExecuteNonQuery();
											Logger.LogVerbose($"Dumped staging data to {dumpTableName} for debugging");
										}
									}
									catch (SqlException x)
									{
										Logger.LogError("Failed to dump data into a table for debugging. This was not expected.", x.Errors[0].Message, sql);
									}
								}
								
								// Preserve staging table for debugging - don't drop it
								Logger.LogVerbose($"Preserving staging table {workingTableName} for debugging purposes");
								
								return false;
							}
						}
						
						// If we get here, all retries failed
						return false;
					}
					else
					{
						return true;
					}
				}
			}
		}

		private async Task<string> SnapshotTimeForInstance(SqlConnection connectionRepository)
		{
			// The nolock here is safe as nothing is modifying or writing data for specific instance but it does not block other threads modifying their own instances
			var sql = @"select 'case ' + char(10) + (
												select 'when [snapshot_type_id] = ' + convert(varchar(10),[snapshot_type_id]) + ' then ''' + convert(varchar(23),max([snapshot_time])) + '''' + char(10)
												from [dbo].[sqlwatch_logger_snapshot_header] with (nolock)
												where sql_instance = @SqlInstance
												group by [snapshot_type_id],[sql_instance]
												for xml path('')
											) + char(10) + ' else '''' end '";

			using (var cmd = new SqlCommand(sql, connectionRepository))
			{
				cmd.Parameters.AddWithValue("@SqlInstance", this.SqlInstance);
				var result = await cmd.ExecuteScalarAsync();
				return result.ToString();
			}
		}

		private string BuildJoinCondition(string primaryKeys, string leftAlias, string rightAlias)
		{
			if (string.IsNullOrEmpty(primaryKeys))
				return "1=1"; // Fallback condition
				
			var keyColumns = primaryKeys.Split(',').Select(k => k.Trim().Replace("[", "").Replace("]", "")).ToArray();
			var conditions = keyColumns.Select(col => $"{leftAlias}.[{col}] = {rightAlias}.[{col}]");
			return string.Join(" AND ", conditions);
		}

		public async Task<string> GetVersion()
		{
			string sql = "SELECT [sqlwatch_version] FROM [dbo].[vw_sqlwatch_app_version]";
			string version = "";

			using (SqlConnection connetion = new SqlConnection(this.ConnectionString))
			{
				await connetion.OpenAsync();
				using (SqlCommand cmdGetVersion = new SqlCommand(sql, connetion))
				{
					version = (await cmdGetVersion.ExecuteScalarAsync()).ToString();
				}
			}

			this.Vesion = version;
			return version;
		}
		public async Task<bool> IsOnline()
		{
			// Checks if the server is online

			Logger.LogVerbose($"Checking if Central Repository is online");
			try
			{
				using (SqlConnection conn = new SqlConnection(this.ConnectionString))
				{
					await conn.OpenAsync();
					return true;
				}
			}
			catch (SqlException e)
			{
				Logger.LogError($"Unable to open connection to {this.SqlInstance}", e.Errors[0].Message);
				return false;
			}
		}

		public class Table : SqlWatchInstance, IDisposable
		{
			public string TableName { get; set; }

		}

		#region CommandLine

		public bool AddRemoteInstance(string SqlInstance, string SqlDatabase, string Hostname = null, int? SqlPort = null, string SqlUser = null, string SqlPassword = null)
		{
			string SqlSecret = null;

			if (SqlPassword != null)
			{
				SqlSecret = Tools.Encrypt(SqlPassword);
			}

			if (SqlPort == 0)
			{
				SqlPort = null;
			}

			using (SqlConnection conn = new SqlConnection(this.ConnectionString))
			{
				string query = @"insert into [dbo].[sqlwatch_config_sql_instance]([sql_instance]
										,[hostname],[sql_port],[sqlwatch_database_name]
										,[repo_collector_is_active],[sql_user],[sql_secret])
						values(@sql_instance, @hostname, @port, @database, 1, @sql_user, @sql_secret);";

				using (SqlCommand command = new SqlCommand(query, conn))
				{
					command.Parameters.AddWithValue("@sql_instance", SqlInstance);
					command.Parameters.AddWithValue("@hostname", Hostname ?? (object)DBNull.Value);
					command.Parameters.AddWithValue("@port", SqlPort ?? (object)DBNull.Value);
					command.Parameters.AddWithValue("@database", SqlDatabase);
					command.Parameters.AddWithValue("@sql_user", SqlUser ?? (object)DBNull.Value);
					command.Parameters.AddWithValue("@sql_secret", SqlSecret ?? (object)DBNull.Value);

					conn.Open();

					try
					{
						command.ExecuteNonQuery();
						Console.WriteLine("OK");
						return true;
					}
					catch (SqlException e)
					{
						Console.WriteLine(e.Errors[0].Message);
						return false;
					}
				}
			}
		}

		public bool UpdateRemoteInstance(string SqlInstance, string SqlUser, string SqlPassword)
		{
			string SqlSecret = Tools.Encrypt(SqlPassword);
			using (SqlConnection conn = new SqlConnection(this.ConnectionString))
			{
				string query = @"update [dbo].[sqlwatch_config_sql_instance]
							set sql_secret = @sql_secret, 
							    sql_user = @sql_user
						where sql_instance = @sql_instance";

				using (SqlCommand command = new SqlCommand(query, conn))
				{
					command.Parameters.AddWithValue("@sql_instance", SqlInstance);
					command.Parameters.AddWithValue("@sql_user", SqlUser);
					command.Parameters.AddWithValue("@sql_secret", SqlSecret);

					conn.Open();

					try
					{
						command.ExecuteNonQuery();
						Console.WriteLine("OK");
						return true;
					}
					catch (SqlException e)
					{
						Console.WriteLine(e.Errors[0].Message);
						return false;
					}
				}
			}

		}

		#endregion

	}
}
