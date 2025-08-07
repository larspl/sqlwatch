# SQLWATCH AI Coding Agent Instructions

## Architecture Overview
SQLWATCH is a decentralized SQL Server monitoring solution with two main components:
- **Database Project** (`SqlWatch.Monitor/Project.SqlWatch.Database/`): SQL Server Database Project (DACPAC) containing all monitoring logic, stored procedures, tables, and SQL Agent jobs
- **Import Application** (`SqlWatch.Monitor/Project.SqlWatchImport/`): C# console application for centralized data collection from multiple remote instances

### Core Design Principles
- **Decentralized Architecture**: Each SQL Server instance monitors itself locally via SQL Agent jobs
- **5-second Granularity**: High-frequency data collection to capture workload spikes
- **Service Broker Activation**: Uses SQL Server Service Broker for efficient, event-driven data collection
- **Minimal Performance Impact**: Designed for ~1% overhead on single-core instances
- **Self-Maintaining**: Automated retention, purging, and maintenance workflows

## Project Structure

### Database Component (`Project.SqlWatch.Database/`)
- **Tables**: Split into `meta_*` (metadata), `logger_*` (time-series data), `config_*` (configuration)
- **Procedures**: `usp_sqlwatch_logger_*` for data collection, `usp_sqlwatch_internal_*` for framework logic
- **Extended Events**: Custom XE sessions in `dbo/Extended Events/` for query monitoring
- **Post-Deployment Scripts**: Reference data and configuration in `Scripts/Post-Deployment/`

### Import Application (`Project.SqlWatchImport/`)
- **Config.cs**: Uses `ConfigurationManager.AppSettings` for all configuration via `App.config`
- **SqlWatchInstance.cs**: Core class handling SQL connections, bulk copy operations, and parallel processing
- **Program.cs**: Entry point supporting both batch mode (no args) and interactive CLI mode

## Development Workflows

### Building the Solution
```bash
# Restore packages and build (requires Visual Studio 2019+ for C# project)
nuget restore SqlWatch.Monitor/SqlWatch.Monitor.sln
MSBuild.exe SqlWatch.Monitor/SqlWatch.Monitor.sln /p:Configuration=Release
```

### Database Deployment
```bash
# Deploy using SqlPackage.exe
SqlPackage.exe /Action:Publish /SourceFile:SQLWATCH.dacpac /TargetDatabaseName:SQLWATCH /TargetServerName:YOURSQLSERVER
```

### Testing Framework
- **Pester Tests**: PowerShell-based tests in `SqlWatch.Test/` directory
- **Test Runner**: `Run-Tests.p5.ps1` with parallel job execution support
- **Test Categories**: Use `-IncludeTags` and `-ExcludeTags` for selective test execution

## Configuration Patterns

### Database Configuration
- All configuration stored in `sqlwatch_config_*` tables
- Performance counters defined in `sqlwatch_config_performance_counters`
- Remote instances configured in `sqlwatch_config_sql_instance`

### Import Application Configuration (`App.config`)
- **Connection Strings**: Support both Windows Authentication and SQL Authentication with encryption
- **Threading**: `MinThreads=-1` for auto-scaling, `MaxThreads=0` for automatic management
- **Bulk Copy**: `SqlBulkCopy.EnableStreaming=true` for performance, configurable batch sizes
- **Logging**: Dual output to console and file with rotation support

## Key Implementation Details

### SQL Server Service Broker Integration
- Uses activated procedures (`usp_sqlwatch_internal_exec_activated`) for efficient data collection
- Queue-based processing with automatic scaling based on workload
- Broker diagnostics available via `usp_sqlwatch_internal_broker_diagnostics`

### Data Import Architecture
```csharp
// Parallel processing pattern used throughout SqlWatchImport
Parallel.ForEach(RemoteInstances, RemoteInstance => {
    Task.Run(async () => {
        await SqlWatchRemote.ImportAsync(SqlWatchTables);
    });
});
```

### Version Compatibility
- Repository and remote instances must have matching SQLWATCH versions
- Version stored in DACPAC metadata and checked during import operations

## SQL Server Compatibility
- Supports SQL Server 2008 R2 SP3 through 2019
- Some limitations on 2008 R2 (documented in README)
- Docker/Linux support with WMI disk utilization limitations

## Performance Considerations
- Uses Read Committed Snapshot Isolation for minimal blocking
- Bulk copy operations with streaming for large data sets
- Automatic thread pool management based on instance/table count
- Connection pooling with configurable min/max pool sizes

## Deployment and Distribution
- Primary distribution via DACPAC through dbatools: `Install-DbaSqlWatch`
- Release process automated via `SQLWATCH-Build-Release.ps1`
- AppVeyor CI/CD with multi-version SQL Server testing
- Grafana dashboards and Power BI reports in `SqlWatch.Dashboard/`

## Troubleshooting Patterns
- **Error Handling**: `Config.hasErrors` flag for exit code management
- **Data Dump**: `DumpDataOnError=true` creates debugging tables on failures
- **Logging**: Dual console/file output with configurable verbosity levels
- **Connection Issues**: Timeout configurations for repository vs. remote connections
