# SQLWATCH Import Hybrid Approach - Configuration Guide

## Overview

The Hybrid Approach is an advanced multi-threading system designed to handle high-concurrency imports from multiple SQL Server instances (50-100+) into a central SQLWATCH repository. It provides fine-grained control over database load, reduces lock contention, and optimizes performance for different table types.

## Key Features

- **Global Concurrency Control**: Prevents overwhelming the central database
- **Table-Type Optimization**: Different strategies for logger vs meta tables
- **Staging Approach**: Reduces lock contention for upsert operations
- **Load Distribution**: Staggers operations to smooth peak loads
- **Intelligent Throttling**: Adapts concurrency based on table characteristics

## Configuration Parameters

### Basic Threading (Existing)
```xml
<!-- Traditional thread pool settings -->
<add key="MinThreads" value="30" />          <!-- Fixed thread count recommended for 100+ instances -->
<add key="MaxThreads" value="50" />          <!-- Upper limit to prevent resource exhaustion -->
<add key="MinPoolSize" value="10" />         <!-- Connection pool minimum -->
<add key="MaxPoolSize" value="100" />        <!-- Connection pool maximum -->
```

### Hybrid Approach Settings (New)
```xml
<!-- Global database protection -->
<add key="GlobalConcurrencyLimit" value="20" />     <!-- Max concurrent operations on central DB -->

<!-- Table-specific concurrency control -->
<add key="LoggerTableConcurrency" value="6" />      <!-- Logger tables (append-only, higher concurrency) -->
<add key="MetaTableConcurrency" value="2" />        <!-- Meta tables (upserts, lower concurrency) -->
<add key="DefaultTableConcurrency" value="3" />     <!-- Other tables -->

<!-- Advanced optimizations -->
<add key="UseStaginForMetaTables" value="true" />   <!-- Enable staging for meta tables -->
<add key="MergeBatchSize" value="5000" />           <!-- Batch size for staged merges -->

<!-- Load distribution -->
<add key="StaggerDelayMinMs" value="100" />         <!-- Minimum stagger delay -->
<add key="StaggerDelayMaxMs" value="1000" />        <!-- Maximum stagger delay -->
```

## Configuration Scenarios

### Small Environment (1-10 instances)
```xml
<add key="GlobalConcurrencyLimit" value="10" />
<add key="LoggerTableConcurrency" value="4" />
<add key="MetaTableConcurrency" value="2" />
<add key="UseStaginForMetaTables" value="false" />
<add key="StaggerDelayMinMs" value="0" />
<add key="StaggerDelayMaxMs" value="0" />
```

### Medium Environment (10-50 instances)
```xml
<add key="GlobalConcurrencyLimit" value="15" />
<add key="LoggerTableConcurrency" value="5" />
<add key="MetaTableConcurrency" value="2" />
<add key="UseStaginForMetaTables" value="true" />
<add key="StaggerDelayMinMs" value="50" />
<add key="StaggerDelayMaxMs" value="500" />
```

### Large Environment (50-100+ instances)
```xml
<add key="GlobalConcurrencyLimit" value="20" />
<add key="LoggerTableConcurrency" value="6" />
<add key="MetaTableConcurrency" value="2" />
<add key="UseStaginForMetaTables" value="true" />
<add key="StaggerDelayMinMs" value="100" />
<add key="StaggerDelayMaxMs" value="2000" />
```

### High-Performance Environment (Powerful hardware)
```xml
<add key="GlobalConcurrencyLimit" value="30" />
<add key="LoggerTableConcurrency" value="8" />
<add key="MetaTableConcurrency" value="4" />
<add key="UseStaginForMetaTables" value="true" />
<add key="StaggerDelayMinMs" value="50" />
<add key="StaggerDelayMaxMs" value="500" />
```

## How It Works

### 1. Global Throttling
- Limits total concurrent operations to prevent database overwhelming
- Uses `SemaphoreSlim` for efficient async coordination
- Configurable via `GlobalConcurrencyLimit`

### 2. Table-Type Intelligence
The system automatically categorizes tables:

**Logger Tables** (`sqlwatch_logger_*`):
- Append-only operations
- Higher concurrency allowed (6 concurrent by default)
- Direct bulk insert approach
- Minimal lock contention

**Meta Tables** (`sqlwatch_meta_*`, `sqlwatch_config_*`):
- Require upsert operations
- Lower concurrency (2 concurrent by default)
- Optional staging approach for large environments
- Enhanced lock management

### 3. Staging Approach
When `UseStaginForMetaTables=true`:
1. Data is bulk-loaded into unique staging tables per session
2. Staging tables use session-specific names to avoid conflicts
3. Coordinated merge operations minimize lock duration
4. Automatic cleanup of staging resources

### 4. Load Distribution
- Random stagger delays spread operation starts
- Prevents all instances hitting the database simultaneously
- Configurable delay ranges for different environments

## Monitoring and Troubleshooting

### Log Messages
The hybrid approach provides enhanced logging:

```
Using Hybrid Import Approach with Global Concurrency Limit: 20
Table Concurrency - Logger: 6, Meta: 2, Default: 3
Starting controlled import: "SERVER01.dbo.sqlwatch_logger_perf_os_performance_counters" 
(Global: 15/20, Table: 4/6)
Staggered import for "SERVER01.dbo.sqlwatch_meta_database" by 750ms
Merged 1247 rows from staging table for "SERVER01" in 45ms using staging approach
```

### Performance Metrics
Monitor these indicators:
- Global concurrency usage (should stay below limit)
- Table-specific concurrency patterns
- Merge operation duration
- Lock wait statistics on central database

### Tuning Guidelines

**If you see frequent lock waits:**
- Reduce `MetaTableConcurrency`
- Increase stagger delays
- Enable staging for meta tables

**If import is too slow:**
- Increase `GlobalConcurrencyLimit` (monitor database performance)
- Increase `LoggerTableConcurrency`
- Reduce stagger delays

**If getting connection pool exhaustion:**
- Increase `MaxPoolSize`
- Reduce concurrency limits proportionally

## Environment-Specific Considerations

### SQL Server Version
- SQL Server 2008 R2: Use lower concurrency settings
- SQL Server 2012+: Full feature support
- Always On environments: Monitor replica lag

### Hardware Resources

**Database Server CPU:**
- 4-8 cores: GlobalConcurrencyLimit ≤ 15
- 8-16 cores: GlobalConcurrencyLimit ≤ 25  
- 16+ cores: GlobalConcurrencyLimit ≤ 35

**Database Server Memory:**
- Consider connection pool sizes
- Monitor buffer pool pressure during imports

**Storage Performance:**
- SSD storage: Higher concurrency possible
- Traditional disks: More conservative settings

## Backwards Compatibility

The hybrid approach is fully backwards compatible:
- If new config values are missing, system falls back to original behavior
- Existing `MinThreads=-1` auto-scaling is respected but capped by `GlobalConcurrencyLimit`
- All existing configuration parameters continue to work

## Best Practices

1. **Start Conservative**: Begin with recommended settings and tune based on performance
2. **Monitor Database**: Watch for lock waits, CPU usage, and connection counts
3. **Test in Non-Production**: Validate settings before deploying to production
4. **Document Changes**: Keep track of configuration modifications and their impact
5. **Regular Review**: Periodically review settings as your environment grows

## Example Complete Configuration

```xml
<!-- Hybrid Approach Configuration for 100 SQL Server Instances -->
<appSettings>
  <!-- Central Repository -->
  <add key="CentralRepositorySqlInstance" value="SQLWATCH-REPO-1" />
  <add key="CentralRepositorySqlDatabase" value="SQLWATCH" />
  
  <!-- Traditional Threading -->
  <add key="MinThreads" value="30" />
  <add key="MaxThreads" value="50" />
  <add key="MinPoolSize" value="10" />
  <add key="MaxPoolSize" value="150" />
  
  <!-- Hybrid Approach -->
  <add key="GlobalConcurrencyLimit" value="20" />
  <add key="LoggerTableConcurrency" value="6" />
  <add key="MetaTableConcurrency" value="2" />
  <add key="DefaultTableConcurrency" value="3" />
  <add key="UseStaginForMetaTables" value="true" />
  <add key="MergeBatchSize" value="5000" />
  <add key="StaggerDelayMinMs" value="100" />
  <add key="StaggerDelayMaxMs" value="1000" />
  
  <!-- Bulk Copy Settings -->
  <add key="SqlBulkCopy.EnableStreaming" value="true"/>
  <add key="SqlBulkCopy.BatchSize" value="4000"/>
  <add key="SqlBulkCopy.BulkCopyTimeout" value="300"/>
</appSettings>
```

## Migration from Original Approach

1. **Backup Configuration**: Save current `App.config`
2. **Add New Settings**: Include hybrid approach parameters
3. **Start Conservative**: Use recommended settings for your environment size
4. **Test Import**: Run with subset of instances first
5. **Monitor Performance**: Watch database and application metrics
6. **Tune Settings**: Adjust based on observed performance
7. **Document Results**: Record optimal settings for future reference
