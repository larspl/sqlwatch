# SQLWatch Grafana Dashboard Linked Server Modifications

## Overview
This document describes the modifications made to SQLWatch Grafana dashboards to enable them to work with linked servers in a central repository configuration. The changes allow the dashboards to dynamically route queries through the appropriate linked servers based on the selected SQL instance.

## Problem Statement
The original SQLWatch Grafana dashboards were designed to query data directly from local tables using the `sql_instance` parameter. However, in a central repository setup where multiple SQL Server instances are monitored from a central location, the data needs to be queried through linked servers.

## Solution Architecture

### Core Principle
Instead of querying tables directly like:
```sql
FROM [dbo].[table_name] WHERE sql_instance = '$sql_instance'
```

The modified queries now use dynamic SQL to route through linked servers:
```sql
DECLARE @linked_server_name NVARCHAR(255), @database_name NVARCHAR(128)
SELECT @linked_server_name = ISNULL(linked_server_name, sql_instance),
       @database_name = sqlwatch_database_name
FROM [dbo].[sqlwatch_config_sql_instance] 
WHERE sql_instance = '$sql_instance'

DECLARE @sql NVARCHAR(MAX) = '
SELECT ... 
FROM [' + @linked_server_name + '].[' + @database_name + '].[dbo].[table_name]
WHERE sql_instance = ''$sql_instance'''

EXEC sp_executesql @sql
```

### Key Components

#### 1. Instance Configuration Lookup
Each query starts by looking up the linked server configuration:
- `linked_server_name`: The name of the linked server (e.g., 'SQLWATCH-REMOTE-ServerName')
- `sqlwatch_database_name`: The name of the SQLWatch database on the remote server
- If no linked server is configured, it falls back to the instance name (for local connections)

#### 2. Dynamic SQL Construction
The actual query is built dynamically using the linked server name and database name from the configuration.

#### 3. Parameterized Execution
The dynamic SQL is executed using `sp_executesql` to maintain security and performance.

## Modified Dashboards

### 1. SQL Instance Overview.json
**Location:** `/Users/larspl/source/github/larspl/sqlwatch/SqlWatch.Dashboard/Grafana/SQL Instance Overview.json`

**Modified Queries:**
- Pending tasks query (scheduler statistics)
- Blocked sessions query (blocking chains)
- Memory usage query (process memory)
- Performance counters query (CPU, batch requests, etc.)
- Wait statistics query
- Sessions statistics query
- User requests query
- File statistics query (disk latency)
- Checks status query
- Check details query
- Disk space usage query

### 2. Long Queries.json
**Location:** `/Users/larspl/source/github/larspl/sqlwatch/SqlWatch.Dashboard/Grafana/Long Queries.json`

**Modified Queries:**
- Long queries count
- Long queries details
- Specific long query details

### 3. Wait Events.json
**Location:** `/Users/larspl/source/github/larspl/sqlwatch/SqlWatch.Dashboard/Grafana/Wait Events.json`

**Modified Queries:**
- Wait events aggregation
- Wait events details

### 4. Repository Dashboard.json
**Location:** `/Users/larspl/source/github/larspl/sqlwatch/SqlWatch.Dashboard/Grafana/Repository Dashboard.json`

**Modified Queries:**
- Long processes query (uses cursor to handle multiple instances)

## Usage Instructions

### Prerequisites
1. Ensure linked servers are properly configured using the SQLWatch stored procedure:
   ```sql
   EXEC [dbo].[usp_sqlwatch_config_repository_create_linked_server] 
       @sql_instance = 'YourInstanceName',
       @rmtuser = 'YourUsername', 
       @rmtpassword = 'YourPassword'
   ```

2. Verify the `sqlwatch_config_sql_instance` table contains correct configuration:
   ```sql
   SELECT sql_instance, hostname, linked_server_name, sqlwatch_database_name 
   FROM [dbo].[sqlwatch_config_sql_instance]
   ```

### Dashboard Variable Configuration
The dashboard variable `sql_instance` should continue to use:
```sql
SELECT [servername] FROM [dbo].[sqlwatch_meta_server]
```

This provides the list of available instances for selection.

### Testing
1. Select an instance from the dropdown in Grafana
2. Verify that data appears for the selected instance
3. Check Grafana query inspector or SQL Server Profiler to confirm queries are being routed through the correct linked server

## Troubleshooting

### Common Issues

#### 1. "Invalid object name" errors
- **Cause:** Linked server not properly configured or incorrect database name
- **Solution:** Verify linked server exists and `sqlwatch_database_name` is correct in config table

#### 2. "Login failed" errors
- **Cause:** Authentication issues with linked server
- **Solution:** Check linked server login mapping using `sp_helplinkedsrvlogin`

#### 3. Performance issues
- **Cause:** Linked server queries can be slower than local queries
- **Solution:** Consider query optimization and indexing on remote servers

#### 4. Dynamic SQL escaping issues
- **Cause:** Single quotes in filter values not properly escaped
- **Solution:** The pattern uses double single quotes ('') for escaping

### Debugging Queries
To debug a specific query, you can run the dynamic SQL generation portion separately:
```sql
DECLARE @linked_server_name NVARCHAR(255), @database_name NVARCHAR(128)
SELECT @linked_server_name = ISNULL(linked_server_name, sql_instance),
       @database_name = sqlwatch_database_name
FROM [dbo].[sqlwatch_config_sql_instance] 
WHERE sql_instance = 'YourInstanceName'

PRINT 'Linked Server: ' + @linked_server_name
PRINT 'Database: ' + @database_name
```

## Future Modifications

### Adding New Queries
When adding new queries that need to work with linked servers, follow this pattern:

1. **Start with configuration lookup:**
   ```sql
   DECLARE @linked_server_name NVARCHAR(255), @database_name NVARCHAR(128)
   SELECT @linked_server_name = ISNULL(linked_server_name, sql_instance),
          @database_name = sqlwatch_database_name
   FROM [dbo].[sqlwatch_config_sql_instance] 
   WHERE sql_instance = '$sql_instance'
   ```

2. **Build dynamic SQL with proper escaping:**
   ```sql
   DECLARE @sql NVARCHAR(MAX) = '
   SELECT ... 
   FROM [' + @linked_server_name + '].[' + @database_name + '].[dbo].[table_name]
   WHERE sql_instance = ''$sql_instance'''
   ```

3. **Execute the dynamic SQL:**
   ```sql
   EXEC sp_executesql @sql
   ```

### Multi-Instance Queries
For queries that need to aggregate data from multiple instances (like the Repository Dashboard), use a cursor approach:

```sql
DECLARE @sql NVARCHAR(MAX) = ''
DECLARE @instance VARCHAR(32), @linked_server_name NVARCHAR(255), @database_name NVARCHAR(128)

DECLARE instance_cursor CURSOR FOR
SELECT DISTINCT sql_instance, ISNULL(linked_server_name, sql_instance), sqlwatch_database_name
FROM [dbo].[sqlwatch_config_sql_instance] 
WHERE sql_instance IN ($sql_instance)

-- Build UNION ALL query for each instance
-- Execute final query
```

## Security Considerations

1. **SQL Injection Protection:** Dynamic SQL uses proper escaping with double single quotes
2. **Linked Server Security:** Ensure linked servers use appropriate authentication methods
3. **Least Privilege:** Grant only necessary permissions to linked server logins
4. **Connection Encryption:** Configure linked servers to use encrypted connections when possible

## Performance Considerations

1. **Query Optimization:** Linked server queries may perform differently than local queries
2. **Indexing:** Ensure remote databases have appropriate indexes
3. **Connection Pooling:** Consider connection pooling settings for linked servers
4. **Query Timeouts:** Monitor and adjust query timeout settings if needed

## Maintenance

### Regular Tasks
1. **Monitor linked server connectivity:** Check `sys.servers` and test connections
2. **Update configurations:** Keep `sqlwatch_config_sql_instance` table updated
3. **Review performance:** Monitor query execution times and optimize as needed
4. **Security audits:** Regularly review linked server permissions and configurations

### Version Control
When making changes to dashboard files:
1. Test changes thoroughly in a development environment
2. Document modifications in this file
3. Consider backing up original dashboard configurations
4. Use version control for tracking changes

## Contact and Support
For questions or issues related to these modifications, refer to the SQLWatch documentation or community forums.
