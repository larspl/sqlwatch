# SQLWATCH macOS Build Instructions

## Prerequisites

Before running the build script, ensure you have the required tools installed:

### 1. Install Homebrew (if not already installed)
```bash
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
```

### 2. Install Required Tools
```bash
# Install Mono (includes msbuild)
brew install mono

# Install NuGet
brew install nuget

# Verify installations
msbuild --version
nuget help
```

## Usage

### Quick Build
```bash
# Navigate to the SQLWATCH root directory
cd /Users/larspl/source/github/larspl/sqlwatch

# Run the build script
./SQLWATCH-Build-Release.sh
```

### Manual Build Steps (Alternative)
If you prefer to build manually:

```bash
# 1. Restore NuGet packages
nuget restore SqlWatch.Monitor/SqlWatch.Monitor.sln

# 2. Build the C# solution
msbuild SqlWatch.Monitor/SqlWatch.Monitor.sln /p:Configuration=Release

# 3. Check output
ls -la SqlWatch.Monitor/Project.SqlWatchImport/bin/Release/
```

## What Gets Built

✅ **C# Projects (SqlWatchImport.exe)**
- All new queue-based architecture features
- Smart load strategy implementation  
- SQLWATCH T-SQL architecture adaptations
- Enhanced configuration and logging

⚠️ **SQL Server Database Project (DACPAC)**
- Cannot be built on macOS (requires SQL Server Data Tools)
- Script will copy existing DACPAC if available
- For new DACPAC, build on Windows or use existing one

✅ **Dashboard Files**
- Grafana dashboards
- Power BI reports
- Azure workbooks

## Output Structure

The script creates:
```
RELEASE/
└── SQLWATCH [Version] [Timestamp]/
    ├── SqlWatchImport.exe          # Enhanced C# import application
    ├── SqlWatchImport.exe.config   # Configuration with new features
    ├── CommandLine.dll             # Dependencies
    ├── SQLWATCH.dacpac            # Database project (if available)
    └── SqlWatch.Dashboard/         # Dashboard files
        ├── Grafana/
        ├── Power BI/
        └── Azure/
```

## New Features Included

The built `SqlWatchImport.exe` includes all the enhancements we implemented:

### 🎯 Queue-Based Processing
```xml
<add key="UseQueueBasedProcessing" value="true" />
```

### 🧠 Smart Load Strategy  
```xml
<add key="UseSmartLoadStrategy" value="true" />
<add key="MetaTablesFullLoadOnly" value="true" />
<add key="LoggerTablesDeltaLoad" value="true" />
```

### 📊 Dependency Management
```xml
<add key="UseDependencyBasedOrdering" value="true" />
<add key="MetaTablePriority" value="1" />
<add key="LoggerTablePriority" value="2" />
```

### ⚡ Performance Optimizations
- Adaptive timeout handling for large datasets
- Staging approach for meta tables
- Advanced retry logic with constraint violation handling
- Emergency full load fallback mechanisms

## Troubleshooting

### Build Errors
- **"nuget command not found"**: Install NuGet with `brew install nuget`
- **"msbuild command not found"**: Install Mono with `brew install mono`
- **Permission denied**: Make script executable with `chmod +x SQLWATCH-Build-Release.sh`

### Missing DACPAC
- The SQL Server Database Project requires Windows to build
- Copy an existing DACPAC from a Windows build
- Or use the DACPAC deployment separately

### Large File Warnings
- The enhanced C# application may be larger due to new features
- This is expected and includes all queue-based processing capabilities

## Integration with Existing Workflow

The macOS build integrates with your existing CI/CD:

1. **Development**: Use this script for local macOS builds
2. **Testing**: Built application includes all new features for testing
3. **Production**: Use Windows build for final DACPAC, macOS for C# application

## Next Steps

After building:
1. Test the enhanced import application with new configuration options
2. Deploy the C# application to your import servers
3. Configure queue-based processing as needed
4. Monitor performance improvements with large datasets

The built application is production-ready with all SQLWATCH T-SQL architecture adaptations!
