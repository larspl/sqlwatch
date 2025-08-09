#!/bin/bash

# SQLWATCH Build Release Script for macOS
# Converts the PowerShell build script to work on macOS with Mono and MSBuild

# Exit on any error
set -e

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "🚀 Starting SQLWATCH Release Build for macOS..."

# Create temp folder to store release files
echo "📁 Creating Release folder and copying all files for the release..."

TMP_FOLDER="$SCRIPT_DIR/RELEASE"
RELEASE_FOLDER_NAME="SQLWATCH Latest"
RELEASE_FOLDER="$TMP_FOLDER/$RELEASE_FOLDER_NAME"

# Remove existing release folder if it exists
if [ -d "$TMP_FOLDER" ]; then
    echo "🗑️  Removing existing release folder..."
    rm -rf "$TMP_FOLDER"
fi

# Create new release folder
mkdir -p "$RELEASE_FOLDER"

echo "📦 Restoring NuGet packages..."

# Check if nuget is available
if ! command -v nuget &> /dev/null; then
    echo "❌ Error: nuget command not found. Please install NuGet:"
    echo "   brew install nuget"
    exit 1
fi

# Restore NuGet packages
nuget restore "$SCRIPT_DIR/SqlWatch.Monitor/SqlWatch.Monitor.sln" -Verbosity quiet

if [ $? -ne 0 ]; then
    echo "❌ Error: NuGet restore failed"
    exit 1
fi

echo "🔨 Building SQLWATCH solution..."

# Check if msbuild is available (from Mono)
if ! command -v msbuild &> /dev/null; then
    echo "❌ Error: msbuild command not found. Please install Mono:"
    echo "   brew install mono"
    exit 1
fi

# Build the C# projects only (exclude SQL Server Database Project which can't build on macOS)
echo "🔨 Building C# projects with MSBuild..."
msbuild "$SCRIPT_DIR/SqlWatch.Monitor/Project.SqlWatchImport/SqlWatchImport.csproj" \
    /p:Configuration=Release \
    /p:Platform=AnyCPU \
    /p:OutputPath="$RELEASE_FOLDER/" \
    /verbosity:minimal \
    /nologo

if [ $? -ne 0 ]; then
    echo "❌ Error: MSBuild failed"
    exit 1
fi

echo "✅ C# build completed successfully"

# Note: SQL Server projects (.sqlproj) cannot be built on macOS as they require SQL Server Data Tools
# We'll copy any existing DACPAC files instead
echo "📋 Looking for existing DACPAC files..."

DACPAC_SOURCE="$SCRIPT_DIR/SqlWatch.Monitor/Project.SqlWatch.Database/bin/Release/SQLWATCH.dacpac"
if [ -f "$DACPAC_SOURCE" ]; then
    echo "📦 Copying existing DACPAC file..."
    cp "$DACPAC_SOURCE" "$RELEASE_FOLDER/"
else
    echo "⚠️  Warning: No DACPAC file found. SQL Server Database Project cannot be built on macOS."
    echo "   You may need to build the DACPAC on Windows and copy it manually."
fi

# Copy Dashboard files
echo "📊 Copying Dashboard files..."
if [ -d "$SCRIPT_DIR/SqlWatch.Dashboard" ]; then
    cp -R "$SCRIPT_DIR/SqlWatch.Dashboard" "$RELEASE_FOLDER/"
    
    # Remove .bak files (equivalent to PowerShell -Exclude *.bak)
    find "$RELEASE_FOLDER/SqlWatch.Dashboard" -name "*.bak" -type f -delete
else
    echo "⚠️  Warning: Dashboard folder not found"
fi

# Get SQLWATCH Version number from the dacpac (if it exists)
if [ -f "$RELEASE_FOLDER/SQLWATCH.dacpac" ]; then
    echo "🔍 Extracting version from DACPAC..."
    
    # Create temporary extraction folder
    DACPAC_EXTRACT_DIR="$RELEASE_FOLDER/SQLWATCH-DACPAC"
    
    # Copy dacpac as zip and extract
    cp "$RELEASE_FOLDER/SQLWATCH.dacpac" "$RELEASE_FOLDER/SQLWATCH.dacpac.zip"
    
    # Check if unzip is available
    if command -v unzip &> /dev/null; then
        unzip -q "$RELEASE_FOLDER/SQLWATCH.dacpac.zip" -d "$DACPAC_EXTRACT_DIR"
        
        # Extract version from DacMetadata.xml
        if [ -f "$DACPAC_EXTRACT_DIR/DacMetadata.xml" ]; then
            # Use grep and sed to extract version (cross-platform alternative to PowerShell XML parsing)
            VERSION=$(grep -o '<Version>[^<]*</Version>' "$DACPAC_EXTRACT_DIR/DacMetadata.xml" | sed 's/<[^>]*>//g' | tr -d '[:space:]')
            
            if [ ! -z "$VERSION" ]; then
                echo "📋 Found version: $VERSION"
                
                # Rename folder to include version number
                TIMESTAMP=$(date +"%Y%m%d%H%M%S")
                NEW_RELEASE_FOLDER_NAME="SQLWATCH $VERSION $TIMESTAMP"
                
                mv "$TMP_FOLDER/$RELEASE_FOLDER_NAME" "$TMP_FOLDER/$NEW_RELEASE_FOLDER_NAME"
                RELEASE_FOLDER_NAME="$NEW_RELEASE_FOLDER_NAME"
            else
                echo "⚠️  Warning: Could not extract version from DACPAC"
                TIMESTAMP=$(date +"%Y%m%d%H%M%S")
                NEW_RELEASE_FOLDER_NAME="SQLWATCH Latest $TIMESTAMP"
                mv "$TMP_FOLDER/$RELEASE_FOLDER_NAME" "$TMP_FOLDER/$NEW_RELEASE_FOLDER_NAME"
                RELEASE_FOLDER_NAME="$NEW_RELEASE_FOLDER_NAME"
            fi
        else
            echo "⚠️  Warning: DacMetadata.xml not found in DACPAC"
        fi
        
        # Clean up temporary files
        rm -rf "$DACPAC_EXTRACT_DIR"
        rm -f "$TMP_FOLDER/$RELEASE_FOLDER_NAME/SQLWATCH.dacpac.zip"
    else
        echo "⚠️  Warning: unzip command not found. Cannot extract version from DACPAC."
        TIMESTAMP=$(date +"%Y%m%d%H%M%S")
        NEW_RELEASE_FOLDER_NAME="SQLWATCH Latest $TIMESTAMP"
        mv "$TMP_FOLDER/$RELEASE_FOLDER_NAME" "$TMP_FOLDER/$NEW_RELEASE_FOLDER_NAME"
        RELEASE_FOLDER_NAME="$NEW_RELEASE_FOLDER_NAME"
    fi
else
    echo "⚠️  No DACPAC found, using timestamp for folder name"
    TIMESTAMP=$(date +"%Y%m%d%H%M%S")
    NEW_RELEASE_FOLDER_NAME="SQLWATCH Latest $TIMESTAMP"
    mv "$TMP_FOLDER/$RELEASE_FOLDER_NAME" "$TMP_FOLDER/$NEW_RELEASE_FOLDER_NAME"
    RELEASE_FOLDER_NAME="$NEW_RELEASE_FOLDER_NAME"
fi

# Create ZIP archive
echo "📦 Creating ZIP archive..."

if command -v zip &> /dev/null; then
    cd "$TMP_FOLDER"
    zip -r -q "$RELEASE_FOLDER_NAME.zip" "$RELEASE_FOLDER_NAME"
    
    if [ $? -eq 0 ]; then
        echo "✅ ZIP archive created: $TMP_FOLDER/$RELEASE_FOLDER_NAME.zip"
    else
        echo "❌ Error: Failed to create ZIP archive"
        exit 1
    fi
else
    echo "⚠️  Warning: zip command not found. Archive not created."
fi

# Display summary
echo ""
echo "🎉 Build completed successfully!"
echo "📁 Release folder: $TMP_FOLDER/$RELEASE_FOLDER_NAME"
if [ -f "$TMP_FOLDER/$RELEASE_FOLDER_NAME.zip" ]; then
    echo "📦 ZIP archive: $TMP_FOLDER/$RELEASE_FOLDER_NAME.zip"
fi

# List contents of release folder
echo ""
echo "📋 Release contents:"
ls -la "$TMP_FOLDER/$RELEASE_FOLDER_NAME/"

# Show file sizes
echo ""
echo "📊 File sizes:"
du -sh "$TMP_FOLDER/$RELEASE_FOLDER_NAME"/*

echo ""
echo "✅ SQLWATCH Release Build for macOS completed!"
