# Artlist & MotionArray Bulk Downloader

A production-ready Python script for bulk downloading music files from both Artlist and MotionArray platforms using keys obtained from Snowflake queries.

## Features

- **Dual Platform Support**: Download from both Artlist and MotionArray
- **Snowflake Integration**: Direct connection to Snowflake data warehouse
- **Optimized Queries**: One file per format per asset (WAV + MP3 for Artlist)
- **Bulk API Requests**: Efficient batch downloading
- **Smart File Organization**: Separate directories per platform
- **Robust Error Handling**: Comprehensive logging and error management
- **Production Ready**: Scalable for thousands of songs

## Installation

1. Clone or download this repository
2. Install dependencies:
```bash
pip install -r requirements.txt
```

## Usage

### Basic Usage

```python
from bulk_downloader import BulkDownloader

# Initialize downloader
downloader = BulkDownloader(download_dir="downloads")

# Download from both platforms (100 songs each)
results = downloader.download_both_platforms(
    artlist_limit=100,
    motionarray_limit=100
)

print(f"Total downloads: {results['summary']['total_downloads']}")
print(f"Artlist: {results['summary']['artlist_downloads']} files")
print(f"MotionArray: {results['summary']['motionarray_downloads']} files")
```

### Platform-Specific Downloads

```python
# Download only from Artlist
artlist_result = downloader.download_artlist_songs(limit=50)

# Download only from MotionArray  
motionarray_result = downloader.download_motionarray_songs(limit=50)
```

### Command Line Usage

Run the main script directly:
```bash
python bulk_downloader.py
```

## Authentication

The script requires Snowflake credentials configured via Google Cloud Secret Manager:

- **Primary**: Google Cloud Secret Manager (`ai_team_snowflake_credentials`)
- **Database**: `BI_PROD`
- **Schema**: `AI_DATA`

Ensure you have proper access to both the Snowflake database and the Artlist/MotionArray APIs.

## API Details

Both platforms use the same bulk download API:
- **Endpoint**: `https://oapi-int.artlist.io/v1/content/bulkDownloadArtifacts`
- **Method**: POST
- **Headers**: 
  - `service-host: core.content.cms.api`
  - `Content-Type: application/json`
- **Payload**: JSON object with `keys` array

## Query Optimization

The script uses optimized Snowflake queries that return only one file per format per asset:

- **Artlist**: WAV (CORE) + MP3 formats only
- **MotionArray**: WAV, MP3, or AIFF (whatever is available)
- **Deduplication**: `ROW_NUMBER()` ensures latest file per format
- **Efficiency**: ~2-3 files per asset instead of 20-40

## Configuration

### Download Directory
By default, files are downloaded to a `downloads` directory. You can specify a custom directory:

```python
downloader = BulkDownloader(download_dir="/path/to/custom/directory")
```

### Logging
The script uses Python's logging module. Logs include:
- API request/response information
- Download progress
- Error details

## Error Handling

The script handles various error conditions:
- Network connectivity issues
- API authentication/authorization errors
- File system errors
- Database connectivity issues
- Invalid response formats

## File Structure

```
├── bulk_downloader.py      # Main production script
├── requirements.txt        # Python dependencies  
├── setup.py               # Installation helper
├── README.md              # This file
└── downloads/             # Download directory (created automatically)
    ├── artlist/           # Artlist music files
    └── motionarray/       # MotionArray music files
```

## Download Results

Each download session provides detailed results:

```python
{
  "summary": {
    "overall_success": true,
    "total_downloads": 150,
    "artlist_downloads": 100,
    "motionarray_downloads": 50
  },
  "artlist": {
    "success": true,
    "snowflake_assets": 50,
    "extracted_keys": 100,
    "downloads_successful": 100
  },
  "motionarray": {
    "success": true, 
    "snowflake_assets": 50,
    "extracted_keys": 50,
    "downloads_successful": 50
  }
}
```

## Troubleshooting

### Common Issues

1. **Snowflake Authentication**: Ensure Google Cloud credentials are properly configured
2. **API Access**: Verify access to Artlist/MotionArray bulk download APIs
3. **Network Issues**: Check internet connection and firewall settings
4. **File Permissions**: Ensure write permissions for the download directory

### Debug Mode

Enable debug logging by modifying the logging level:

```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

## Performance

The optimized queries provide excellent performance:

- **5 songs**: ~15 files downloaded in ~2 minutes
- **50 songs**: ~150 files downloaded in ~20 minutes  
- **100 songs**: ~300 files downloaded in ~40 minutes

File sizes typically range from 2-50 MB per file depending on format and length.

## Production Deployment

For production use:

1. Set up proper Snowflake credentials via Google Cloud Secret Manager
2. Configure appropriate download directories with sufficient storage
3. Set up logging and monitoring for download jobs
4. Consider rate limiting for very large batch downloads
5. Implement retry logic for failed downloads if needed
