# Audio Duplicate Detection System

A production-ready system for generating and storing audio fingerprints using Chromaprint, designed to detect duplicates in large-scale audio libraries.

## 🎯 Production System

### Core Files

- **`audio_fingerprint_processor.py`** - Main production script for fingerprint processing
- **`snowflake_utils.py`** - Snowflake database connection utilities  
- **`requirements.txt`** - Python dependencies
- **`analysis/`** - Historical analysis and test scripts

### Features

✅ **Production-Ready Fingerprint Processing**
- Downloads audio files from S3 using API-obtained signed URLs
- Generates Chromaprint fingerprints using `fpcalc`
- Stores results in Snowflake `BI_PROD.AI_DATA.AUDIO_FINGERPRINT` table
- Download → Fingerprint → Store → Delete workflow (no disk space accumulation)
- Robust error handling and logging
- Batch processing with progress tracking

✅ **Scalable Architecture**
- Handles 100,000+ songs efficiently
- Memory and disk space optimized
- Resume functionality for interrupted jobs
- Configurable batch sizes

✅ **Database Integration**
- Connects to production Snowflake warehouse
- Automatic credential management via Google Cloud Secret Manager
- Stores asset_id, file_key, format, duration, fingerprint, and metadata

## 🚀 Usage

### Basic Usage
```bash
# Process specific assets
python audio_fingerprint_processor.py --asset-ids "12345,67890,11111"

# Process batch of unprocessed assets
python audio_fingerprint_processor.py --batch-size 100

# Resume interrupted processing
python audio_fingerprint_processor.py --batch-size 50 --resume

# View processing statistics
python audio_fingerprint_processor.py --stats
```

### Requirements

1. **Chromaprint Installation**:
   ```bash
   brew install chromaprint
   ```

2. **Python Dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

3. **Google Cloud Authentication** (for Snowflake credentials):
   ```bash
   gcloud auth application-default login
   ```

## 📊 Database Schema

The system stores fingerprints in `BI_PROD.AI_DATA.AUDIO_FINGERPRINT`:

```sql
CREATE TABLE AI_DATA.AUDIO_FINGERPRINT (
    ASSET_ID VARCHAR(50) NOT NULL,
    FILE_KEY VARCHAR(500) NOT NULL,
    FORMAT VARCHAR(10),
    DURATION FLOAT,
    FINGERPRINT TEXT,
    FILE_SIZE BIGINT,
    PROCESSING_STATUS VARCHAR(20) DEFAULT 'SUCCESS',
    ERROR_MESSAGE TEXT,
    CREATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (ASSET_ID, FILE_KEY)
);
```

## 🔍 Analysis Results

The `analysis/` folder contains historical research and testing:

- **Audio Modification Tests** - Chromaprint robustness against various audio changes
- **Format Comparisons** - Cross-format similarity analysis (WAV vs MP3)
- **Duration Analysis** - Clustering strategies for large-scale processing
- **Similarity Thresholds** - Established thresholds for duplicate classification:
  - `≥0.95`: Identical files (auto-delete candidates)
  - `0.80-0.95`: Same content, different format (manual review)
  - `0.60-0.80`: Related versions (flag as variants)
  - `<0.60`: Different songs

## 🏗️ Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Snowflake     │    │  Audio Files     │    │   Chromaprint   │
│   (Asset Data)  │───▶│  (S3 + API)      │───▶│  (Fingerprint)  │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                                                         │
┌─────────────────┐    ┌──────────────────┐             │
│   Snowflake     │◀───│  Fingerprint     │◀────────────┘
│  (Fingerprints) │    │   Processor      │
└─────────────────┘    └──────────────────┘
```

## 📈 Performance

- **Processing Rate**: ~0.4-0.5 assets/second
- **Memory Usage**: Minimal (one file at a time)
- **Disk Usage**: Zero accumulation (immediate cleanup)
- **Scalability**: Tested with 100,000+ song datasets

## 🛠️ Development

The system was developed through extensive testing and analysis:

1. **Chromaprint Integration** - Resolved library loading and environment issues
2. **API Integration** - Implemented Artlist/MotionArray download APIs  
3. **Database Design** - Optimized schema for fingerprint storage
4. **Error Handling** - Robust processing with comprehensive logging
5. **Performance Optimization** - Memory and disk space efficient processing

---

**Status**: ✅ Production Ready - Fully functional fingerprint processing system