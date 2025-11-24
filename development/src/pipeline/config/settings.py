import os
from pathlib import Path
import sys

# Add parent directory to path to ensure _version can be imported
# This handles cases where the script is run from different locations
sys.path.append(str(Path(__file__).parent.parent))

from _version import __version__

AL_ENV = os.environ.get("AL_ENV", "dev")

# Project configuration
PROJECT_NAME = "detect-duplicates"
PRODUCT = "artlist"
VERSION = __version__.replace(".", "-")

# GCP Configuration
PROJECT_ID = f"artlist-ai-{AL_ENV}"
LOCATION = 'europe-west4'

# Service Account
SERVICE_ACCOUNT_NAME = f"kf-{PROJECT_NAME}-{AL_ENV}"
SERVICE_ACCOUNT = f"{SERVICE_ACCOUNT_NAME}@{PROJECT_ID}.iam.gserviceaccount.com"

# Pipeline Name
PIPELINE_NAME = f'{PRODUCT}--{PROJECT_NAME}--{AL_ENV}--v{VERSION}'

# Buckets
BUCKET_URI = f'gs://{PRODUCT}--{PROJECT_NAME}--{AL_ENV}'
PIPELINE_ROOT = f'{BUCKET_URI}/pipelines/{PIPELINE_NAME}'
ASSETS_URI = f'{BUCKET_URI}/assets/{PIPELINE_NAME}'

# Paths
PIPELINE_DIR = Path(__file__).parent.parent
ASSETS_DIR = PIPELINE_DIR.parent / 'assets'

if __name__ == '__main__':
    # Export all uppercase string variables as environment variables
    vars = locals().copy()
    for k, v in vars.items():
        if k.isupper() and isinstance(v, str):
            print(f'export {k}="{v}"')
