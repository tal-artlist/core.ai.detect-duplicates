#!/bin/bash

set -e
set -o pipefail

# Define variables directly without importing from settings.py
if [ -z "$AL_ENV" ]; then
    echo "Error: AL_ENV environment variable is not set"
    exit 1
fi

# Project references
if [ "$AL_ENV" = "dev" ]; then
    PROJECT_REFERENCE="889375371783"
elif [ "$AL_ENV" = "prd" ]; then
    PROJECT_REFERENCE="845261183607"
else
    echo "Error: AL_ENV must be 'dev' or 'prd'"
    exit 1
fi

# Core project settings
PROJECT_ID="artlist-ai-${AL_ENV}"
AIENV="$AL_ENV"
LOCATION="europe-west4"
PROJECT_NAME="detect-duplicates"
PRODUCT="artlist"

# Get version from _version.py
VERSION=$(python3 -c "import sys, os; sys.path.append(os.getcwd()); from _version import __version__; print(__version__.replace('.', '-'))")

# Service account settings
SERVICE_ACCOUNT_NAME="kf-${PROJECT_NAME}-${AL_ENV}"
SERVICE_ACCOUNT="${SERVICE_ACCOUNT_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"

# Bucket settings
BUCKET_URI="gs://${PRODUCT}--${PROJECT_NAME}--${AL_ENV}"

# Load the environment variables from the settings.py file
python3 config/settings.py > .env.tmp
source .env.tmp
rm .env.tmp

AL_ENV_UPPER=$(echo $AL_ENV | tr '[:lower:]' '[:upper:]')
CSA_USER=CSA-OAPI-$AL_ENV_UPPER-USER
CSA_KEY=CSA-OAPI-$AL_ENV_UPPER-KEY
SNOWFLAKE_USER=snowflake-ai_team_artlist

echo Create the following service account, bucket and associated permissions in the \"$AL_ENV\" environment.
echo Service account: $SERVICE_ACCOUNT
echo Bucket: $BUCKET_URI
echo "Continue? [y/n]"
read answer

if [[ ! $answer =~ ^[yY]$ ]]; then
    echo "Exiting..."
    exit 0
fi

echo Setting project $PROJECT_ID ...
gcloud config set project $PROJECT_ID

# Check if the service account already exists
if gcloud iam service-accounts list --format="value(email)" | grep -q $SERVICE_ACCOUNT; then

    echo "Service account $SERVICE_ACCOUNT already exists. Continue? [y/n]"
    read answer

    if [[ ! $answer =~ ^[yY]$ ]]; then
        echo "Exiting..."
        exit 0
    fi

else
    echo Creating service account $SERVICE_ACCOUNT ...
    gcloud iam service-accounts create $SERVICE_ACCOUNT_NAME --description="Service account for $PROJECT_NAME" --display-name=$SERVICE_ACCOUNT_NAME 

    # Wait for the service account to be created
    sleep 3
fi

# Check if bucket already exists
if gsutil ls | grep -q $BUCKET_URI; then

    echo "Bucket $BUCKET_URI already exists. Continue? [y/n]"
    read answer

    if [[ ! $answer =~ ^[yY]$ ]]; then
        echo "Exiting..."
        exit 0
    fi

else
    echo Creating bucket $BUCKET_URI ...
    gsutil mb -l $LOCATION $BUCKET_URI

    echo Giving service account admin permissions for $BUCKET_URI
    gsutil iam ch serviceAccount:$SERVICE_ACCOUNT:roles/storage.objectAdmin $BUCKET_URI
fi


echo Granting permissions to $SERVICE_ACCOUNT to read from artifact registry and run pipelines...

gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:$SERVICE_ACCOUNT" \
    --role="roles/artifactregistry.reader" \
    --condition=None \
    # > /dev/null 2>&1

gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:$SERVICE_ACCOUNT" \
    --role="roles/aiplatform.user" \
    --condition=None \
    # > /dev/null 2>&1

echo Giving access to secrets

# Optional: Check if secrets exist before adding bindings, or ignore errors
gcloud secrets add-iam-policy-binding projects/$PROJECT_REFERENCE/secrets/$CSA_USER \
    --member="serviceAccount:$SERVICE_ACCOUNT" \
    --role="roles/secretmanager.secretAccessor" \
    --condition=None \
    || echo "Warning: Could not add binding for $CSA_USER"

gcloud secrets add-iam-policy-binding projects/$PROJECT_REFERENCE/secrets/$CSA_KEY \
    --member="serviceAccount:$SERVICE_ACCOUNT" \
    --role="roles/secretmanager.secretAccessor" \
    --condition=None \
    || echo "Warning: Could not add binding for $CSA_KEY"

gcloud secrets add-iam-policy-binding projects/$PROJECT_REFERENCE/secrets/$SNOWFLAKE_USER \
    --member="serviceAccount:$SERVICE_ACCOUNT" \
    --role="roles/secretmanager.secretAccessor" \
    --condition=None \
    || echo "Warning: Could not add binding for $SNOWFLAKE_USER"

