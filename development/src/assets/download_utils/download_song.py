#!/usr/bin/env python3
"""
Download a song by asset ID.
Usage: python download_song.py <asset_id>
Example: python download_song.py 1234
"""

import sys
import os
import requests
try:
    from .snowflake_utils import SnowflakeConnector
except ImportError:
    from snowflake_utils import SnowflakeConnector


def get_file_key(asset_id: int):
    """Get file key from AUDIO_FINGERPRINT table"""
    snowflake = SnowflakeConnector()
    
    query = f"""
    SELECT FILE_KEY, SOURCE, FORMAT
    FROM AI_DATA.AUDIO_FINGERPRINT
    WHERE ASSET_ID = '{asset_id}'
        AND PROCESSING_STATUS = 'SUCCESS'
        AND FINGERPRINT IS NOT NULL
    LIMIT 1
    """
    
    cursor = snowflake.execute_query(query)
    result = cursor.fetchone()
    cursor.close()
    snowflake.close()
    
    if result:
        return {'file_key': result[0], 'source': result[1], 'format': result[2]}
    
    return None


def get_auth_token():
    """Get OAuth token for API authentication"""
    try:
        from google.cloud import secretmanager
        client = secretmanager.SecretManagerServiceClient()
        
        project_id = "889375371783"
        user_secret = f"projects/{project_id}/secrets/CSA-OAPI-DEV-USER/versions/latest"
        key_secret = f"projects/{project_id}/secrets/CSA-OAPI-DEV-KEY/versions/latest"
        
        user = client.access_secret_version(request={"name": user_secret}).payload.data.decode('UTF-8')
        key = client.access_secret_version(request={"name": key_secret}).payload.data.decode('UTF-8')
        
        auth_url = "https://oapi-gw.dev.artlist.io/oauth2/token"
        headers = {
            "service-host": "core.content.cms.api",
            "Content-Type": "application/x-www-form-urlencoded"
        }
        data = {"grant_type": "client_credentials"}
        
        auth_response = requests.post(
            auth_url,
            headers=headers,
            data=data,
            auth=(user, key),
            timeout=30
        )
        auth_response.raise_for_status()
        
        token_data = auth_response.json()
        return f"{token_data['token_type']} {token_data['access_token']}"
    except Exception as e:
        print(f"Failed to get auth token: {e}")
        raise


def get_download_url(file_key: str):
    """Get download URL from API"""
    api_url = "https://oapi-int.artlist.io/v1/content/bulkDownloadArtifacts"
    
    # Get authentication token
    auth_token = get_auth_token()
    
    headers = {
        'service-host': 'core.content.cms.api',
        'Content-Type': 'application/json',
        'Authorization': auth_token
    }
    
    response = requests.post(api_url, headers=headers, json={"keys": [file_key]}, timeout=30)
    response.raise_for_status()
    
    for key, resp in response.json()['data']['downloadArtifactResponses'].items():
        if 'url' in resp:
            return resp['url']
    return None


def download_file(url: str, output_path: str):
    """Download file"""
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    response = requests.get(url, stream=True, timeout=60)
    response.raise_for_status()
    
    with open(output_path, 'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                f.write(chunk)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python download_song.py <asset_id>")
        print("Example: python download_song.py 1234")
        sys.exit(1)
    
    try:
        asset_id = int(sys.argv[1])
    except ValueError:
        print("❌ Asset ID must be a number")
        sys.exit(1)
    
    print(f"🔍 Looking up asset {asset_id}...")
    file_info = get_file_key(asset_id)
    if not file_info:
        print(f"❌ Asset {asset_id} not found in Artlist or MotionArray")
        sys.exit(1)
    
    print(f"✅ Found: {file_info['source']} {file_info['format']}")
    
    print(f"🔗 Getting download URL...")
    url = get_download_url(file_info['file_key'])
    if not url:
        print(f"❌ Could not get download URL")
        sys.exit(1)
    
    filename = f"asset_{asset_id}_{file_info['source']}.{file_info['format'].lower()}"
    output_path = os.path.join("downloads", filename)
    
    print(f"⬇️  Downloading to {output_path}...")
    download_file(url, output_path)
    
    file_size = os.path.getsize(output_path) / 1024 / 1024
    print(f"✅ Downloaded {file_size:.1f} MB successfully!")


