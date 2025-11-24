#!/usr/bin/env python3
"""
Snowflake-specific utility functions for the bulk downloader.
"""

import json
import logging
import os
from typing import List, Dict, Any, Optional

logger = logging.getLogger(__name__)

try:
    import snowflake.connector
    from google.cloud import secretmanager
    SNOWFLAKE_AVAILABLE = True
except ImportError:
    SNOWFLAKE_AVAILABLE = False
    logger.warning("Snowflake/GCP dependencies not available. Install with: pip install snowflake-connector-python google-cloud-secret-manager")


class SnowflakeConnector:
    """Manages Snowflake connections and queries for audio fingerprint processing."""
    
    def __init__(self, config: Dict = None):
        """Initialize the Snowflake connector."""
        if not SNOWFLAKE_AVAILABLE:
            raise ImportError("Snowflake/GCP dependencies not available")
        self.config = config or {}
        self._connection = None
        
        # Check for environment variables as fallback
        if not self.config and all(key in os.environ for key in ['SNOWFLAKE_USER', 'SNOWFLAKE_PASSWORD', 'SNOWFLAKE_ACCOUNT']):
            self.config = {
                'user': os.environ['SNOWFLAKE_USER'],
                'password': os.environ['SNOWFLAKE_PASSWORD'], 
                'account': os.environ['SNOWFLAKE_ACCOUNT'],
                'warehouse': os.environ.get('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH'),
                'database': 'BI_PROD',
                'schema': 'AI_DATA'
            }
    
    def get_snowflake_secret(self, secret_id: str = "snowflake-secret-privatekey", 
                           project_id: str = "889375371783") -> Optional[Dict[str, Any]]:
        """
        Get Snowflake credentials from Google Cloud Secret Manager.
        Defaults to 'snowflake-secret-privatekey' from project '889375371783'.
        """
        try:
            # If project_id is explicitly None, try to derive from env (backward compatibility)
            if project_id is None:
                al_env = os.environ.get('AL_ENV')
                env = 'prd' if al_env == 'prd' else 'dev'
                project_id = f"artlist-ai-{env}"

            client = secretmanager.SecretManagerServiceClient()
            name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
            response = client.access_secret_version(request={"name": name})
            return json.loads(response.payload.data.decode("UTF-8"))
        except Exception as e:
            logger.warning(f"Failed to get credentials from Google Cloud ({project_id}/{secret_id}): {e}")
            return None
    
    def get_legacy_secret(self, secret_name: str = "snowflake-ai_team_artlist", 
                         env: str = "prd") -> Optional[Dict[str, Any]]:
        """Fallback method for legacy secret retrieval."""
        try:
            # Try to get the legacy secret from the dynamic project ID
            al_env = os.environ.get('AL_ENV', 'dev')
            project_id = f"artlist-ai-{al_env}"
            return self.get_snowflake_secret(secret_id=secret_name, project_id=project_id)
        except Exception as e:
            logger.warning(f"Failed to get legacy credentials: {e}")
            return None
    
    def _get_connection(self):
        """Get or create Snowflake connection"""
        if self._connection is None:
            # Try provided config first
            if self.config:
                creds = self.config
            else:
                # Try the primary secret (snowflake-secret-privatekey from 889375371783)
                creds = self.get_snowflake_secret()
                
                # Fallback to legacy name if primary fails
                if creds is None:
                    logger.info("Primary secret not found, trying legacy secret...")
                    creds = self.get_legacy_secret()
                
                if creds is None:
                    raise Exception("Could not obtain Snowflake credentials from any source")
            
            # Add defaults for BI PROD AI DATA
            if "database" not in creds:
                creds["database"] = "BI_PROD"
            if "schema" not in creds:
                creds["schema"] = "AI_DATA"
            
            # Add connection timeout settings to prevent hanging
            if "login_timeout" not in creds:
                creds["login_timeout"] = 30  # 30 seconds to establish connection
            if "network_timeout" not in creds:
                creds["network_timeout"] = 60  # 60 seconds for query operations
            
            # Handle private key conversion if present
            # Snowflake connector expects bytes for private_key, but JSON gives string
            if "private_key" in creds and isinstance(creds["private_key"], str):
                # Simple check if it looks like a PEM key
                if "-----BEGIN" in creds["private_key"]:
                    creds["private_key"] = creds["private_key"].encode('utf-8')

            # Connect
            self._connection = snowflake.connector.connect(**creds)
        
        return self._connection
    
    def execute_query(self, query: str, params: Dict = None):
        """Execute query with optional parameters"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            # Commit the transaction immediately for write operations
            if query.strip().upper().startswith(('INSERT', 'UPDATE', 'DELETE', 'CREATE')):
                conn.commit()
            return cursor
        except Exception as e:
            cursor.close()
            raise e
    
    def close(self):
        """Close connection"""
        if self._connection:
            self._connection.close()
            self._connection = None


class SnowflakeManager(SnowflakeConnector):
    """Legacy alias for backward compatibility"""
    
    def __init__(self):
        """Initialize the Snowflake manager."""
        super().__init__()
    
    def get_snowflake_secret(self, secret_id: str = "snowflake-secret-privatekey", 
                           project_id: str = "889375371783") -> Optional[Dict[str, Any]]:
        """
        Get Snowflake credentials from Google Cloud Secret Manager.
        """
        return super().get_snowflake_secret(secret_id, project_id)
    
    def get_legacy_secret(self, secret_name: str = "snowflake-ai_team_artlist", 
                         env: str = "prd") -> Optional[Dict[str, Any]]:
        return super().get_legacy_secret(secret_name, env)
    
    def open_snowflake_cursor(self):
        """
        Open Snowflake cursor with proper authentication.
        """
        conn = self._get_connection()
        return conn.cursor()
    
    def execute_query(self, query: str) -> List[Dict[str, Any]]:
        """
        Execute a Snowflake query and return results as list of dictionaries.
        """
        cursor = None
        try:
            cursor = self.open_snowflake_cursor()
            cursor.execute(query)
            results = cursor.fetchall()
            
            # Convert results to list of dictionaries
            if cursor.description:
                columns = [desc[0] for desc in cursor.description]
                data = []
                for row in results:
                    row_dict = dict(zip(columns, row))
                    data.append(row_dict)
                return data
            return []
            
        except Exception as e:
            logger.error(f"Failed to execute Snowflake query: {e}")
            raise
        finally:
            if cursor:
                cursor.close()


def get_artlist_query(limit: int = 100) -> str:
    """
    Get the SQL query for retrieving Artlist assets.
    
    Args:
        limit: Maximum number of assets to retrieve
        
    Returns:
        SQL query string
    """
    return f"""
    WITH base AS (
      SELECT
        da.asset_id,
        sf.filekey                    AS file_key,
        sf.role                       AS format,
        sf.createdat                  AS created_at
      FROM BI_PROD.dwh.DIM_ASSETS da
      JOIN ODS_PROD.cross_products_ods.POSTGRES_ASM_ASSET a
        ON da.asset_id::string = a.externalid::int::string
      JOIN ODS_PROD.cross_products_ods.POSTGRES_ASM_songFILE sf
        ON a.id = sf.songid
      WHERE da.product_indicator = 1
        AND da.asset_type = 'Music'
        AND sf.role IN ('CORE', 'MP3')  -- Only WAV (CORE) and MP3 formats
    ),
    one_per_format AS (
      SELECT asset_id, format, file_key, created_at
      FROM (
        SELECT
          asset_id,
          format,
          file_key,
          created_at,
          ROW_NUMBER() OVER (
            PARTITION BY asset_id, format
            ORDER BY created_at DESC, file_key
          ) AS rn
        FROM base
      )
      WHERE rn = 1
    )
    SELECT
      asset_id,
      COUNT(*) AS num_file_keys,
      ARRAY_AGG(file_key) AS file_keys,
      ARRAY_AGG(format)   AS formats,
      ARRAY_AGG(OBJECT_CONSTRUCT('file_key', file_key, 'format', format)) AS key_format_pairs,
      OBJECT_AGG(format, TO_VARIANT(file_key)) AS format_to_file_key
    FROM one_per_format
    GROUP BY asset_id
    ORDER BY num_file_keys DESC, asset_id
    LIMIT {limit};
    """


def get_motionarray_query(limit: int = 100) -> str:
    """
    Get the SQL query for retrieving MotionArray assets.
    
    Args:
        limit: Maximum number of assets to retrieve
        
    Returns:
        SQL query string
    """
    return f"""
    WITH base AS (
      SELECT
        a.asset_id,
        b.guid AS file_key,
        CASE
          WHEN pf.format_id = 1 THEN 'WAV'
          WHEN pf.format_id = 2 THEN 'MP3'
          WHEN pf.format_id = 3 THEN 'AIFF'
          ELSE NULL
        END AS format,
        c.created_at
      FROM BI_PROD.dwh.DIM_ASSETS a
      JOIN ODS_PROD.motion_array_ods.MYSQL_PRODUCT_CMS_RESOLUTIONS b
        ON a.asset_id::string = b.product_id::string
      JOIN ODS_PROD.motion_array_ods.MYSQL_PRODUCT_CMS_AUDIO_RESOLUTIONS c
        ON b.id = c.parent_id
      LEFT JOIN ODS_PROD.motion_array_ods.MYSQL_PRODUCT_FORMAT pf
        ON pf.product_id = a.asset_id
      WHERE a.product_indicator = 3
        AND a.asset_sub_type ILIKE '%music%'
        AND b.resolution_format = 1
        AND format IS NOT NULL
    ),
    one_per_format AS (
      SELECT asset_id, format, file_key, created_at
      FROM (
        SELECT
          asset_id,
          format,
          file_key,
          created_at,
          ROW_NUMBER() OVER (
            PARTITION BY asset_id, format
            ORDER BY created_at DESC, file_key
          ) AS rn
        FROM base
      )
      WHERE rn = 1
    )
    SELECT
      asset_id,
      COUNT(*) AS num_file_keys,
      ARRAY_AGG(file_key) AS file_keys,
      ARRAY_AGG(format)   AS formats,
      ARRAY_AGG(OBJECT_CONSTRUCT('file_key', file_key, 'format', format)) AS key_format_pairs,
      OBJECT_AGG(format, TO_VARIANT(file_key)) AS format_to_file_key
    FROM one_per_format
    GROUP BY asset_id
    ORDER BY num_file_keys DESC, asset_id
    LIMIT {limit};
    """


def get_artlist_keys_from_snowflake(limit: int = 100) -> List[Dict[str, Any]]:
    """
    Get Artlist file keys from Snowflake using the provided query.
    Only gets WAV and MP3 formats, one file per format per asset.
    
    Args:
        limit: Maximum number of assets to retrieve
        
    Returns:
        List of dictionaries with asset_id and key_format_pairs
    """
    snowflake_manager = SnowflakeManager()
    query = get_artlist_query(limit)
    
    try:
        data = snowflake_manager.execute_query(query)
        logger.info(f"Retrieved {len(data)} Artlist assets from Snowflake")
        return data
    except Exception as e:
        logger.error(f"Failed to get Artlist keys from Snowflake: {e}")
        raise


def get_motionarray_keys_from_snowflake(limit: int = 100) -> List[Dict[str, Any]]:
    """
    Get MotionArray file keys from Snowflake using the provided query.
    Gets one file per format per asset.
    
    Args:
        limit: Maximum number of assets to retrieve
        
    Returns:
        List of dictionaries with asset_id and key_format_pairs
    """
    snowflake_manager = SnowflakeManager()
    query = get_motionarray_query(limit)
    
    try:
        data = snowflake_manager.execute_query(query)
        logger.info(f"Retrieved {len(data)} MotionArray assets from Snowflake")
        return data
    except Exception as e:
        logger.error(f"Failed to get MotionArray keys from Snowflake: {e}")
        raise
