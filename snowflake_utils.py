#!/usr/bin/env python3
"""
Snowflake-specific utility functions for the bulk downloader.
"""

import json
import logging
from typing import List, Dict, Any, Optional

logger = logging.getLogger(__name__)

try:
    import snowflake.connector
    from google.cloud import secretmanager
    SNOWFLAKE_AVAILABLE = True
except ImportError:
    SNOWFLAKE_AVAILABLE = False
    logger.warning("Snowflake/GCP dependencies not available. Install with: pip install snowflake-connector-python google-cloud-secret-manager")


class SnowflakeManager:
    """Manages Snowflake connections and queries."""
    
    def __init__(self):
        """Initialize the Snowflake manager."""
        if not SNOWFLAKE_AVAILABLE:
            raise ImportError("Snowflake/GCP dependencies not available")
    
    def get_snowflake_secret(self, secret_id: str = "ai_team_snowflake_credentials", 
                           project_id: str = "889375371783") -> Optional[Dict[str, Any]]:
        """
        Get Snowflake credentials from Google Cloud Secret Manager.
        
        Args:
            secret_id: Secret ID in Google Cloud
            project_id: Google Cloud project ID
            
        Returns:
            Dictionary with Snowflake credentials or None if failed
        """
        try:
            client = secretmanager.SecretManagerServiceClient()
            name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
            response = client.access_secret_version(request={"name": name})
            return json.loads(response.payload.data.decode("UTF-8"))
        except Exception as e:
            logger.warning(f"Failed to get credentials from Google Cloud: {e}")
            return None
    
    def get_legacy_secret(self, secret_name: str = "snowflake-ai_team-artlist", 
                         env: str = "prd") -> Optional[Dict[str, Any]]:
        """
        Fallback method for legacy secret retrieval.
        
        Args:
            secret_name: Name of the secret
            env: Environment (prd, dev, etc.)
            
        Returns:
            Dictionary with credentials or None
        """
        try:
            # This would need to be implemented based on your legacy system
            # For now, return None to indicate fallback failed
            logger.warning("Legacy secret retrieval not implemented")
            return None
        except Exception as e:
            logger.warning(f"Failed to get legacy credentials: {e}")
            return None
    
    def open_snowflake_cursor(self):
        """
        Open Snowflake cursor with proper authentication.
        
        Returns:
            Snowflake cursor object
            
        Raises:
            Exception: If credentials cannot be obtained or connection fails
        """
        # Try Google Cloud first
        creds = self.get_snowflake_secret()
        
        # Fallback to legacy method
        if creds is None:
            creds = self.get_legacy_secret()
        
        if creds is None:
            raise Exception("Could not obtain Snowflake credentials from any source")
        
        # Add defaults
        if "database" not in creds:
            creds["database"] = "BI_PROD"
        if "schema" not in creds:
            creds["schema"] = "AI_DATA"
        
        # Connect
        conn = snowflake.connector.connect(**creds)
        return conn.cursor()
    
    def execute_query(self, query: str) -> List[Dict[str, Any]]:
        """
        Execute a Snowflake query and return results as list of dictionaries.
        
        Args:
            query: SQL query to execute
            
        Returns:
            List of dictionaries with query results
            
        Raises:
            Exception: If query execution fails
        """
        cursor = None
        try:
            cursor = self.open_snowflake_cursor()
            cursor.execute(query)
            results = cursor.fetchall()
            
            # Convert results to list of dictionaries
            columns = [desc[0] for desc in cursor.description]
            data = []
            
            for row in results:
                row_dict = dict(zip(columns, row))
                data.append(row_dict)
            
            return data
            
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
