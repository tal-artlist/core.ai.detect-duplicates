from kfp import dsl
from artlist.ai.kf_components.images import SLIM_IMAGE

@dsl.component(
    base_image=SLIM_IMAGE,
    packages_to_install=[
        "cryptography",
        "snowflake-connector-python",
        "google-cloud-secret-manager",
        "requests"
    ]
)
def download_song_op(
    asset_id: int
) -> str:
    import os
    import logging
    from download_utils.download_song import get_file_key, get_download_url, download_file

    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    logger.info(f"Looking up asset {asset_id}...")
    file_info = get_file_key(asset_id)
    if not file_info:
        msg = f"Asset {asset_id} not found in Artlist or MotionArray"
        logger.error(msg)
        raise ValueError(msg)
    
    logger.info(f"Found: {file_info['source']} {file_info['format']}")
    
    logger.info(f"Getting download URL...")
    url = get_download_url(file_info['file_key'])
    if not url:
        msg = f"Could not get download URL"
        logger.error(msg)
        raise ValueError(msg)
    
    filename = f"asset_{asset_id}_{file_info['source']}.{file_info['format'].lower()}"
    # Save to a path that will be preserved or just temp?
    # The user just wants to download it. In a pipeline, files are ephemeral unless saved to output artifact.
    # But the prompt says "it will download the song and thats it".
    
    output_path = filename
    
    logger.info(f"Downloading to {output_path}...")
    download_file(url, output_path)
    
    file_size = os.path.getsize(output_path) / 1024 / 1024
    logger.info(f"Downloaded {file_size:.1f} MB successfully!")
    
    return output_path
