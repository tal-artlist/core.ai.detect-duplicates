from kfp import dsl
from operators import download_song_op
import config.settings as settings

@dsl.pipeline(
    name=settings.PIPELINE_NAME,
    pipeline_root=settings.PIPELINE_ROOT
)
def detect_duplicates_pipeline(
    asset_id: int = 122429
):
    """
    Simple pipeline to download a song.
    """
    download_task = download_song_op(
        asset_id=asset_id
    ).set_display_name("Download Song")

