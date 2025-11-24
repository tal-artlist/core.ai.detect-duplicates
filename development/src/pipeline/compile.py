from artlist.ai.kf_components.compiler import Compiler
import logging
import config.settings as settings
from construct_pipeline import detect_duplicates_pipeline

def compile_pipeline(pipeline_name: str, pipeline_func):
    """
    Compiles the given pipeline function into a YAML file.
    """
    compiler = Compiler(
        project_id=settings.PROJECT_ID,
        verbosity=logging.DEBUG
    )
    
    pipeline_file = f'{pipeline_name}.yaml'
    
    # Parameters can be extracted or passed explicitly if needed
    # Here we rely on defaults in the pipeline function or pass empty dict if not overriding
    params = {} 

    compiler.compile(
        pipeline_func=pipeline_func,
        package_path=pipeline_file,
        pipeline_name=pipeline_name,
        pipeline_parameters=params,
        dependencies={
            'download_song_op': ['download_utils'],
        },
        local_assets_dir=settings.ASSETS_DIR,
        target_assets_dir=settings.ASSETS_URI,
        exclude=['*.pyc', '__pycache__/']
    )

    return pipeline_file

