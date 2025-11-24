from google.cloud import aiplatform as aip
from compile import compile_pipeline
import config.settings as settings
from construct_pipeline import detect_duplicates_pipeline
from artlist.ai.kf_components.utils.logging import getDefaultLogger

logger = getDefaultLogger(__name__, level='INFO')

if __name__ == '__main__':
    pipeline_func = detect_duplicates_pipeline
    pipeline_name = settings.PIPELINE_NAME
    
    print(f"Deploying pipeline: {pipeline_name}")
    print(f"Project ID: {settings.PROJECT_ID}")
    print(f"Region: {settings.LOCATION}")
    
    input_result = input(f"This will deploy {pipeline_name} to {settings.AL_ENV} - continue? (y/n)")

    if input_result.lower() not in ['y', 'yes']:
        logger.info('Exiting')
        exit()

    # compile the pipeline
    pipeline_file = compile_pipeline(pipeline_name, pipeline_func)

    aip.init(
        project=settings.PROJECT_ID,
        location=settings.LOCATION,
        staging_bucket=settings.PIPELINE_ROOT,
    )

    job = aip.PipelineJob(
        display_name=pipeline_name,
        template_path=pipeline_file,
        pipeline_root=settings.PIPELINE_ROOT,
        enable_caching=False
    )

    job.run(
        service_account=settings.SERVICE_ACCOUNT,
        sync=False
    )

