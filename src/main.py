"""Creating and executing a pipeline.
"""
from configuration import run_configuration
from stage_factory import create_pipeline_from_config

pipeline = create_pipeline_from_config("pipeline_config.yaml")
run_configuration()
pipeline.execute()
