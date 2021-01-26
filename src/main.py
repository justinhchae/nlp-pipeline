"""Creating and executing a pipeline.
"""
from configuration import run_configuration
from stage_factory import create_pipeline_from_config

import argparse

parser = argparse.ArgumentParser(description='Running a pipeline.')
parser.add_argument('--config-file', action='append', nargs='+')
args = parser.parse_args()

run_configuration()
for config_file in args.config_file:
    pipeline = create_pipeline_from_config(config_file[0])
    pipeline.execute()
