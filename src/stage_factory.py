"""Factory for stages.
"""
from pipeline import Pipeline
from stage_corpus_analysis import CorpusAnalysisStage
from stage_corpus_split import CorpusSplitStage
from stage_fandom_wiki_scraping import FandomWikiScrapingStage
from stage_fandom_wiki_text_cleaning import FandomWikiTextCleaningStage
from stage_frequency_filtering import FrequencyFilteringStage
from stage_srilm_model import SRILMModelStage
from stage_wikipedia_scraping import WikipediaScrapingStage
from stage_wikipedia_text_cleaning import WikipediaTextCleaningStage

import constants

from os.path import join

import yaml

possible_stages = [CorpusAnalysisStage,
                   CorpusSplitStage,
                   FandomWikiScrapingStage,
                   FandomWikiTextCleaningStage,
                   FrequencyFilteringStage,
                   SRILMModelStage,
                   WikipediaScrapingStage,
                   WikipediaTextCleaningStage]
stage_name_mapping = {s.name: s for s in possible_stages}

def create_stage(stage_config):
    """A factory method for creating a stage.

    Args:
        stage_config: a dictionary with the configuration details for the stage.

    Returns:
        A stage generated with provided configuration.
    """
    if stage_config["name"] not in stage_name_mapping:
        raise LookupError("There is no stage with the {} name.".format(stage_config["name"]))
        return None
    stage_name = stage_config["name"]
    del stage_config["name"]
    return stage_name_mapping[stage_name](**stage_config)

def create_pipeline(pipeline_config):
    """A factory method for creating a pipeline.

    Args:
        pipeline_config: a dictionary with the configuration details for the pipeline.

    Returns:
        A pipeline generated with provided configuration.
    """
    stages = [create_stage(stage_config) for stage_config in pipeline_config["stages"]]
    del pipeline_config["stages"]
    return Pipeline(stages=stages, **pipeline_config)

def create_pipeline_from_config(config_filename="pipeline_config.yaml"):
    """A factory method for creating a pipeline from config file.

    Args:
        config_filename: str with the name of the config file.

    Returns:
        A pipeline generated with provided configuration.
    """
    config_filepath = join(constants.CONFIG_PATH, config_filename)
    with open(config_filepath) as file:
        pipeline_config = yaml.load(file, Loader=yaml.FullLoader)
        pipeline = create_pipeline(pipeline_config)
    return pipeline
