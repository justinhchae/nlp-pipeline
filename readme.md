# NLP pipeline

This is an NLP pipeline.

## Overview

This is a natural-language processing pipeline. Currently it supports these stages:

1. Scrape articles from the Wikipedia.
2. Clean the scraped text.
3. Perform a rudimentary analysis on the cleaned text.
4. Split the text into training / testing / validation files.
5. Perform frequency filtering.

## Executing pipeline / workflow

Edit the pipeline_config file to run the stages that you want, and run the following command:
```
make run
```

To clean the directory:
```
make clean
```

## Logging

This project uses logging library. The workflow generates log files that can be found in logs folder. Use logger.info / debug / error / warning instead of print for proper logging when creating new stages.
