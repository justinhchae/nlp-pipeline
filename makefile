PYTHON = python3
PIP = pip3

run: prep
	$(PYTHON) src/main.py --config-file wikipedia_scraping_pipeline.yaml --config-file srilm_model_pipeline.yaml

wikipedia-scraping: prep
	$(PYTHON) src/main.py --config-file wikipedia_scraping_pipeline.yaml

srilm-model: prep
	$(PYTHON) src/main.py --config-file srilm_model_pipeline.yaml

prep:
	mkdir -p logs tmp data output

lint:
	pylint src/

install:
	 $(PIP) install -r requirements.txt

clean:
	find . -type f -name \*.pyc -exec rm {} \;
	rm -rf dist *.egg-info .coverage .DS_Store logs tmp data output apicache-py3 *.lwp *.ctrl
