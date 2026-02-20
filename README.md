# MLOps Batch Job

## Setup
pip install -r requirements.txt

## Run Locally
python run.py --input data.csv --config config.yaml --output metrics.json --log-file run.log

## Docker
docker build -t mlops-task .
docker run --rm mlops-task

## Output
metrics.json contains computed signal_rate metric.

## Dependencies
- pandas
- numpy
- pyyaml
