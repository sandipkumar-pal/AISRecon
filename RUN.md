# Pipeline Execution Instructions

## Local Execution
1. Create a Python 3.10 virtual environment.
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Adjust `config.yaml` to point at local or S3 inputs.
4. Run the pipeline:
   ```bash
   python main_pipeline.py --config config.yaml
   ```
   > TODO: Implement CLI parsing in `main_pipeline.py`.

## AWS Batch Notes
- Package this repository as a container image with Python 3.10 runtime.
- Mount IAM role or credentials for S3 access; ensure `s3fs` is installed.
- Use environment variables to override config path and runtime parameters (TODO in loader).
- Emit logs to CloudWatch by configuring the root logger before invoking `run_pipeline`.

## Airflow DAG Integration
- Import `run_pipeline` within an Airflow DAG PythonOperator.
- Use Airflow Variables or Connections to supply configuration paths or overrides.
- Consider chunked ingestion (`iter_batches`) to manage memory on worker nodes (TODO in ingest modules).
- Leverage Airflow XCom to capture `output_artifacts` returned from the pipeline.
