# Cloud Computing Mini-Project 2: Cloud Service Log Analytics

This repository is a starter implementation for the workflow:

`cloud service log dataset -> cloud object storage -> MapReduce baseline analytics -> Ray extension analytics -> comparison`

The implementation uses:

- Alibaba Cloud OSS for object storage evidence.
- Hadoop Streaming compatible Python mapper/reducer scripts for the MapReduce baseline.
- Ray remote tasks for degraded service detection.
- Local simulation scripts for correctness checking before running on Alibaba Cloud ECS/EMR.

## 1. Project structure

```text
cloud_miniproject2_alicloud_starter/
├── data/
│   └── cloud_service_logs.csv
├── mapreduce/
│   ├── request_count_mapper.py
│   ├── request_count_reducer.py
│   ├── error_count_mapper.py
│   ├── error_count_reducer.py
│   ├── slow_endpoint_mapper.py
│   └── slow_endpoint_reducer.py
├── ray/
│   └── degraded_service_detection.py
├── scripts/
│   ├── run_local_mapreduce.py
│   ├── run_all_local.py
│   └── manual_validate.py
├── deploy/alicloud/
│   ├── oss_upload_example.sh
│   └── run_hadoop_streaming_example.sh
├── outputs/
│   ├── request_count_by_service.txt
│   ├── server_error_count_by_service.txt
│   ├── top10_slow_endpoints.txt
│   ├── degraded_services_ray.csv
│   ├── ray_service_metrics.csv
│   └── runtime_results.txt
└── docs/
    └── report_evidence_notes.md
```

## 2. Local environment

Python 3.10+ is recommended.

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

On Windows PowerShell:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

## 3. Run locally first

Run MapReduce simulation and Ray detection locally:

```bash
python scripts/run_all_local.py --input data/cloud_service_logs.csv --output-dir outputs
```

Validate the outputs against an independent direct Python calculation:

```bash
python scripts/manual_validate.py --input data/cloud_service_logs.csv --output-dir outputs
```

## 4. Upload dataset to Alibaba Cloud OSS

Edit `deploy/alicloud/oss_upload_example.sh` and replace the placeholders:

```bash
OSS_BUCKET="your-anonymized-bucket-name"
OSS_PREFIX="mini-project-2/input"
OSS_ENDPOINT="oss-cn-hangzhou.aliyuncs.com"
```

Then run:

```bash
bash deploy/alicloud/oss_upload_example.sh
```

Take an anonymised screenshot or terminal output showing that the dataset object exists in OSS. Do not expose bucket names, account names, AccessKey values, user names, or identifiable paths in the report.

## 5. Run MapReduce with Hadoop Streaming

After copying the dataset from OSS to the Hadoop environment or HDFS, use:

```bash
bash deploy/alicloud/run_hadoop_streaming_example.sh
```

Edit the script first to match your Hadoop Streaming jar path, input path, and output path.

## 6. Expected core outputs

The project requires:

1. Request count by service.
2. Server error count by service, where `status_code >= 500`.
3. Top 10 slow endpoints, where `response_time_ms > 800`.
4. Ray degraded-service detection, where a service is degraded if:
   - slow request rate > 20%, or
   - server error rate > 10%, or
   - Timeout error count >= 5.

## 7. Report evidence checklist

For the group report, prepare selected anonymised evidence:

- OSS object-storage evidence.
- One row from each MapReduce output.
- Ray degraded-service output.
- One concrete validation example.
- Runtime table: one MapReduce job runtime and Ray detection runtime.
- Execution environment description.
- Team integration example using Member A/B/C only.

For the individual report, focus on your own tasks, artefacts, verification, and GenAI use.
