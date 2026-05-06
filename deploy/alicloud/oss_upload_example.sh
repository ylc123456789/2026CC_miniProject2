#!/usr/bin/env bash
# Upload the dataset to Alibaba Cloud OSS.
# Replace the placeholder values before running this script.
# Do not commit real AccessKey values or identity-revealing bucket names.

set -euo pipefail

OSS_BUCKET="your-anonymized-bucket-name"
OSS_PREFIX="mini-project-2/input"
OSS_ENDPOINT="oss-cn-hangzhou.aliyuncs.com"
LOCAL_DATASET="data/cloud_service_logs.csv"

# Recommended: configure credentials with `ossutil config` or a config file.
# Avoid passing AccessKey values directly in shell commands.

ossutil cp "${LOCAL_DATASET}" "oss://${OSS_BUCKET}/${OSS_PREFIX}/cloud_service_logs.csv" -e "${OSS_ENDPOINT}"
ossutil ls "oss://${OSS_BUCKET}/${OSS_PREFIX}/" -e "${OSS_ENDPOINT}"
