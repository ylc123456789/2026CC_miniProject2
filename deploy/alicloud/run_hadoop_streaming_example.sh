#!/usr/bin/env bash
# Hadoop Streaming example for Alibaba Cloud ECS/EMR or a Hadoop environment.
# Edit the paths before running.

set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
HADOOP_STREAMING_JAR="/path/to/hadoop-streaming.jar"
LOCAL_DATASET="${PROJECT_DIR}/data/cloud_service_logs.csv"
HDFS_INPUT="/user/cloudproject2/input/cloud_service_logs.csv"
HDFS_OUTPUT_BASE="/user/cloudproject2/output"

# If your Hadoop command cannot find the streaming jar, locate it with:
# find /opt/apps -name "hadoop-streaming*.jar" 2>/dev/null | grep -v sources
# or:
# find /usr -name "hadoop-streaming*.jar" 2>/dev/null | grep -v sources

hadoop fs -mkdir -p /user/cloudproject2/input
hadoop fs -put -f "${LOCAL_DATASET}" "${HDFS_INPUT}"
hadoop fs -rm -r -f "${HDFS_OUTPUT_BASE}/request_count" || true
hadoop fs -rm -r -f "${HDFS_OUTPUT_BASE}/error_count" || true
hadoop fs -rm -r -f "${HDFS_OUTPUT_BASE}/slow_endpoint" || true

# Job 1: request count by service
hadoop jar "${HADOOP_STREAMING_JAR}" \
  -D mapreduce.job.name="project2_request_count_by_service" \
  -files "${PROJECT_DIR}/mapreduce/request_count_mapper.py,${PROJECT_DIR}/mapreduce/request_count_reducer.py" \
  -mapper "python3 request_count_mapper.py" \
  -reducer "python3 request_count_reducer.py" \
  -input "${HDFS_INPUT}" \
  -output "${HDFS_OUTPUT_BASE}/request_count"

# Job 2: server error count by service
hadoop jar "${HADOOP_STREAMING_JAR}" \
  -D mapreduce.job.name="project2_server_error_count_by_service" \
  -files "${PROJECT_DIR}/mapreduce/error_count_mapper.py,${PROJECT_DIR}/mapreduce/error_count_reducer.py" \
  -mapper "python3 error_count_mapper.py" \
  -reducer "python3 error_count_reducer.py" \
  -input "${HDFS_INPUT}" \
  -output "${HDFS_OUTPUT_BASE}/error_count"

# Job 3: top 10 slow endpoints
# Use one reducer because top-10 requires a global ranking.
hadoop jar "${HADOOP_STREAMING_JAR}" \
  -D mapreduce.job.name="project2_top10_slow_endpoints" \
  -D mapreduce.job.reduces=1 \
  -files "${PROJECT_DIR}/mapreduce/slow_endpoint_mapper.py,${PROJECT_DIR}/mapreduce/slow_endpoint_reducer.py" \
  -mapper "python3 slow_endpoint_mapper.py" \
  -reducer "python3 slow_endpoint_reducer.py" \
  -input "${HDFS_INPUT}" \
  -output "${HDFS_OUTPUT_BASE}/slow_endpoint"

mkdir -p "${PROJECT_DIR}/outputs/hadoop"
hadoop fs -cat "${HDFS_OUTPUT_BASE}/request_count/part-*" > "${PROJECT_DIR}/outputs/hadoop/request_count_by_service.txt"
hadoop fs -cat "${HDFS_OUTPUT_BASE}/error_count/part-*" > "${PROJECT_DIR}/outputs/hadoop/server_error_count_by_service.txt"
hadoop fs -cat "${HDFS_OUTPUT_BASE}/slow_endpoint/part-*" > "${PROJECT_DIR}/outputs/hadoop/top10_slow_endpoints.txt"

echo "[OK] Hadoop Streaming outputs saved to outputs/hadoop/"
