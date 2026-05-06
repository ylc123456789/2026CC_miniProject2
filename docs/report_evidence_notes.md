# Report Evidence Notes

Use this file to prepare report evidence. Remove or anonymise identifying information before submission.

## Object storage evidence

Include one cropped screenshot or terminal output showing that the dataset object exists in Alibaba Cloud OSS.

Do not reveal:

- Bucket name
- Account name
- AccessKey ID or secret
- Full user path
- Student name or ID
- Group ID

## MapReduce evidence

Include selected output rows, not full logs.

Example explanation for `request_count_by_service`:

Input record:

```text
2026-04-10T08:00:00Z,R43318,U0915,search-service,/search/autocomplete,GET,200,823,eu-west,
```

Mapper emits:

```text
search-service\t1
```

Reducer receives all values with the key `search-service` and sums the values to produce the final request count.

## Ray evidence

Mention that the Ray script uses `@ray.remote` in `ray/degraded_service_detection.py`. Each task processes one partition and returns partial metrics:

- total requests
- slow requests
- server errors
- Timeout errors

The driver combines the partial metrics and applies degraded-service rules.

## Validation evidence

A concrete validation example can use the service metrics output.

For example, if `payment-service` has:

```text
total_requests = 7914
slow_requests = 2134
server_errors = 1362
timeout_errors = 664
```

Then:

```text
slow_rate = 2134 / 7914 = 26.96% > 20%
server_error_rate = 1362 / 7914 = 17.21% > 10%
timeout_errors = 664 >= 5
```

Therefore, `payment-service` is degraded for multiple reasons.

## Runtime evidence

Use `outputs/runtime_results.txt` as the starting point. If you run on ECS/EMR, add the cloud environment runtime as the main report evidence.
