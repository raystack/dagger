# API Monitoring

API Monitoring refers to the practice of monitoring APIs in production to gain visibility into performance, availability and functional correctness. You want to get some insights about the metrics in real-time as soon as possible and possibly have some alerting mechanisms for unusual behaviour.

## How Dagger Solves?

If you look closely, API health/uptime/performance is mostly a _real-time streaming aggregation problem_ and hence Dagger can be a tool of choice. The only prerequisite is that you need some sort of structured events to be streamed for each API calls. The events can be streamed by the middleware/API gateway layer with all sort of information.

This is sample Data flow of API monitoring in Dagger.
![API Monitoring](/img/api-monitoring.png)

### Sample Schema Definition

This is a sample schema of API logs.

```protobuf
message SampleAPILog {
    google.protobuf.Timestamp event_timestamp = 1;
    int32 http_status_code = 2;
    string api_method = 13;
    string api_id = 14;
    string api_uri = 15;
    string api_name = 16;
    string api_upstream_url = 17;
}
```

### Sample Query in Dagger

We want to aggregate APIs on status codes. This will help us bucket APIs which are responding 4XX or 5XX and possibly have some issues.

```SQL
SELECT
  api_name AS api_name,
  api_uri AS api_uri,
  api_method AS api_method,
  Cast(http_status_code AS BIGINT) as http_status_code,
  count(1) as request_count,
  Tumble_end(rowtime, INTERVAL '60' second) AS event_timestamp
FROM
  `api_logs`
GROUP BY
  api_name,
  api_uri,
  http_status_code,
  api_method,
  Tumble (rowtime, INTERVAL '60' second)
```

This is a Dashboard on a sample API based on the status code powered by Dagger.

![Profile Enrichment](/img/api-status.png)

### Impact

- Since Dagger support influx as one of the sinks, user can easily set up alerts and visualise API performance on some visualization tools in near real-time.
- Apart from API uptime all sort of other performance details per API like endpoint latency, proxy latency can be aggregated by Dagger.
- Apart from simple health monitoring you can do a lot of complex streaming analysis on the middleware logs. Since Dagger scales in an instant, the scale of API event logs should not be an issue.
