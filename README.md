# pglog-fetcher
Fetches log from AWS RDS.

Each time it fetches new log entries, a new file
containing the downloaded content is saved.

To configure credentials, see [go aws sdk docs](https://github.com/aws/aws-sdk-go#configuring-credentials).
