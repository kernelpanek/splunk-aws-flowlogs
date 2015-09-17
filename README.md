# splunk-aws-flowlogs

## Usage
 - Works with Python 2.7+

### Running directly in the command line

```bash
./consumer.sh -t 00:01:00 -w 8 -i 6 -f mylogdata.txt -c /home/me/.aws/config
```
(Using the AWS config file, script retrieves from all accounts in config file the past _1_ minute of data with _8_ workers, _6_ second intervals on writing incoming results to the file, _mylogdata.txt_)

### Running in Splunk
 - Copy files to {SPLUNK_HOME}/etc/apps/scripts/bin/flowlogs/
 - Create a new script data input
 - Set Command to "{SPLUNK_HOME}/etc/apps/scripts/bin/flowlogs/consumer.sh"
 - Optionally add switches for control over time span, number of workers, data writer interval
 - DO NOT SET -f switch!

## Notes

This script uses Python's multiprocessing module. The script uses 1 data writer process and N worker processes.
Here is a breakdown of steps:
 * Gather CLI arguments and initialize logging
 * Initialize the pending work queue by enumerating all flowlog log groups and streams within those groups
 * Start worker processes
 * Start data writer process
 * Worker processes will:
   - pull items off the pending queue
   - perform work
   - save work to completed work queue
   - write to pending queue if nextTokens are present (or if exception occurs)
 * Data writer process will:
   - pull items off the completed work queue
   - write to stdout/file

## Problems / TODOs / Considerations for improvement

The reason for using multiprocessing is to capitalize on network I/O. The botocore module will handle the rate limit exceptions/retries, but using too many workers will result in longer completion times as work will be re-queued, therefore extending the length of time to complete.

Multi-account processing iterates over the profiles found within the configuration file. One account is handled at a time.
