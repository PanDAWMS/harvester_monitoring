# PanDA server log signatures

## What is it
A weekly cron job that reads the errors of one PanDA server log from OpenSearch
(`atlas_pandalogs-*`), groups them into signatures and posts a summary to Mattermost.
Each log has its own section in the settings file and its own cron job.

A signature is `<method> | <error>`: the first word of the message, and the parsed error
code (e.g. `dbErrorCode`) or, without a code, the error text with numbers and quoted values
masked. This week is compared with the 3 weeks before it, read in the same query, so
nothing is stored between runs. A signature is flagged as:
- **new**: not seen in any of the previous weeks
- **spike**: this week >= `spike_factor` x the median of the previous weeks, and at least `spike_min_count`
- **always**: its error code is in `always_flag`

The message lists the flagged signatures with their latest occurrence, or says that
nothing is new. The full table of the week's signatures is in the message details (card).

## Ignored signatures
Expected errors go into the ignore file of the log (e.g. `ignore_dbproxy.ini`), with a
reason. They are excluded from the download and only counted, so they are cheap, and
they are only flagged when they spike. Copy the method and error from the report.
Keep the noisy signatures in this file: without it, the job downloads every document
of the 4 weeks (stopped at `max_documents`).

## Configuration
`settings.ini.template`, with the secrets filled in by teigi:
- `[esserver]`: OpenSearch account, the same as the other monitoring scripts
- `[mattermost]`: incoming webhook URL
- one section per log, e.g. `[dbproxy]`

## Running
```
python3 panda_server_logs.py -s settings.ini -l dbproxy
```
- `--dry-run`: print the message instead of posting it
- `--replay <file>`: use a saved `_search` response instead of querying OpenSearch

Logs go to `logs/panda_server_logs.log`. The exit code is 1 when the query or the post fails.

### Cronjob
Monday 08:00, one line per log:
```
0 8 * * 1 atlpan /opt/venv/bin/python3 /data/atlpan/panda_server_logs/panda_server_logs.py -s /data/atlpan/panda_server_logs/settings.ini -l dbproxy > /dev/null 2>&1
```
