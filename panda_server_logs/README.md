# PanDA server log signatures

## What is it
A weekly cron job that reads the errors of one PanDA server log from OpenSearch
(`atlas_pandalogs-*`), groups them into signatures and emails a report.
Each log has its own section in the settings file and its own cron job.

A signature is `<method> | <error>`: the first word of the message, and the parsed error
code (e.g. `dbErrorCode`) or, without a code, the error text with numbers and quoted values
masked. This week is compared with the 3 weeks before it, read in the same query, so
nothing is stored between runs. A signature is flagged as:
- **new**: not seen in any of the previous weeks
- **spike**: this week >= `spike_factor` x the median of the previous weeks, and at least `spike_min_count`
- **always**: its error code is in `always_flag`

The email is sent every week, also when nothing is flagged. It contains the flagged,
active and ignored signatures with their weekly counts, and the latest occurrence of each
active signature with its full traceback, flagged ones first. It is an HTML email (the
signature IDs link to their samples) with a plain-text version for text-only clients.

## Ignored signatures
Expected errors go into the ignore file of the log (e.g. `ignore_dbproxy.ini`), with a
reason. They are excluded from the download and only counted, so they are cheap, and
they are only flagged when they spike. Copy the method and error from the report.
Keep the noisy signatures in this file: without it, the job downloads every document
of the 4 weeks (stopped at `max_documents`).

## Configuration
`settings.ini.template`, with the secrets filled in by teigi:
- `[esserver]`: OpenSearch account, the same as the other monitoring scripts
- `[email]`: recipients, mail server and sender
- one section per log, e.g. `[dbproxy]`

## Running
The email is sent with `harvester_service_monitoring/libs/notifications.py`, so the script
runs from the repository checkout. The ignore files are read from the script directory,
the settings file can be anywhere.
```
python3 panda_server_logs.py -s settings.ini -l dbproxy
```
- `--dry-run`: print the text version of the email instead of sending it
- `--html <file>`: also write the HTML version to a file, to look at it in a browser
- `--replay <file>`: use a saved `_search` response instead of querying OpenSearch

Logs go to `logs/panda_server_logs.log`. The exit code is 1 when the query or the email fails.

### Cronjob
Monday 08:00, one line per log:
```
0 8 * * 1 atlpan /opt/venv/bin/python3 /data/atlpan/harvester_monitoring/panda_server_logs/panda_server_logs.py -s /data/atlpan/panda_server_logs/settings.ini -l dbproxy > /dev/null 2>&1
```
