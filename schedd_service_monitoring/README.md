# Schedd service monitoring

## What is it
The schedd service monitoring watches health metrics on the HTCondor schedd's used by Harvester. Current checks are:
- Workers in healthy (submitted, finished, running) and unhealthy (failed, held, cancelled) states submitted in the last 30 minutes
- Disk space availability
- Processes are running

## Installation

### Cronjob
```
-bash-4.2$ cat /etc/cron.d/schedd_hsm.cron
HOME=/home/atlpan

*/10 * * * * atlpan python /data/atlpan/schedd_service_monitoring/cron.py -s /data/atlpan/schedd_service_monitoring/cron_settings.ini > /dev/null 2>&1
```
