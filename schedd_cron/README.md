# Schedd service monitoring

## What is it
The schedd service monitoring watches health metrics on the HTCondor schedd's used by Harvester. Current checks are:
- Workers in healthy (submitted, finished, running) and unhealthy (failed, held, cancelled) states submitted in the last 30 minutes
- Disk space availability
- Processes are running

## Basic operations
- Node: aipanda179
- Important folders:
  - [x] Work directory: */data/harvester_service_monitoring*
  - [x] Configuration: */data/harvester_service_monitoring/schedd_configuration* . Contains one XML file per submissionhost
  - [x] Internal SQLite cache: */data/harvester_service_monitoring/storage*
  - [x] Logging: */data/harvester_service_monitoring/logs*
  - [x] Cronjob: 
```
[root@aipanda179 tmp]# cat /etc/crontab
...
*/10 * * * * root /usr/bin/python /data/harvester_service_monitoring/schedd_monitoring.py > /dev/null 2>&1
```

## Installation

### Cronjob
```
-bash-4.2$ cat /etc/cron.d/schedd_hsm.cron
HOME=/home/atlpan

*/10 * * * * atlpan python /data/atlpan/schedd_service_monitoring/cron.py -s /data/atlpan/schedd_service_monitoring/cron_settings.ini > /dev/null 2>&1
```
