# Harvester service monitoring

## What is it
The Harvester service monitoring runs as a cronjob and checks prefedined Harvester metrics available in Elastic Search. When the metrics exceed a threshold, alerts will be sent out by email to the responsible team.

## Current metrics
- Memory usage
- CPU usage
- Disk usage
- Last submission of workers
- Last heartbeat

## Basic operations
- Node: aipanda179
- Important folders:
  - [x] Work directory: */data/harvester_service_monitoring*
  - [x] Configuration: */data/harvester_service_monitoring/configuration* . Contains one XML file per harvester instance
  - [x] Internal SQLite cache: */data/harvester_service_monitoring/storage*
  - [x] Logging: */data/harvester_service_monitoring/logs*
  - [x] Cronjob: 
```
[root@aipanda179 tmp]# cat /etc/crontab
...
*/10 * * * * root /usr/bin/python /data/harvester_service_monitoring/harvester_monitoring.py > /dev/null 2>&1
```
### Configuration
**Add some examples for configuration. How to disable a particular metric?**
