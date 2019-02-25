# Harvester service monitoring

## What is it
The Harvester service monitoring runs as a cronjob and checks prefedined Harvester metrics available in Elastic Search. When the metrics exceed a threshold, alerts will be sent out by email to the responsible team.

## Current metrics
- Memory usage
- CPU usage
- Disk usage
- Last submission of workers

## Operating it
- Node and work directory: aipanda179:/data/harvester_service_monitoring
- Important folders:
  - [x] Configuration: ./configuration. Contains one XML file per harvester instance
  - [ ] Logging: there is currently no logging
  - [ ] Cron: where is the cron running?
