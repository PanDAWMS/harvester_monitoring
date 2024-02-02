# Harvester service monitoring

## What is it
The Harvester service monitoring runs as a cronjob and checks prefedined Harvester metrics available in Elastic Search. When the metrics exceed a threshold, alerts will be sent out by email to the responsible team.

## Current metrics
- Memory usage
- CPU usage
- Disk usage
- Last submission of workers
- Fraction of workers in bad states, e.g. missed
- Last heartbeat

## Basic operations
- Node: aipanda110
- Important folders:
  - [x] Work directory: */opt/harvester_monitoring/harvester_service_monitoring/*
  - [x] Configuration: */opt/harvester_monitoring/harvester_service_monitoring/configuration* . Contains one XML file per harvester instance
  - [x] Internal SQLite cache: */opt/harvester_monitoring/harvester_service_monitoring/storage*
  - [x] Logging: */opt/harvester_monitoring/harvester_service_monitoring/logs*
  - [x] Cronjob: 
```
[root@aipanda110 tmp]# cat /etc/crontab
...
*/10 * * * * root /usr/bin/python /data/harvester_service_monitoring/harvester_monitoring.py > /dev/null 2>&1
```
### Configuration
XML-template of configuration for Harvester service monitoring:
```
<?xml version="1.0"?>
<instances>
    <instance harvesterid="CERN_central_X" instanceisenable="True">
        <hostlist>
            <host hostname="aipandaXXX.cern.ch" hostisenable="True">
                <contacts>
                    <email>forcontact@mail.com</email>
                    <email>forcontact@mail.com</email>
                </contacts>
                <memory>6000</memory>
                <metrics warning_delay="6h" critical_delay="6h">
                    <metric name="lastsubmittedworker" enable="True">
                        <value>60d</value>
                    </metric>
                    <metric name="lastheartbeat" enable="True">
                        <value>30</value>
                    </metric>
                    <metric name="memory" enable="True">
                        <memory_warning>50</memory_warning>
                        <memory_critical>80</memory_critical>
                    </metric>
                    <metric name="cpu" enable="True">
                        <cpu_warning>50</cpu_warning>
                        <cpu_critical>80</cpu_critical>
                    </metric>
                    <metric name="disk" enable="True">
                        <disk_warning>70</disk_warning>
                        <disk_critical>80</disk_critical>
                    </metric>
                </metrics>
            </host>
        </hostlist>
    </instance>
</instances>
```
For disabling instances, hosts and metrics use the following properties:
- For instance tags. instanceisenable="False"
- For host tags. hostisenable="False"
- For metric tags. enable="False"

