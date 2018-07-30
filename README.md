# Hadoop Exporter for Prometheus
Exports hadoop metrics via HTTP for Prometheus consumption.

How to run
```
python resourcemanager_exporter.py
python namenode_exporter.py
```

Help on flags of namenode_exporter:
```
usage: namenode_exporter.py [-h] [-url url] [--telemetry-path telemetry_path]
                            [-p port] --cluster cluster

optional arguments:
  -h, --help            show this help message and exit
  -url url, --namenode.jmx.url url
                        Hadoop NameNode JMX URL. (default
                        "http://localhost:50070/jmx")
  --telemetry-path telemetry_path
                        Path under which to expose metrics. (default
                        "/metrics")
  -p port, --port port  Listen to this port. (default ":9088")
  --cluster cluster     label for cluster
```

Help on flags of resourcemanager_exporter:
```
usage: resourcemanager_exporter.py [-h] [-url url]
                                   [--telemetry-path telemetry_path] [-p port]
                                   --cluster cluster

optional arguments:
  -h, --help            show this help message and exit
  -url url, --resourcemanager.url url
                        Hadoop ResourceManager URL. (default
                        "http://localhost:8088")
  --telemetry-path telemetry_path
                        Path under which to expose metrics. (default
                        "/metrics")
  -p port, --port port  Listen to this port. (default ":9088")
  --cluster cluster     label for cluster
```

Tested on Apache Hadoop 2.7.3
# hadoop_exporter
