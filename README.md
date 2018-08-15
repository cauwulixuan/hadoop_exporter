# Hadoop Exporter for Prometheus
Exports hadoop metrics via HTTP for Prometheus consumption.

How to run
```
python hadoop_exporter.py
```

Help on flags of hadoop_exporter:
```
$ python hadoop_exporter.py -h
usage: hadoop_exporter.py [-h] [-c cluster_name] [-hdfs namenode_jmx_url]
                          [-rm resourcemanager_jmx_url] [-dn datanode_jmx_url]
                          [-jn journalnode_jmx_url] [-mr mapreduce2_jmx_url]
                          [-hbase hbase_jmx_url] [-hive hive_jmx_url]
                          [-p metrics_path] [-host ip_or_hostname] [-P port]

hadoop node exporter args, including url, metrics_path, address, port and
cluster.

optional arguments:
  -h, --help            show this help message and exit
  -c cluster_name, --cluster cluster_name
                        Hadoop cluster labels. (default "cluster_indata")
  -hdfs namenode_jmx_url, --namenode-url namenode_jmx_url
                        Hadoop hdfs metrics URL. (default
                        "http://indata-10-110-13-165.indata.com:50070/jmx")
  -rm resourcemanager_jmx_url, --resourcemanager-url resourcemanager_jmx_url
                        Hadoop resourcemanager metrics URL. (default
                        "http://indata-10-110-13-164.indata.com:8088/jmx")
  -dn datanode_jmx_url, --datanode-url datanode_jmx_url
                        Hadoop datanode metrics URL. (default
                        "http://indata-10-110-13-163.indata.com:1022/jmx")
  -jn journalnode_jmx_url, --journalnode-url journalnode_jmx_url
                        Hadoop journalnode metrics URL. (default
                        "http://indata-10-110-13-163.indata.com:8480/jmx")
  -mr mapreduce2_jmx_url, --mapreduce2-url mapreduce2_jmx_url
                        Hadoop mapreduce2 metrics URL. (default
                        "http://indata-10-110-13-165.indata.com:19888/jmx")
  -hbase hbase_jmx_url, --hbase-url hbase_jmx_url
                        Hadoop hbase metrics URL. (default
                        "http://indata-10-110-13-164.indata.com:16010/jmx")
  -hive hive_jmx_url, --hive-url hive_jmx_url
                        Hadoop hive metrics URL. (default
                        "http://ip:port/jmx")
  -p metrics_path, --path metrics_path
                        Path under which to expose metrics. (default
                        "/metrics")
  -host ip_or_hostname, -ip ip_or_hostname, --address ip_or_hostname, --addr ip_or_hostname
                        Polling server on this address. (default "127.0.0.1")
  -P port, --port port  Listen to this port. (default "9130")
```


Tested on Apache Hadoop 2.7.3
# hadoop_exporter
