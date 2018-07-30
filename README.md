# Hadoop Exporter for Prometheus
Exports hadoop metrics via HTTP for Prometheus consumption.

How to run
```
python hadoop_exporter.py -c CLUSTER_NAME
```

Help on flags of namenode_exporter:
```
usage: hadoop_exporter.py [-h] [-c cluster]
                          [-hdfs {http://10.110.13.163:8480/jmx,http://10.110.13.165:16010/jmx,http://10.110.13.164:8480/jmx,http://10.110.13.42:6188/jmx,htt
p://10.110.13.164:8088/jmx,http://10.110.13.164:1022/jmx,http://10.110.13.165:19888/jmx,http://10.110.13.165:8088/jmx,http://10.110.13.164:16010/jmx,http://1
0.110.13.163:1022/jmx,http://10.110.13.165:1022/jmx,http://10.110.13.165:50070/jmx,http://10.110.13.165:8480/jmx,http://10.110.13.164:50070/jmx}]
                          [-yarn {http://10.110.13.163:8480/jmx,http://10.110.13.165:16010/jmx,http://10.110.13.164:8480/jmx,http://10.110.13.42:6188/jmx,htt
p://10.110.13.164:8088/jmx,http://10.110.13.164:1022/jmx,http://10.110.13.165:19888/jmx,http://10.110.13.165:8088/jmx,http://10.110.13.164:16010/jmx,http://1
0.110.13.163:1022/jmx,http://10.110.13.165:1022/jmx,http://10.110.13.165:50070/jmx,http://10.110.13.165:8480/jmx,http://10.110.13.164:50070/jmx}]
                          [-dn {http://10.110.13.163:8480/jmx,http://10.110.13.165:16010/jmx,http://10.110.13.164:8480/jmx,http://10.110.13.42:6188/jmx,http:
//10.110.13.164:8088/jmx,http://10.110.13.164:1022/jmx,http://10.110.13.165:19888/jmx,http://10.110.13.165:8088/jmx,http://10.110.13.164:16010/jmx,http://10.
110.13.163:1022/jmx,http://10.110.13.165:1022/jmx,http://10.110.13.165:50070/jmx,http://10.110.13.165:8480/jmx,http://10.110.13.164:50070/jmx}]
                          [-jn {http://10.110.13.163:8480/jmx,http://10.110.13.165:16010/jmx,http://10.110.13.164:8480/jmx,http://10.110.13.42:6188/jmx,http:
//10.110.13.164:8088/jmx,http://10.110.13.164:1022/jmx,http://10.110.13.165:19888/jmx,http://10.110.13.165:8088/jmx,http://10.110.13.164:16010/jmx,http://10.
110.13.163:1022/jmx,http://10.110.13.165:1022/jmx,http://10.110.13.165:50070/jmx,http://10.110.13.165:8480/jmx,http://10.110.13.164:50070/jmx}]
                          [-mr {http://10.110.13.163:8480/jmx,http://10.110.13.165:16010/jmx,http://10.110.13.164:8480/jmx,http://10.110.13.42:6188/jmx,http:
//10.110.13.164:8088/jmx,http://10.110.13.164:1022/jmx,http://10.110.13.165:19888/jmx,http://10.110.13.165:8088/jmx,http://10.110.13.164:16010/jmx,http://10.
110.13.163:1022/jmx,http://10.110.13.165:1022/jmx,http://10.110.13.165:50070/jmx,http://10.110.13.165:8480/jmx,http://10.110.13.164:50070/jmx}]
                          [-hbase {http://10.110.13.163:8480/jmx,http://10.110.13.165:16010/jmx,http://10.110.13.164:8480/jmx,http://10.110.13.42:6188/jmx,ht
tp://10.110.13.164:8088/jmx,http://10.110.13.164:1022/jmx,http://10.110.13.165:19888/jmx,http://10.110.13.165:8088/jmx,http://10.110.13.164:16010/jmx,http://
10.110.13.163:1022/jmx,http://10.110.13.165:1022/jmx,http://10.110.13.165:50070/jmx,http://10.110.13.165:8480/jmx,http://10.110.13.164:50070/jmx}]
                          [-hive {http://10.110.13.163:8480/jmx,http://10.110.13.165:16010/jmx,http://10.110.13.164:8480/jmx,http://10.110.13.42:6188/jmx,htt
p://10.110.13.164:8088/jmx,http://10.110.13.164:1022/jmx,http://10.110.13.165:19888/jmx,http://10.110.13.165:8088/jmx,http://10.110.13.164:16010/jmx,http://1
0.110.13.163:1022/jmx,http://10.110.13.165:1022/jmx,http://10.110.13.165:50070/jmx,http://10.110.13.165:8480/jmx,http://10.110.13.164:50070/jmx}]
                          [-p metrics_path] [-host ADDRESS] [-P port]

hadoop node exporter args, including url, metrics_path, address, port and
cluster.

optional arguments:
  -h, --help            show this help message and exit
  -c cluster, --cluster cluster
                        Hadoop cluster labels.
  -hdfs {http://10.110.13.163:8480/jmx,http://10.110.13.165:16010/jmx,http://10.110.13.164:8480/jmx,http://10.110.13.42:6188/jmx,http://10.110.13.164:8088/jm
x,http://10.110.13.164:1022/jmx,http://10.110.13.165:19888/jmx,http://10.110.13.165:8088/jmx,http://10.110.13.164:16010/jmx,http://10.110.13.163:1022/jmx,htt
p://10.110.13.165:1022/jmx,http://10.110.13.165:50070/jmx,http://10.110.13.165:8480/jmx,http://10.110.13.164:50070/jmx}, --namenode-url {http://10.110.13.163
:8480/jmx,http://10.110.13.165:16010/jmx,http://10.110.13.164:8480/jmx,http://10.110.13.42:6188/jmx,http://10.110.13.164:8088/jmx,http://10.110.13.164:1022/j
mx,http://10.110.13.165:19888/jmx,http://10.110.13.165:8088/jmx,http://10.110.13.164:16010/jmx,http://10.110.13.163:1022/jmx,http://10.110.13.165:1022/jmx,ht
tp://10.110.13.165:50070/jmx,http://10.110.13.165:8480/jmx,http://10.110.13.164:50070/jmx}
                        Hadoop hdfs metrics URL. (default
                        "http://ip:port/jmx")
  -yarn {http://10.110.13.163:8480/jmx,http://10.110.13.165:16010/jmx,http://10.110.13.164:8480/jmx,http://10.110.13.42:6188/jmx,http://10.110.13.164:8088/jm
x,http://10.110.13.164:1022/jmx,http://10.110.13.165:19888/jmx,http://10.110.13.165:8088/jmx,http://10.110.13.164:16010/jmx,http://10.110.13.163:1022/jmx,htt
p://10.110.13.165:1022/jmx,http://10.110.13.165:50070/jmx,http://10.110.13.165:8480/jmx,http://10.110.13.164:50070/jmx}, --resourcemanager-url {http://10.110
.13.163:8480/jmx,http://10.110.13.165:16010/jmx,http://10.110.13.164:8480/jmx,http://10.110.13.42:6188/jmx,http://10.110.13.164:8088/jmx,http://10.110.13.164
:1022/jmx,http://10.110.13.165:19888/jmx,http://10.110.13.165:8088/jmx,http://10.110.13.164:16010/jmx,http://10.110.13.163:1022/jmx,http://10.110.13.165:1022
/jmx,http://10.110.13.165:50070/jmx,http://10.110.13.165:8480/jmx,http://10.110.13.164:50070/jmx}
                        Hadoop yarn metrics URL. (default
                        "127.0.0.1:8088/jmx")
  -dn {http://10.110.13.163:8480/jmx,http://10.110.13.165:16010/jmx,http://10.110.13.164:8480/jmx,http://10.110.13.42:6188/jmx,http://10.110.13.164:8088/jmx,
http://10.110.13.164:1022/jmx,http://10.110.13.165:19888/jmx,http://10.110.13.165:8088/jmx,http://10.110.13.164:16010/jmx,http://10.110.13.163:1022/jmx,http:
//10.110.13.165:1022/jmx,http://10.110.13.165:50070/jmx,http://10.110.13.165:8480/jmx,http://10.110.13.164:50070/jmx}, --datanode-url {http://10.110.13.163:8
480/jmx,http://10.110.13.165:16010/jmx,http://10.110.13.164:8480/jmx,http://10.110.13.42:6188/jmx,http://10.110.13.164:8088/jmx,http://10.110.13.164:1022/jmx
,http://10.110.13.165:19888/jmx,http://10.110.13.165:8088/jmx,http://10.110.13.164:16010/jmx,http://10.110.13.163:1022/jmx,http://10.110.13.165:1022/jmx,http
://10.110.13.165:50070/jmx,http://10.110.13.165:8480/jmx,http://10.110.13.164:50070/jmx}
                        Hadoop datanode metrics URL. (default
                        "http://ip:port/jmx")
  -jn {http://10.110.13.163:8480/jmx,http://10.110.13.165:16010/jmx,http://10.110.13.164:8480/jmx,http://10.110.13.42:6188/jmx,http://10.110.13.164:8088/jmx,
http://10.110.13.164:1022/jmx,http://10.110.13.165:19888/jmx,http://10.110.13.165:8088/jmx,http://10.110.13.164:16010/jmx,http://10.110.13.163:1022/jmx,http:
//10.110.13.165:1022/jmx,http://10.110.13.165:50070/jmx,http://10.110.13.165:8480/jmx,http://10.110.13.164:50070/jmx}, --journalnode-url {http://10.110.13.16
3:8480/jmx,http://10.110.13.165:16010/jmx,http://10.110.13.164:8480/jmx,http://10.110.13.42:6188/jmx,http://10.110.13.164:8088/jmx,http://10.110.13.164:1022/
jmx,http://10.110.13.165:19888/jmx,http://10.110.13.165:8088/jmx,http://10.110.13.164:16010/jmx,http://10.110.13.163:1022/jmx,http://10.110.13.165:1022/jmx,h
ttp://10.110.13.165:50070/jmx,http://10.110.13.165:8480/jmx,http://10.110.13.164:50070/jmx}
                        Hadoop datanode metrics URL. (default
                        "http://ip:port/jmx")
  -mr {http://10.110.13.163:8480/jmx,http://10.110.13.165:16010/jmx,http://10.110.13.164:8480/jmx,http://10.110.13.42:6188/jmx,http://10.110.13.164:8088/jmx,
http://10.110.13.164:1022/jmx,http://10.110.13.165:19888/jmx,http://10.110.13.165:8088/jmx,http://10.110.13.164:16010/jmx,http://10.110.13.163:1022/jmx,http:
//10.110.13.165:1022/jmx,http://10.110.13.165:50070/jmx,http://10.110.13.165:8480/jmx,http://10.110.13.164:50070/jmx}, --mapreduce2-url {http://10.110.13.163
:8480/jmx,http://10.110.13.165:16010/jmx,http://10.110.13.164:8480/jmx,http://10.110.13.42:6188/jmx,http://10.110.13.164:8088/jmx,http://10.110.13.164:1022/j
mx,http://10.110.13.165:19888/jmx,http://10.110.13.165:8088/jmx,http://10.110.13.164:16010/jmx,http://10.110.13.163:1022/jmx,http://10.110.13.165:1022/jmx,ht
tp://10.110.13.165:50070/jmx,http://10.110.13.165:8480/jmx,http://10.110.13.164:50070/jmx}
                        Hadoop datanode metrics URL. (default
                        "http://ip:port/jmx")
  -hbase {http://10.110.13.163:8480/jmx,http://10.110.13.165:16010/jmx,http://10.110.13.164:8480/jmx,http://10.110.13.42:6188/jmx,http://10.110.13.164:8088/j
mx,http://10.110.13.164:1022/jmx,http://10.110.13.165:19888/jmx,http://10.110.13.165:8088/jmx,http://10.110.13.164:16010/jmx,http://10.110.13.163:1022/jmx,ht
tp://10.110.13.165:1022/jmx,http://10.110.13.165:50070/jmx,http://10.110.13.165:8480/jmx,http://10.110.13.164:50070/jmx}, --hbase-url {http://10.110.13.163:8
480/jmx,http://10.110.13.165:16010/jmx,http://10.110.13.164:8480/jmx,http://10.110.13.42:6188/jmx,http://10.110.13.164:8088/jmx,http://10.110.13.164:1022/jmx
,http://10.110.13.165:19888/jmx,http://10.110.13.165:8088/jmx,http://10.110.13.164:16010/jmx,http://10.110.13.163:1022/jmx,http://10.110.13.165:1022/jmx,http
://10.110.13.165:50070/jmx,http://10.110.13.165:8480/jmx,http://10.110.13.164:50070/jmx}
                        Hadoop datanode metrics URL. (default
                        "http://ip:port/jmx")
  -hive {http://10.110.13.163:8480/jmx,http://10.110.13.165:16010/jmx,http://10.110.13.164:8480/jmx,http://10.110.13.42:6188/jmx,http://10.110.13.164:8088/jm
x,http://10.110.13.164:1022/jmx,http://10.110.13.165:19888/jmx,http://10.110.13.165:8088/jmx,http://10.110.13.164:16010/jmx,http://10.110.13.163:1022/jmx,htt
p://10.110.13.165:1022/jmx,http://10.110.13.165:50070/jmx,http://10.110.13.165:8480/jmx,http://10.110.13.164:50070/jmx}, --hive-url {http://10.110.13.163:848
0/jmx,http://10.110.13.165:16010/jmx,http://10.110.13.164:8480/jmx,http://10.110.13.42:6188/jmx,http://10.110.13.164:8088/jmx,http://10.110.13.164:1022/jmx,h
ttp://10.110.13.165:19888/jmx,http://10.110.13.165:8088/jmx,http://10.110.13.164:16010/jmx,http://10.110.13.163:1022/jmx,http://10.110.13.165:1022/jmx,http:/
/10.110.13.165:50070/jmx,http://10.110.13.165:8480/jmx,http://10.110.13.164:50070/jmx}
                        Hadoop datanode metrics URL. (default
                        "http://ip:port/jmx")
  -p metrics_path, --path metrics_path
                        Path under which to expose metrics. (default
                        "/metrics")
  -host ADDRESS, -ip ADDRESS, --address ADDRESS, --addr ADDRESS
                        Polling server on this address. (default "127.0.0.1")
  -P port, --port port  Listen to this port. (default "9130")
```


Tested on Apache Hadoop 2.7.3
# hadoop_exporter
