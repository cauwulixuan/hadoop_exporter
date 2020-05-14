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

# Usage
You can run each `Collector` under directory `cmd/`, just like:
```
cd hadoop_exporter/cmd
python hdfs_namenode.py  -h
# input the params the script asked.
```
Or if you want to run the entire project, you should have an webhook/api with url = `http://<rest_api_host_and_port>/alert/getservicesbyhost` to provide the jmx urls: the content in the webhook or api should be like:
```
{
    "cluster_name1": [
        {
            "node1.fqdn.com": {
                "DATANODE": {
                    "jmx": "http://node1.fqdn.com:1022/jmx"
                },
                "HBASE_REGIONSERVER": {
                    "jmx": "http://node1.fqdn.com:60030/jmx"
                },
                "HISTORYSERVER": {
                    "jmx": "node1.fqdn.com:19888/jmx"
                },
                "JOURNALNODE": {
                    "jmx": "node1.fqdn.com:8480/jmx"
                },
                "NAMENODE": {
                    "jmx": "node1.fqdn.com:50070/jmx"
                },
                "NODEMANAGER": {
                    "jmx": "node1.fqdn.com:8042/jmx"
                }
            }
        },
        {
            "node2.fqdn.com": {
                "DATANODE": {
                    "jmx": "http://node2.fqdn.com:1022/jmx"
                },
                "HBASE_REGIONSERVER": {
                    "jmx": "http://node2.fqdn.com:60030/jmx"
                },
                "HIVE_LLAP": {
                    "jmx": "http://node2.fqdn.com:15002/jmx"
                },
                "HIVE_SERVER_INTERACTIVE": {
                    "jmx": "http://node2.fqdn.com:10502/jmx"
                },
                "JOURNALNODE": {
                    "jmx": "http://node2.fqdn.com:8480/jmx"
                },
                "NODEMANAGER": {
                    "jmx": "http://node2.fqdn.com:8042/jmx"
                }
            }
        },
        {
            "node3.fqdn.com": {
                "DATANODE": {
                    "jmx": "http://node3.fqdn.com:1022/jmx"
                },
                "HBASE_MASTER": {
                    "jmx": "http://node3.fqdn.com:16010/jmx"
                },
                "HBASE_REGIONSERVER": {
                    "jmx": "http://node3.fqdn.com:60030/jmx"
                },
                "JOURNALNODE": {
                    "jmx": "http://node3.fqdn.com:8480/jmx"
                },
                "NODEMANAGER": {
                    "jmx": "http://node3.fqdn.com:8042/jmx"
                },
                "RESOURCEMANAGER": {
                    "jmx": "http://node3.fqdn.com:8088/jmx"
                }
            }
        }
    ],
    "cluster_name2": [
        {
            "node4.fqdn.com": {
                "DATANODE": {
                    "jmx": "http://node4.fqdn.com:1022/jmx"
                },
                "HBASE_REGIONSERVER": {
                    "jmx": "http://node4.fqdn.com:60030/jmx"
                },
                "HISTORYSERVER": {
                    "jmx": "node4.fqdn.com:19888/jmx"
                },
                "JOURNALNODE": {
                    "jmx": "node4.fqdn.com:8480/jmx"
                },
                "NAMENODE": {
                    "jmx": "node4.fqdn.com:50070/jmx"
                },
                "NODEMANAGER": {
                    "jmx": "node4.fqdn.com:8042/jmx"
                }
            }
        },
        {
            "node5.fqdn.com": {
                "DATANODE": {
                    "jmx": "http://node5.fqdn.com:1022/jmx"
                },
                "HBASE_REGIONSERVER": {
                    "jmx": "http://node5.fqdn.com:60030/jmx"
                },
                "HIVE_LLAP": {
                    "jmx": "http://node5.fqdn.com:15002/jmx"
                },
                "HIVE_SERVER_INTERACTIVE": {
                    "jmx": "http://node5.fqdn.com:10502/jmx"
                },
                "JOURNALNODE": {
                    "jmx": "http://node5.fqdn.com:8480/jmx"
                },
                "NODEMANAGER": {
                    "jmx": "http://node5.fqdn.com:8042/jmx"
                }
            }
        },
        {
            "node6.fqdn.com": {
                "DATANODE": {
                    "jmx": "http://node6.fqdn.com:1022/jmx"
                },
                "HBASE_MASTER": {
                    "jmx": "http://node6.fqdn.com:16010/jmx"
                },
                "HBASE_REGIONSERVER": {
                    "jmx": "http://node6.fqdn.com:60030/jmx"
                },
                "JOURNALNODE": {
                    "jmx": "http://node6.fqdn.com:8480/jmx"
                },
                "NODEMANAGER": {
                    "jmx": "http://node6.fqdn.com:8042/jmx"
                },
                "RESOURCEMANAGER": {
                    "jmx": "http://node6.fqdn.com:8088/jmx"
                }
            }
        }
    ]
}
```
Then you can run:
```
# -s means the rest api or webhook url mentioned above, should be in <host:port> format, no schema and path( I know it's ugly).
# -P (upper) means hadoop_exporter should export metrics in this port. you can get metrics from <http://hostname:9131/metrics>
python hadoop_exporter.py -s "<rest_api_host_and_port>" -P 9131
```

**One more thing**:
you should run all this steps in **all hadoop nodes**.

MAYBE I'll improve this project for common use.
