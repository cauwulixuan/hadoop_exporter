from prometheus_client import start_http_server, Summary, Histogram
import random
import time

'''
# Create a metric to track time spent and requests made.
a = Histogram('test_name', 'Time spent test name')
a.observe(1)

# Decorate function with metric.
@a.time()
def process_request(t):
    """A dummy function that takes some time."""
    time.sleep(t)

if __name__ == '__main__':
    # Start up the server to expose the metrics.
    try:
        start_http_server(8000)
        from consul import Consul
        c = Consul(host='10.110.13.216')
        # Register Service
        c.agent.service.register('hadoop_python_test111',
                                 service_id='consul_python_test111',
                                 address='10.9.11.95',
                                 port=8000,
                                 tags=['hadoop'])
        print("Polling %s. Serving at port: %s" % ('10.9.11.95', '8000'))
        # Generate some requests.
        while True:
            process_request(random.random())
    except:
        c.agent.service.deregister(service_id='consul_python_test111')
        print(" Interrupted")
        exit(0)
'''
from prometheus_client.core import GaugeMetricFamily, CounterMetricFamily, HistogramMetricFamily, REGISTRY

class CustomCollector(object):
    def collect(self):
        yield GaugeMetricFamily('my_gauge', 'Help text', value=7)
        c = CounterMetricFamily('my_counter_total', 'Help text', labels=['foo'])
        c.add_metric(['bar'], 1.7)
        c.add_metric(['baz'], 3.8)
        yield c
        h = HistogramMetricFamily('my_histogram', 'Help text', labels = ['handler'])
        h.add_metric(['prometheus'], buckets=[('.025',11), ('.05',12), ('.075',13), ('.1',14), ('.25',15), ('.5',16), ('.75',17), ('1.0',18), ('+Inf', 19)], sum_value=135)
        yield h

def main():
    from consul import Consul
    c = Consul(host='10.110.13.216')
    # Register Service
    port = 8002
    REGISTRY.register(CustomCollector())
    c.agent.service.register('hadoop_python_test2323',
                             service_id='consul_python_test2323',
                             address='10.9.11.95',
                             port=port,
                             tags=['hadoop'])
    start_http_server(port)
    print("Serving at port: %s" % (port))
    while True:
        time.sleep(1)

if __name__ == '__main__':
    main()