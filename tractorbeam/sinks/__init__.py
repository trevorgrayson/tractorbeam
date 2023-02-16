import logging
from apache_beam import Map
from apache_beam.io import FileBasedSink, WriteToPubSub
from urllib.parse import urlparse

SINK_PROTOCOLS = {
    'file': lambda r: FileBasedSink(r.path),
    'pubsub': lambda r: WriteToPubSub('projects/' + r.netloc + r.path),
    'dryrun': lambda r: Map(print)
}


def sink(url: str):
    resource = urlparse(url)
    logging.info(f"Sink Resource: {resource}")

    proto = SINK_PROTOCOLS.get(resource.scheme, SINK_PROTOCOLS['dryrun'])
    return proto(resource)
