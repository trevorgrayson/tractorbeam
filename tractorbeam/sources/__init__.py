import logging
from apache_beam.io import ReadFromText, ReadFromPubSub
from urllib.parse import urlparse

SOURCE_PROTOCOLS = {
    'file': lambda r: ReadFromText(r.path),
    'pubsub': lambda r: ReadFromPubSub(subscription="projects/" + r.netloc + r.path)
}


def source(url: str):
    resource = urlparse(url)
    logging.info(f"Source Resource: {resource}")

    protocol = SOURCE_PROTOCOLS.get(resource.scheme)
    if protocol:
        return protocol(resource)
