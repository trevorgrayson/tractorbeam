from tractorbeam import sink
from tempfile import mktemp
from pytest import fixture

from apache_beam.io import WriteToPubSub, WriteToText


class TestSink:
    @fixture
    def tmpfile(self):
        tmpfile = mktemp()
        fp = open(tmpfile, 'w')
        fp.write("")
        fp.close()

        return tmpfile

    def test_file(self, tmpfile):

        s = sink(f"file://{tmpfile}")
        assert isinstance(s, WriteToText)

    def test_file(self):
        s = sink(f"pubsub://project/topics/sname")
        assert isinstance(s, WriteToPubSub)
