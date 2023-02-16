from tractorbeam import source
from tempfile import mktemp
from pytest import fixture

from apache_beam.io import ReadFromText, ReadFromPubSub


class TestSource:
    @fixture
    def tmpfile(self):
        tmpfile = mktemp()
        fp = open(tmpfile, 'w')
        fp.write("")
        fp.close()

        return tmpfile

    def test_file(self, tmpfile):

        s = source(f"file://{tmpfile}")
        assert isinstance(s, ReadFromText)

    def test_file(self):
        s = source(f"pubsub://project/subscriptions/sname")
        assert isinstance(s, ReadFromPubSub)
