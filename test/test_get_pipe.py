from tractorbeam import get_pipe

from apache_beam import PTransform


class TestGetPipe:
    def test_get_pipe(self):
        pipe = get_pipe("pipelines.example.example_pipeline")
        assert isinstance(pipe, PTransform)
