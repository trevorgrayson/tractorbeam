from apache_beam import Create
from apache_beam.testing.test_pipeline import TestPipeline as PipelineTester
from apache_beam.testing.util import assert_that, equal_to

from pipelines.example import example_pipeline


WORDS = ['one', 'two', 'three']


class TestExamplePipeline:
    def test_example(self):
        with PipelineTester() as p:
            input = p | Create(WORDS)
            output = input | example_pipeline

            assert_that(
                output,
                equal_to([
                    ">one<===",
                    ">two<===",
                    ">three<==="
                ])
            )
