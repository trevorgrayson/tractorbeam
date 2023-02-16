from argparse import ArgumentParser

from apache_beam import Pipeline
from apache_beam.options.pipeline_options import PipelineOptions

from . import source, sink, get_pipe

parser = ArgumentParser(description="configurable beam pipelines")
parser.add_argument("--from", dest="source",
                    help="")
parser.add_argument("--to",
                    help="")
parser.add_argument("--pipeline",
                    help="intermediary beam pipeline to transform data")
parser.add_argument("--setup",
                    help="setup.py file for installing on nodes")
args = parser.parse_args()

opts = {}
if args.setup:
    opts['setup_file'] = args.setup
options = PipelineOptions(**opts)
pipeline = Pipeline(options=options)

# Source
pipe = pipeline | "Source" >> source(args.source)

# Transform
if args.pipeline:
    pipe = pipe | get_pipe(args.pipeline)

# Sink, defaults to print
pipe | "Sink" >> sink(args.to)

pipeline.run()
