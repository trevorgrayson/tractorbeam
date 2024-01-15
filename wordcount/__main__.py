#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# source: https://beam.apache.org/get-started/wordcount-example/
#
import argparse
import logging


import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

from . import pipeline

logging.getLogger().setLevel(logging.INFO)
"""Main entry point; defines and runs the wordcount pipeline."""

parser = argparse.ArgumentParser()
parser.add_argument(
  '--input',
  dest='input',
  help='Input file to process.')
parser.add_argument(
  '--output',
  dest='output',
  default='pipeline.out',
  help='Output file to write results to.')
known_args, pipeline_args = parser.parse_known_args()

pipeline_options = PipelineOptions(pipeline_args)

# We use the save_main_session option because one or more DoFn's in this
# workflow rely on global context (e.g., a module imported at module level).
# pipeline_options.view_as(SetupOptions).save_main_session = True

SOURCE = ReadFromText(known_args.input)
SINK =  WriteToText(known_args.output)

with beam.Pipeline(options=pipeline_options) as p:
  pipeline(p | SOURCE)| SINK