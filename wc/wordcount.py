from __future__ import absolute_import

import logging as log
import re

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from past.builtins import unicode


class WordCountPipeline(beam.DoFn):

    def __init__(self):
        self.source1 = "gs://ingress-source-bucket/shakespeare/hamlet.txt"
        self.source2 = "gs://ingress-source-bucket/shakespeare/kinglear.txt"
        self.source3 = "gs://ingress-source-bucket/shakespeare/midsummersnightsdream.txt"
        self.output = "gs://pipelines-results-bucket/wordcount"
        self.pipeline_options = PipelineOptions(
                    runner='DataflowRunner',
                    project='oreilly-labs',
                    job_name='wordcountjob-1',
                    temp_location='gs://pipelines-temp-bucket/temp',
                    region='us-central1',
                    autoscaling_algorithm='THROUGHPUT_BASED',
                    num_workers=3,
                    max_num_workers=6

        )
        self.pipeline_options.view_as(SetupOptions).save_main_session = True

    def process(self, element, *args, **kwargs):
        return self.get_all_words(element)

    def run(self):
        with beam.Pipeline(options=self.pipeline_options) as p:
            input1 = p | 'Read First Input File from GCS' >> ReadFromText(self.source1)
            input2 = p | 'Read Second Input File from GCS' >> ReadFromText(self.source2)
            input3 = p | 'Read Third Input File from GCS' >> ReadFromText(self.source3)

            lines = (input1, input2, input3) | 'Merge PCollections' >> beam.Flatten()

            counts = (
                    lines
                        | 'Split Words' >> (beam.ParDo(WordCountPipeline()).with_output_types(unicode))
                        | 'Pair With One' >> beam.Map(lambda x: (x, 1))
                        | 'Group And Sum' >> beam.CombinePerKey(sum)
                        | 'Find  Most Frequent Words' >> beam.CombineGlobally(
                                beam.combiners.TopCombineFn(n=7, compare=lambda a, b: a[1] < b[1])
                            ).without_defaults()
                        | 'Write' >> WriteToText(self.output)
                    )

    def get_all_words(self, element):
        regex = r'\w+'
        return re.findall(regex, element, re.UNICODE)

    def to_runner_api_parameter(self, unused_context):
        pass


if __name__ == '__main__':
    log.getLogger().setLevel(log.INFO)
    log.info("Started")
    wcpln = WordCountPipeline()
    wcpln.run()