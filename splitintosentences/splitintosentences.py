import apache_beam as beam
import re
import uuid
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


class GenerateIdForSentence(beam.DoFn):

    def process(self, element):
        res = [str(uuid.uuid4()), element]
        print(res)
        return res


def split_into_sentences(element):
    return element


def is_not_empty(element):
    return bool(element.strip())


def contains_only_numbers(element):
    return not str(element).isdecimal()


def build_pipeline_options():
    pipeline_options = PipelineOptions(
        # runner='DataflowRunner',
        # project='oreilly-labs',
        job_name='split-into-sentences-1',
        temp_location='gs://pipelines-temp-bucket/temp',
        region='us-central1',
        # autoscaling_algorithm='THROUGHPUT_BASED',
        # num_workers=3,
        # max_num_workers=6
    )
    pipeline_options.view_as(SetupOptions).save_main_session = True
    return pipeline_options


def run():
    input_pattern = "gs://ingress-source-bucket/splitintosentences/*.txt"
    output = "gs://pipelines-results-bucket/splitintosentence/"

    p = beam.Pipeline(options=build_pipeline_options())

    (p
     | 'Read Data' >> beam.io.ReadFromText(file_pattern=input_pattern)
     | 'Split Data Into Sentences' >> beam.Map(lambda x: split_into_sentences(x))
     | 'Filter out empty strings' >> beam.Filter(is_not_empty)
     | 'Filter out numbers' >> beam.Filter(contains_only_numbers)
     | 'ID Generation' >> beam.ParDo(GenerateIdForSentence())
     | 'Write' >> beam.io.WriteToText(output)
     )
    p.run()


if __name__ == '__main__':
    run()
