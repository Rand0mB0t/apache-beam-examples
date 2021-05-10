'''
A sample to serve as a boilerplate for apache beam pipeline.
This basic sample contains the minimum required code for any
beam pipeline .

It has PipelineOptions usage.
ArgumentParser usage to be used in conjunction with PipelineOptions

Usage:

py sample_pipeline_1.py \
    --input_file_path "input_data/sample_1_input_file.csv" \
    --output_file_path "output_data/sample_1_output_file.csv" \
    --runner DirectRunner

'''

import argparse
import logging

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions


class MyTransform(beam.DoFn):

    def process(self, element, *args, **kwargs):
        logging.info(f"this is one element - {element}")
        logging.info("Making transformation in the element")

        try:
            name, country, state = element.split(',')
            if country in ['',None]:
                country = 'India'
            transformed_element = ",".join(list(map(str.capitalize,[name, country, state])))
            yield transformed_element

        except Exception as e:
            logging.error(f'There is error in the record')
            logging.error(f'Errored record - {element}')
            logging.error(f'The error \n {e}')



def run(args=None):
    parser = argparse.ArgumentParser()

    parser.add_argument('--input_file_path')
    parser.add_argument('--output_file_path')

    known_args, pipeline_args = parser.parse_known_args(args)

    pipeline_options = PipelineOptions(pipeline_args)

    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            |  "Source I/O Read Transform" >> beam.io.ReadFromText(known_args.input_file_path)
            |  "Custom ParDo Transformation" >> beam.ParDo(MyTransform())
            |  "Sink I/O Write Transform" >> beam.io.WriteToText(known_args.output_file_path)
        )
    

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()