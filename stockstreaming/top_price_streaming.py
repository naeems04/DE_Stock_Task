# Standard Python Imports
import argparse
import itertools
import logging
import time
import json

# 3rd Party Imports
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions, StandardOptions

class ParseJson(beam.DoFn):

    def process(self, element):
        """
        Parse json message and fetch required fields
        """
        record = json.loads(element.encode('raw_unicode_escape').decode())
        list = []
        for key, value in record.items():
            response = {}
            response['symbol'] = value['quote']['symbol']
            response['price'] = value['quote']['latestPrice']
            response['companyName'] = value['quote']['companyName']
            list.append(response)

        return list


class ConvertToJson(beam.DoFn):

    def process(self, element):
        """
        Prepare the json for outputting to BigQuery
        """
        list = []
        print("ABC")
        print(element)
        for values in element:
            response = {}
            response['symbol'] = values[0][0][0]
            response['price'] = values[0][0][1]
            response['timestamp'] = values[1]
            response['aggregation_type'] = values[2]
            list.append(response)

        return list


class FormKeyValue(beam.DoFn):

    def process(self, element):
        """
        Returns a list of tuples containing symbol and price
        """

        result = [
            (element['symbol'], float(element['price']))
        ]
        return result

class AddTimestamp(beam.DoFn):
    def process(self, element, aggregation_type, window=beam.DoFn.WindowParam):
        """Processes each windowed element by extracting the message body and its
        publish time into a tuple.
        """
        ts_format = '%Y-%m-%d %H:%M:%S.%f'
        window_end = window.end.to_utc_datetime().strftime(ts_format)

        yield (
            element,
            window_end,
            aggregation_type
        )


def run(argv=None):
    '''
    Main method for executing the pipeline operation
    '''
    parser = argparse.ArgumentParser()

    parser.add_argument('--input_mode',
                        default='stream',
                        help='Streaming input or file based batch input')

    parser.add_argument('--input_topic',
                        default='projects/stockstreamingtask/topics/stock-stream',
                        required=True,
                        help='Topic to pull data from.')

    parser.add_argument('--output_table',
                        required=True,
                        help=
                        ('Output BigQuery table for results specified as: PROJECT:DATASET.TABLE '
                         'or DATASET.TABLE.'))

    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True

    if known_args.input_mode == 'stream':
        pipeline_options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=PipelineOptions()) as p:
        rows = (
                p |
                'ReadInput' >> beam.io.ReadFromPubSub(topic=known_args.input_topic).with_output_types(bytes)
                | "Parse" >> beam.ParDo(ParseJson())
                | "Form Tuple" >> beam.ParDo(FormKeyValue())
        )

        top_price_stream = (
                rows
                | 'Fixed Windows' >> beam.WindowInto(beam.window.FixedWindows(300))
                | 'Top stock price' >> beam.CombineGlobally(
            beam.combiners.TopCombineFn(n=1, compare=lambda a, b: a[1] < b[1])).without_defaults()
                | 'AddWindowEndTimestamp' >> (beam.ParDo(AddTimestamp(), 1))
        )

        (top_price_stream
         | "Format output" >> beam.ParDo(ConvertToJson())
         | 'Write to Table' >> beam.io.WriteToBigQuery(known_args.output_table,
                                                       schema='symbol:String, price:FLOAT, timestamp:String, aggregation_type:String',
                                                       write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)
         )



        result = p.run()
        result.wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()