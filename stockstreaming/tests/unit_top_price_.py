# Standard Python Imports
import argparse
import itertools
import logging
import time
import json
import unittest

# 3rd Party Imports
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions, StandardOptions
from apache_beam.io import ReadFromText
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

input_filename = "../../test_data/input/input_data.txt"

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
        for values in element:
            response = {}
            response['symbol'] = values[0]
            response['price'] = values[1]
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


class StockUnitTest(unittest.TestCase):
    '''
    Test suite for test case for stock data
    '''

    def test_top_price(self):

        with beam.Pipeline(options=PipelineOptions()) as p:
            rows = (
                    p
                    | "Read Text" >> ReadFromText(input_filename)
                    | "Parse" >> beam.ParDo(ParseJson())
                    | "Form Tuple" >> beam.ParDo(FormKeyValue())
            )

            top_price_stream = (
                    rows
                    | 'Fixed Windows' >> beam.WindowInto(beam.window.FixedWindows(300))
                    | 'Top 3 avg price' >> beam.CombineGlobally(
                beam.combiners.TopCombineFn(n=1, compare=lambda a, b: a[1] < b[1])).without_defaults()
            )

            response = (top_price_stream
                        | "Format output" >> beam.ParDo(ConvertToJson())
                        )

            response | 'Print' >> beam.ParDo(lambda c: print('%s' % (c)))

            # Assert on the results.
            assert_that(response, equal_to([{'symbol': 'IBM', 'price': 58.0}]))

if __name__ == '__main__':
    unittest.main()