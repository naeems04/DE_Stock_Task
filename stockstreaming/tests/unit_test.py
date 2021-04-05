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
stream_filename = "../../test_data/input/stream_data.txt"

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
            response['average'] = values[1]
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

    def test_top_three_avg(self):

        with beam.Pipeline(options=PipelineOptions()) as p:
            rows = (
                    p
                    | "Read Text" >> ReadFromText(input_filename)
                    | "Parse" >> beam.ParDo(ParseJson())
                    | "Form Tuple" >> beam.ParDo(FormKeyValue())
            )

            one_min_stream = (
                    rows
                    #| 'Hopping Windows' >> beam.WindowInto(beam.window.SlidingWindows(60, 10))
                    | "Calculating average" >> beam.CombinePerKey(
                beam.combiners.MeanCombineFn())
                    | 'Top 3 avg price' >> beam.CombineGlobally(
                beam.combiners.TopCombineFn(n=3, compare=lambda a, b: a[1] < b[1])).without_defaults()
                # | 'AddWindowEndTimestamp' >> (beam.ParDo(AddTimestamp(), 1))
            )

            response = (one_min_stream
                        | "Format output" >> beam.ParDo(ConvertToJson())
                        )

            #response | 'Print' >> beam.ParDo(lambda c: print('%s' % (c)))

            # Assert on the results.
            assert_that(response, equal_to([{'symbol': 'IBM', 'average': 54.0},{'symbol': 'XOM', 'average': 57.0},{'symbol': 'ABN', 'average': 49.5}]))

    def test_one_min_top_three_avg(self):

        with beam.Pipeline(options=PipelineOptions()) as p:
            rows = (
                    p
                    | "Read Text" >> ReadFromText(stream_filename)
                    | "Parse" >> beam.ParDo(ParseJson())
                    | "Form Tuple" >> beam.ParDo(FormKeyValue())
            )

            one_min_stream = (
                    rows
                    | 'Hopping Windows' >> beam.WindowInto(beam.window.SlidingWindows(60, 10))
                    | "Calculating average" >> beam.CombinePerKey(
                beam.combiners.MeanCombineFn())
                    | 'Top 3 avg price' >> beam.CombineGlobally(
                beam.combiners.TopCombineFn(n=3, compare=lambda a, b: a[1] < b[1])).without_defaults()
                # | 'AddWindowEndTimestamp' >> (beam.ParDo(AddTimestamp(), 1))
            )

            response = (one_min_stream
                        | "Format output" >> beam.ParDo(ConvertToJson())
                        )

            #response | 'Print' >> beam.ParDo(lambda c: print('%s' % (c)))
            # Assert on the results.
            assert_that(response,equal_to([{'symbol': 'XOM', 'average': 76.69},{'symbol': 'AMZN', 'average': 54.135000000000005},{'symbol': 'XOM', 'average': 76.69},{'symbol': 'AMZN', 'average': 54.135000000000005},{'symbol': 'XOM', 'average': 76.69},{'symbol': 'AMZN', 'average': 54.135000000000005},{'symbol': 'XOM', 'average': 76.69},{'symbol': 'AMZN', 'average': 54.135000000000005},{'symbol': 'XOM', 'average': 76.69},{'symbol': 'AMZN', 'average': 54.135000000000005},{'symbol': 'XOM', 'average': 76.69},{'symbol': 'AMZN', 'average': 54.135000000000005}]))

    def test_five_min_top_three_avg(self):

        with beam.Pipeline(options=PipelineOptions()) as p:
            rows = (
                    p
                    | "Read Text" >> ReadFromText(stream_filename)
                    | "Parse" >> beam.ParDo(ParseJson())
                    | "Form Tuple" >> beam.ParDo(FormKeyValue())
            )

            five_min_stream = (
                    rows
                    | 'Hopping Windows' >> beam.WindowInto(beam.window.SlidingWindows(300, 10))
                    | "Calculating average" >> beam.CombinePerKey(
                beam.combiners.MeanCombineFn())
                    | 'Top 3 avg price' >> beam.CombineGlobally(
                beam.combiners.TopCombineFn(n=3, compare=lambda a, b: a[1] < b[1])).without_defaults()
                # | 'AddWindowEndTimestamp' >> (beam.ParDo(AddTimestamp(), 1))
            )

            response = (five_min_stream
                        | "Format output" >> beam.ParDo(ConvertToJson())
                        )

            #response | 'Print' >> beam.ParDo(lambda c: print('%s' % (c)))

            # Assert on the results.

            assert_that(response,equal_to([{'symbol': 'XOM', 'average': 76.69},{'symbol': 'AMZN', 'average': 54.135000000000005},{'symbol': 'XOM', 'average': 76.69},{'symbol': 'AMZN', 'average': 54.135000000000005},{'symbol': 'XOM', 'average': 76.69},{'symbol': 'AMZN', 'average': 54.135000000000005},{'symbol': 'XOM', 'average': 76.69},{'symbol': 'AMZN','average': 54.135000000000005},{'symbol': 'XOM', 'average': 76.69},{'symbol': 'AMZN', 'average': 54.135000000000005},{'symbol': 'XOM', 'average': 76.69},{'symbol': 'AMZN', 'average': 54.135000000000005},{'symbol': 'XOM', 'average': 76.69},{'symbol': 'AMZN', 'average': 54.135000000000005},{'symbol': 'XOM', 'average': 76.69},{'symbol': 'AMZN', 'average': 54.135000000000005},{'symbol': 'XOM', 'average': 76.69},{'symbol': 'AMZN', 'average': 54.135000000000005},{'symbol': 'XOM', 'average': 76.69},{'symbol': 'AMZN', 'average': 54.135000000000005},{'symbol': 'XOM', 'average': 76.69},{'symbol': 'AMZN', 'average': 54.135000000000005},{'symbol': 'XOM', 'average': 76.69},{'symbol': 'AMZN', 'average': 54.135000000000005},{'symbol': 'XOM', 'average': 76.69},{'symbol': 'AMZN', 'average': 54.135000000000005},{'symbol': 'XOM', 'average': 76.69},{'symbol': 'AMZN', 'average': 54.135000000000005},{'symbol': 'XOM', 'average': 76.69},{'symbol': 'AMZN', 'average': 54.135000000000005},{'symbol': 'XOM', 'average': 76.69},{'symbol': 'AMZN', 'average': 54.135000000000005},{'symbol': 'XOM', 'average': 76.69},{'symbol': 'AMZN', 'average': 54.135000000000005},{'symbol': 'XOM', 'average': 76.69},{'symbol': 'AMZN', 'average': 54.135000000000005},{'symbol': 'XOM', 'average': 76.69},{'symbol': 'AMZN', 'average': 54.135000000000005},{'symbol': 'XOM', 'average': 76.69},{'symbol': 'AMZN', 'average': 54.135000000000005},{'symbol': 'XOM', 'average': 76.69},{'symbol': 'AMZN', 'average': 54.135000000000005},{'symbol': 'XOM', 'average': 76.69},{'symbol': 'AMZN', 'average': 54.135000000000005},
{'symbol': 'XOM', 'average': 76.69},{'symbol': 'AMZN', 'average': 54.135000000000005},{'symbol': 'XOM', 'average': 76.69},{'symbol': 'AMZN', 'average': 54.135000000000005},{'symbol': 'XOM', 'average': 76.69},{'symbol': 'AMZN', 'average': 54.135000000000005},{'symbol': 'XOM', 'average': 76.69},{'symbol': 'AMZN', 'average': 54.135000000000005},{'symbol': 'XOM', 'average': 76.69},{'symbol': 'AMZN', 'average': 54.135000000000005},{'symbol': 'XOM', 'average': 76.69},{'symbol': 'AMZN', 'average': 54.135000000000005},{'symbol': 'XOM', 'average': 76.69},{'symbol': 'AMZN', 'average': 54.135000000000005},{'symbol': 'XOM', 'average': 76.69},{'symbol': 'AMZN', 'average': 54.135000000000005}]))

    def test_fifteen_min_top_three_avg(self):
        with beam.Pipeline(options=PipelineOptions()) as p:
            rows = (
                    p
                    | "Read Text" >> ReadFromText(stream_filename)
                    | "Parse" >> beam.ParDo(ParseJson())
                    | "Form Tuple" >> beam.ParDo(FormKeyValue())
            )

            fifteen_min_stream = (
                    rows
                    | 'Hopping Windows' >> beam.WindowInto(beam.window.SlidingWindows(900, 10))
                    | "Calculating average" >> beam.CombinePerKey(
                beam.combiners.MeanCombineFn())
                    | 'Top 3 avg price' >> beam.CombineGlobally(
                beam.combiners.TopCombineFn(n=3, compare=lambda a, b: a[1] < b[1])).without_defaults()
                # | 'AddWindowEndTimestamp' >> (beam.ParDo(AddTimestamp(), 1))
            )

            response = (fifteen_min_stream
                        | "Format output" >> beam.ParDo(ConvertToJson())
                        )

            #response | 'Print' >> beam.ParDo(lambda c: print('%s' % (c)))

            # Assert on the results.
            assert_that(response, equal_to([{'symbol': 'XOM', 'average': 76.69},{'symbol': 'AMZN', 'average': 54.135000000000005},{'symbol': 'XOM', 'average': 76.69},{'symbol': 'AMZN', 'average': 54.135000000000005},{'symbol': 'XOM', 'average': 76.69},{'symbol': 'AMZN', 'average': 54.135000000000005},{'symbol': 'XOM', 'average': 76.69},{'symbol': 'AMZN', 'average': 54.135000000000005},{'symbol': 'XOM', 'average': 76.69},{'symbol': 'AMZN', 'average': 54.135000000000005},{'symbol': 'XOM', 'average': 76.69},{'symbol': 'AMZN', 'average': 54.135000000000005},{'symbol': 'XOM', 'average': 76.69},{'symbol': 'AMZN', 'average': 54.135000000000005},{'symbol': 'XOM', 'average': 76.69},{'symbol': 'AMZN', 'average': 54.135000000000005},{'symbol': 'XOM', 'average': 76.69},{'symbol': 'AMZN', 'average': 54.135000000000005},{'symbol': 'XOM', 'average': 76.69},{'symbol': 'AMZN', 'average': 54.135000000000005},{'symbol': 'XOM', 'average': 76.69},{'symbol': 'AMZN', 'average': 54.135000000000005},{'symbol': 'XOM', 'average': 76.69},{'symbol': 'AMZN', 'average': 54.135000000000005},{'symbol': 'XOM', 'average': 76.69},{'symbol': 'AMZN', 'average': 54.135000000000005},{'symbol': 'XOM', 'average': 76.69},{'symbol': 'AMZN', 'average': 54.135000000000005},{'symbol': 'XOM', 'average': 76.69},{'symbol': 'AMZN', 'average': 54.135000000000005},{'symbol': 'XOM', 'average': 76.69},{'symbol': 'AMZN', 'average': 54.135000000000005},{'symbol': 'XOM', 'average': 76.69},{'symbol': 'AMZN', 'average': 54.135000000000005},{'symbol': 'XOM', 'average': 76.69},{'symbol': 'AMZN', 'average': 54.135000000000005},{'symbol': 'XOM', 'average': 76.69},{'symbol': 'AMZN', 'average': 54.135000000000005},{'symbol': 'XOM', 'average': 76.69},{'symbol': 'AMZN', 'average': 54.135000000000005},{'symbol': 'XOM', 'average': 76.69},{'symbol': 'AMZN', 'average': 54.135000000000005},{'symbol': 'XOM', 'average': 76.69},{'symbol': 'AMZN', 'average': 54.135000000000005},{'symbol': 'XOM', 'average': 76.69},{'symbol': 'AMZN', 'average': 54.135000000000005},{'symbol': 'XOM', 'average': 76.69},{'symbol': 'AMZN', 'average': 54.135000000000005},{'symbol': 'XOM', 'average': 76.69},{'symbol': 'AMZN', 'average': 54.135000000000005},{'symbol': 'XOM', 'average': 76.69},{'symbol': 'AMZN', 'average': 54.135000000000005},{'symbol': 'XOM', 'average': 76.69},{'symbol': 'AMZN', 'average': 54.135000000000005},{'symbol': 'XOM', 'average': 76.69},{'symbol': 'AMZN', 'average': 54.135000000000005},{'symbol': 'XOM', 'average': 76.69},{'symbol': 'AMZN', 'average': 54.135000000000005},{'symbol': 'XOM', 'average': 76.69},{'symbol': 'AMZN', 'average': 54.135000000000005},{'symbol': 'XOM', 'average': 76.69},{'symbol': 'AMZN', 'average': 54.135000000000005},{'symbol': 'XOM', 'average': 76.69},{'symbol': 'AMZN', 'average': 54.135000000000005},{'symbol': 'XOM', 'average': 76.69},{'symbol': 'AMZN', 'average': 54.135000000000005},{'symbol': 'XOM', 'average': 76.69},{'symbol': 'AMZN', 'average': 54.135000000000005},{'symbol': 'XOM', 'average': 76.69},{'symbol': 'AMZN', 'average': 54.135000000000005},{'symbol': 'XOM', 'average': 76.69},{'symbol': 'AMZN', 'average': 54.135000000000005},{'symbol': 'XOM', 'average': 76.69},{'symbol': 'AMZN', 'average': 54.135000000000005},{'symbol': 'XOM', 'average': 76.69},{'symbol': 'AMZN', 'average': 54.135000000000005},{'symbol': 'XOM', 'average': 76.69},{'symbol': 'AMZN', 'average': 54.135000000000005},{'symbol': 'XOM', 'average': 76.69},{'symbol': 'AMZN', 'average': 54.135000000000005},{'symbol': 'XOM', 'average': 76.69},{'symbol': 'AMZN', 'average': 54.135000000000005},{'symbol': 'XOM', 'average': 76.69},{'symbol': 'AMZN', 'average': 54.135000000000005},{'symbol': 'XOM', 'average': 76.69},{'symbol': 'AMZN', 'average': 54.135000000000005},{'symbol': 'XOM', 'average': 76.69},{'symbol': 'AMZN', 'average': 54.135000000000005},{'symbol': 'XOM', 'average': 76.69},{'symbol': 'AMZN', 'average': 54.135000000000005},{'symbol': 'XOM', 'average': 76.69},{'symbol': 'AMZN', 'average': 54.135000000000005},{'symbol': 'XOM', 'average': 76.69},{'symbol': 'AMZN', 'average': 54.135000000000005},{'symbol': 'XOM', 'average': 76.69},{'symbol': 'AMZN', 'average': 54.135000000000005},{'symbol': 'XOM', 'average': 76.69},{'symbol': 'AMZN', 'average': 54.135000000000005},{'symbol': 'XOM', 'average': 76.69},{'symbol': 'AMZN', 'average': 54.135000000000005},{'symbol': 'XOM', 'average': 76.69},{'symbol': 'AMZN', 'average': 54.135000000000005},{'symbol': 'XOM', 'average': 76.69},{'symbol': 'AMZN', 'average': 54.135000000000005},{'symbol': 'XOM', 'average': 76.69},{'symbol': 'AMZN', 'average': 54.135000000000005},{'symbol': 'XOM', 'average': 76.69},{'symbol': 'AMZN', 'average': 54.135000000000005},{'symbol': 'XOM', 'average': 76.69},{'symbol': 'AMZN', 'average': 54.135000000000005},{'symbol': 'XOM', 'average': 76.69},{'symbol': 'AMZN', 'average': 54.135000000000005},{'symbol': 'XOM', 'average': 76.69},{'symbol': 'AMZN', 'average': 54.135000000000005},{'symbol': 'XOM', 'average': 76.69},{'symbol': 'AMZN', 'average': 54.135000000000005},{'symbol': 'XOM', 'average': 76.69},{'symbol': 'AMZN', 'average': 54.135000000000005},{'symbol': 'XOM', 'average': 76.69},{'symbol': 'AMZN', 'average': 54.135000000000005},{'symbol': 'XOM', 'average': 76.69},{'symbol': 'AMZN', 'average': 54.135000000000005},{'symbol': 'XOM', 'average': 76.69},{'symbol': 'AMZN', 'average': 54.135000000000005},{'symbol': 'XOM', 'average': 76.69},{'symbol': 'AMZN', 'average': 54.135000000000005},{'symbol': 'XOM', 'average': 76.69},{'symbol': 'AMZN', 'average': 54.135000000000005},{'symbol': 'XOM', 'average': 76.69},{'symbol': 'AMZN', 'average': 54.135000000000005},{'symbol': 'XOM', 'average': 76.69},{'symbol': 'AMZN', 'average': 54.135000000000005},{'symbol': 'XOM', 'average': 76.69},{'symbol': 'AMZN', 'average': 54.135000000000005},{'symbol': 'XOM', 'average': 76.69},{'symbol': 'AMZN', 'average': 54.135000000000005},{'symbol': 'XOM', 'average': 76.69},{'symbol': 'AMZN', 'average': 54.135000000000005},{'symbol': 'XOM', 'average': 76.69},{'symbol': 'AMZN', 'average': 54.135000000000005},{'symbol': 'XOM', 'average': 76.69},{'symbol': 'AMZN', 'average': 54.135000000000005},{'symbol': 'XOM', 'average': 76.69},{'symbol': 'AMZN', 'average': 54.135000000000005},{'symbol': 'XOM', 'average': 76.69},{'symbol': 'AMZN', 'average': 54.135000000000005},{'symbol': 'XOM', 'average': 76.69},{'symbol': 'AMZN', 'average': 54.135000000000005},{'symbol': 'XOM', 'average': 76.69},{'symbol': 'AMZN', 'average': 54.135000000000005},{'symbol': 'XOM', 'average': 76.69},{'symbol': 'AMZN', 'average': 54.135000000000005},{'symbol': 'XOM', 'average': 76.69},{'symbol': 'AMZN', 'average': 54.135000000000005},{'symbol': 'XOM', 'average': 76.69},{'symbol': 'AMZN', 'average': 54.135000000000005},{'symbol': 'XOM', 'average': 76.69},{'symbol': 'AMZN', 'average': 54.135000000000005},{'symbol': 'XOM', 'average': 76.69},{'symbol': 'AMZN', 'average': 54.135000000000005},{'symbol': 'XOM', 'average': 76.69},{'symbol': 'AMZN', 'average': 54.135000000000005},{'symbol': 'XOM', 'average': 76.69},{'symbol': 'AMZN', 'average': 54.135000000000005},{'symbol': 'XOM', 'average': 76.69},{'symbol': 'AMZN', 'average': 54.135000000000005},{'symbol': 'XOM', 'average': 76.69},{'symbol': 'AMZN', 'average': 54.135000000000005},{'symbol': 'XOM', 'average': 76.69},{'symbol': 'AMZN', 'average': 54.135000000000005},{'symbol': 'XOM', 'average': 76.69},{'symbol': 'AMZN', 'average': 54.135000000000005},{'symbol': 'XOM', 'average': 76.69},{'symbol': 'AMZN', 'average': 54.135000000000005},{'symbol': 'XOM', 'average': 76.69},{'symbol': 'AMZN', 'average': 54.135000000000005},{'symbol': 'XOM', 'average': 76.69},{'symbol': 'AMZN', 'average': 54.135000000000005},{'symbol': 'XOM', 'average': 76.69},{'symbol': 'AMZN', 'average': 54.135000000000005}]))


if __name__ == '__main__':
    unittest.main()