# Standard Python Imports
import argparse
import itertools
import logging
import time
import json
import datetime
import random

def get_data():
    event_time = datetime.datetime.utcnow() - datetime.timedelta(seconds=10)

    company = "Corp Ltd"
    outF = open("../../test_data/input/stream_data.txt", "w")
    for i in range(1,5):
        # write line to output file

        symbol = random.choice(['XOM', 'AMZN'])
        json = '"symbol": "{}", "companyName": "{}", "latestPrice": "{}"'.format(symbol,company,round(random.uniform(1.0, 100.0), 2))
        line ='{"'+symbol+'":{"quote":{'+json+'}}}'
        outF.write(line)
        outF.write("\n")

    outF.close()




if __name__ == '__main__':
    response = get_data()
    print(response)