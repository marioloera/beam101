#!/usr/bin/env python3


import apache_beam as beam
from apache_beam import pvalue
import argparse

def even_odd(x):
    type = 'odd' if x % 2 else 'even'
    yield pvalue.TaggedOutput(type, x)
    if x % 10 == 0:
        yield x

if __name__ == '__main__':

    # Command line arguments
    parser = argparse.ArgumentParser(description='Demonstrate side inputs')
    parser.add_argument('--bucket', required=False, help='Specify Cloud Storage bucket for output', default='')
    parser.add_argument('--project',required=False, help='Specify Google Cloud project', default='')
    parser.add_argument('--runner',required=False, help='DirectRunner or DataFlowRunner', default='DirectRunner')

    opts = parser.parse_args()

    argv = [
    '--project={0}'.format(opts.project),
    '--job_name=javahelpjob',
    '--save_main_session',
    '--staging_location=gs://{0}/staging/'.format(opts.bucket),
    '--temp_location=gs://{0}/staging/'.format(opts.bucket),
    '--runner={0}'.format(opts.runner),
    '--region=us-central1',
    '--max_num_workers=5'
    ]

    p = beam.Pipeline(argv=argv)



    output_prefix = 'gs://{0}/javahelp/output'.format(opts.bucket)
    numbers = [i for i in range(23)]
    # print('numbers:', numbers)

    results = numbers | beam.FlatMap(even_odd).with_outputs(
        'odd', 'even'
    )

    # print('results:\n', results)
    # print('results[None]:\n', results[None])
    # print('results["even"]:\n', results['even'])
    # print('results["odd"]:\n', results['odd'])

    # output_prefix = 'output_data/numb_'
    results | 'write' >> beam.io.WriteToText(output_prefix, '_all.txt')
    results[None] | 'write' >> beam.io.WriteToText(output_prefix, '_none.txt' )
    results['even'] | 'write' >> beam.io.WriteToText(output_prefix, '_even.txt' )
    results['odd'] | 'write' >> beam.io.WriteToText(output_prefix, '_odd.txt' )

    p.run()
