#!/usr/bin/env python3

import apache_beam as beam
from apache_beam import pvalue
import argparse
import sys

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
        '--job_name=xHybridRunner',
        '--runner={0}'.format(opts.runner),
    ]

    if opts.runner == 'DirectRunner':
        output_prefix = 'output_data/numb'
        input_data = 'input_data/numbers*.csv'

    else:
        output_prefix = f'gs://{opts.bucket}/output_data/numb'
        input_data = f'gs://{opts.bucket}/input_data/numbers.csv'

        argv += [           
            '--save_main_session',
            '--staging_location=gs://{0}/staging/'.format(opts.bucket),
            '--temp_location=gs://{0}/staging/'.format(opts.bucket),
            '--region=us-central1',
            '--max_num_workers=5'
        ]

    [print(l) for l in argv]

    p = beam.Pipeline(argv=argv)
    
    numbers = [i for i in range(5)]
    # print('numbers:', numbers)

    results = (
        p | 'Read' >> beam.io.ReadFromText(input_data)
        # numbers
        | beam.Map(lambda w: int(w))        
        # | beam.FlatMap(even_odd).with_outputs('odd', 'even') # only worked with numbers a list
        | beam.ParDo(even_odd).with_outputs('odd', 'even')
    )
    # calling results : results['even'] = results.even

    if opts.runner == 'DirectRunner':
        results.even| beam.Filter(lambda x: x < 5) | 'even' >> beam.Map(print)
        results['odd'] | beam.Filter(lambda x: x < 5) | 'odd' >> beam.Map(print)
        results[None] | 'tens' >> beam.Map(print)


    # # write to files
    # results |  'w1' >> beam.io.WriteToText(output_prefix, '_all.txt') # show the objet not the data
    (
        #(results['odd'], results['even'])
        results
        | beam.Flatten() 
        |'w1' >> beam.io.WriteToText(output_prefix, '_all.txt')
    )
    results[None] |  'w2' >> beam.io.WriteToText(output_prefix, '_none.txt' )
    results['even'] | 'w3' >> beam.io.WriteToText(output_prefix, '_even.txt' )
    results['odd'] |  'w4' >> beam.io.WriteToText(output_prefix, '_odd.txt' )

    if opts.runner == 'DataFlowRunner':
        p.run()
    else:
        p.run().wait_until_finish()
