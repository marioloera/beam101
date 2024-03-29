#!/usr/bin/env python3

import apache_beam as beam
from apache_beam import pvalue
import argparse
import sys

class EvenOdd(beam.PTransform):

    def __init__(self):
        pass

    @staticmethod
    def even_odd(x):
        type = 'odd' if x % 2 else 'even'
        yield pvalue.TaggedOutput(type, x)
        if x % 10 == 0:
            yield x

    def expand(self, pcollection):
        return (
            pcollection
            | beam.Map(lambda w: int(w))        
            | beam.ParDo(EvenOdd.even_odd).with_outputs('odd', 'even')
        )


class WriteFiles(beam.PTransform):

    def __init__(self, file_name_prefix, file_name_suffix):
        self.file_name_prefix = file_name_prefix
        self.file_name_suffix = file_name_suffix

    def expand(self, pcollection):

        return (
            pcollection
            | beam.io.WriteToText(self.file_name_prefix , self.file_name_suffix )
        )


if __name__ == '__main__':

    # Command line arguments
    parser = argparse.ArgumentParser(description='Demonstrate side inputs')
    parser.add_argument('--bucket', required=False, help='Specify Cloud Storage bucket for output', default='mllkthdbt_b')
    parser.add_argument('--project',required=False, help='Specify Google Cloud project', default='mllkthdbt')
    parser.add_argument('--runner',required=False, help='DirectRunner or DataFlowRunner', default='DirectRunner')
    parser.add_argument('--src',required=False, help='local or gcs', default='local')

    opts = parser.parse_args()

    argv = [
        '--project={0}'.format(opts.project),
        '--job_name=xhybridrunner',
        '--runner={0}'.format(opts.runner),
    ]

    gcs = f'gs://{opts.bucket}/' if opts.src == 'gcs' or opts.runner == 'DataFlowRunner' else ''

    output_prefix = gcs + 'output_data/numb'
    input_data = gcs + 'input_data/numbers*.csv'

    if opts.runner == 'DataFlowRunner':
        argv += [           
            '--save_main_session',
            '--staging_location=gs://{0}/staging/'.format(opts.bucket),
            '--temp_location=gs://{0}/staging/'.format(opts.bucket),
            '--region=us-central1',
            '--max_num_workers=5'
        ]

    [print(l) for l in argv]
    print(gcs),
    print(output_prefix)
    print(input_data)
    p = beam.Pipeline(argv=argv)
 
    results = (
        p | 'Read' >> beam.io.ReadFromText(input_data)
        # | beam.Map(lambda w: int(w))        
        # | beam.ParDo(even_odd).with_outputs('odd', 'even')
        | EvenOdd()
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
        | 'w1' >> WriteFiles(output_prefix, '_all.txt')
    )
    results[None] |  'w2' >> WriteFiles(output_prefix, '_none.txt' )
    results['even'] | 'w3' >> WriteFiles(output_prefix, '_even.txt' )
    results['odd'] |  'w4' >> WriteFiles(output_prefix, '_odd.txt' )

    if opts.runner == 'DataFlowRunner':
        p.run()
    else:
        p.run().wait_until_finish()
