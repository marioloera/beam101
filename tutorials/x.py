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
    parser = argparse.ArgumentParser(description='Find the most used Java packages')
    parser.add_argument('--output_prefix', default='./output_data/is_popluar_mll_output', help='Output prefix')
    parser.add_argument('--input', default='./input_data/', help='Input directory')

    options, pipeline_args = parser.parse_known_args()
    p = beam.Pipeline(argv=pipeline_args)

    numbers = [i for i in range(23)]
    print('numbers:', numbers)

    results = (
        p | beam.Create(numbers) | beam.ParDo(even_odd).with_outputs('odd', 'even')
        # numbers | beam.FlatMap(even_odd).with_outputs('odd', 'even')
    )
    
    # print results
    results['even'] | 'w1' >> beam.Map(print)
    results[None] | 'w2' >> beam.Map(print)
    results.odd | 'w3' >> beam.Map(print)

    # results to file
    output_prefix = 'output_data/numb'
    (results['even'], results['odd'] ) | beam.Flatten() | 'wf1' >> beam.io.WriteToText(output_prefix, '_all.txt')
    results[None] | 'wf2' >> beam.io.WriteToText(output_prefix, '_none.txt')
    results['even'] | 'wf3' >> beam.io.WriteToText(output_prefix, '_even.txt')
    results['odd'] | 'wf4' >> beam.io.WriteToText(output_prefix, '_odd.txt')

    p.run().wait_until_finish()
