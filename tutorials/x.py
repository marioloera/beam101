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

   results = numbers | beam.FlatMap(even_odd).with_outputs(
      'odd', 'even'
   )

   print('results:\n', results)
   print('results[None]:\n', results[None])
   print('results["even"]:\n', results['even'])
   print('results["odd"]:\n', results['odd'])

   output_prefix = 'output_data/numb_'
   results | 'write' >> beam.io.WriteToText(output_prefix, '_all.txt')
   results[None] | 'write' >> beam.io.WriteToText(output_prefix, '_none.txt' )
   results['even'] | 'write' >> beam.io.WriteToText(output_prefix, '_even.txt' )
   results['odd'] | 'write' >> beam.io.WriteToText(output_prefix, '_odd.txt' )
   p.run().wait_until_finish()
