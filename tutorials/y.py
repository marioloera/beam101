#!/usr/bin/env python3


import apache_beam as beam
from apache_beam import pvalue
import argparse

def even_odd(x):
   type = 'odd' if x % 2 else 'even'
   #yield (type, x)
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

   results = numbers | beam.FlatMap(even_odd).with_outputs()

   print('results:\n', results)
   print('results[None]:\n', results[None])


   print(1, ' r:\n', results._results_by_tag)
   print(2, ' r:\n', results._results_by_tag.items())
   #output1 = (results | beam.Map(print))

   # evens = results.even
   # odds = results.odd
   # tens = results[None]  # the undeclared main output

   # print(evens)
   # print(odds)
   # print(tens)



   p.run().wait_until_finish()
