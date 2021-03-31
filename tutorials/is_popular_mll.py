#!/usr/bin/env python3


import apache_beam as beam
import argparse

def hasPackage(line, terms):
   for term in terms: 
      if term in line:
         yield (term, 1)

if __name__ == '__main__':
   parser = argparse.ArgumentParser(description='Find the most used Java packages')
   parser.add_argument('--output_prefix', default='./output_data/is_popluar_mll_output', help='Output prefix')
   parser.add_argument('--input', default='./input_data/', help='Input directory')

   options, pipeline_args = parser.parse_known_args()
   p = beam.Pipeline(argv=pipeline_args)

   input = '{0}*.java'.format(options.input)
   output_prefix = options.output_prefix
   keywords = ['beam', 'java']

   result = (p
      | 'GetJava' >> beam.io.ReadFromText(input)
      | 'GetImports' >> beam.FlatMap(lambda line: hasPackage(line, keywords))
      | 'TotalUse' >> beam.CombinePerKey(sum) # ok
      #| beam.Map(print)
      #| 'write' >> beam.io.WriteToText(output_prefix)
   )

   output1 = (result | beam.Map(print))


   p.run().wait_until_finish()
