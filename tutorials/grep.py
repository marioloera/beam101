#!/usr/bin/env python3

import apache_beam as beam
import sys

def my_grep(line, term):
   # if line.startswith(term):
   if term in line:   
      print(line) 
      yield line

if __name__ == '__main__':
   p = beam.Pipeline(argv=sys.argv)
   input = './input_data/'
   output_prefix = './output_data/output'
   searchTerm = 'import'

   # find all lines that contain the searchTerm
   (p
      | 'GetJava' >> beam.io.ReadFromText(input)
      | 'Grep1' >> beam.FlatMap(lambda line: my_grep(line, searchTerm) )
      | 'Grep2' >> beam.FlatMap(lambda line: my_grep(line, 'beam') )
      | 'write' >> beam.io.WriteToText(output_prefix)
   )

   p.run().wait_until_finish()
