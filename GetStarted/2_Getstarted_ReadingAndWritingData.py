# https://colab.research.google.com/github/apache/beam/blob/master/examples/notebooks/tour-of-beam/reading-and-writing-data.ipynb#scrollTo=sQUUi4H9s-g2

import apache_beam as beam

input_files = 'data/*.txt'
with beam.Pipeline() as pipeline:
    (
        pipeline
        | 'Read files' >> beam.io.ReadFromText(input_files)
        | 'Print contents' >> beam.Map(print)
    )

output_file_name_prefix = 'outputs/file'

lines_to_write = [
    'Each element must be a string.',
    'It writes one element per line.',
    'There are no guarantees on the line order.',
    'The data might be written into multiple files.',
]

with beam.Pipeline() as pipeline:
    (
        pipeline
        | 'Create file lines' >> beam.Create(lines_to_write)
        | 'Write to files' >> beam.io.WriteToText(
            output_file_name_prefix,
            file_name_suffix='.txt')
    )