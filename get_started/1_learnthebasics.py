# https://colab.research.google.com/github/apache/beam/blob/master/examples/notebooks/tour-of-beam/getting-started.ipynb

import apache_beam as beam

inputs = [0, 1, 2, 3, 4]


# FILTER
def isEven(x):
  return x % 2 == 0

print("\n * add number, fileter even, map, and sum:")
with beam.Pipeline() as pipeline:

  """
    outputs = pipeline | step1 | step2 | step3
    Pipelines can quickly grow long, so it's sometimes easier to read if we surround them with parentheses and break them into multiple lines.

    # This is equivalent to the example above.
    outputs = (
      pipeline
      | step1
      | step2
      | step3
    )
  """

  outputs = (
      pipeline
      # We use the Create transform to feed the pipeline with an iterable of elements, like a list.
      # name must be uniques
      | 'Create values' >> beam.Create(inputs)
      | 'Keep only even numbers' >> beam.Filter(lambda x: x % 2 == 0)
      # same ways to do it
      #| 'Keep only even numbers 2' >>beam.Filter(isEven)  # direct call of the function
      #| 'Keep only even numbers 3' >> beam.Filter(lambda x: isEven(x)) # lamda funtion


      | 'Expand elements' >> beam.FlatMap(lambda x: [x for _ in range(x)])

      #| 'Expand elements' >> beam.Map(lambda x: [x for _ in range(x)])
      # using only Flat
      # [1]
      # [2, 2]
      # [3, 3, 3]


      # names can be skiped
      | 'Multiply by 3' >> beam.Map(lambda x: x * 3.7)
      | 'Sum all values together' >> beam.CombineGlobally(sum)

  )

  # We can only access the elements through another transform.
  outputs | beam.Map(print)


print("\n * animasl and food, GroupByKey")
inputs = [
  ('🐹', '🌽'),
  ('🐼', '🎋'),
  ('🐰', '🥕'),
  ('🐹', '🌰'),
  ('🐰', '🥒'),
]

with beam.Pipeline() as pipeline:
  outputs = (
      pipeline
      | 'Create (animal, food) pairs' >> beam.Create(inputs)
      | 'Group foods by animals' >> beam.GroupByKey()
  )

  outputs | beam.Map(print)