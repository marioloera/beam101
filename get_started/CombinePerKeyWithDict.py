import itertools

import apache_beam as beam


inputs = [
    ('table_0', {'id': 1648378088}),
    ('table_0', {'id': 1622700037}),
    ('table_1', {'id': -764011634}),
    ('table_1', {'id': -368537293}),
]
# https://beam.apache.org/documentation/transforms/python/aggregation/groupbykey/
print("\n * GroupByKey")
with beam.Pipeline() as p:
  p |  beam.Create(inputs) | beam.GroupByKey() | beam.Map(print)

# https://beam.apache.org/documentation/transforms/python/aggregation/combineperkey/

print("\n * CombinePerKey & lambda")
with beam.Pipeline() as p:
  p |  beam.Create(inputs) | beam.CombinePerKey(lambda elements: list(itertools.chain(*elements))) | beam.Map(print)


print("\n * GroupByKey & Function")
def append_lists(list_of_lists):
    return list(itertools.chain(*list_of_lists))

with beam.Pipeline() as p:
  p |  beam.Create(inputs) | beam.CombinePerKey(append_lists) | beam.Map(print)

print("\n * CombinePerKey & CombineFn")
class AppendElements(beam.CombineFn):
  def create_accumulator(self):
    accumulator = []
    return accumulator

  def add_input(self, accumulator, element):
    accumulator.append(element)
    return accumulator

  def merge_accumulators(self, accumulators):
    return list(itertools.chain(*accumulators))

  def extract_output(self, accumulator):
    return accumulator

with beam.Pipeline() as p:
  p |  beam.Create(inputs) | beam.CombinePerKey(AppendElements()) | beam.Map(print)

