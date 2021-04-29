#!/usr/bin/env python3
import apache_beam as beam

_DESTINATION_ELEMENT_PAIRS = [
    # DESTINATION 1
    ('project1:dataset1.table1', {
        'name': 'beam', 'language': 'py'
    }),
    ('project1:dataset1.table1', {
        'name': 'beam', 'language': 'java'
    }),
    ('project1:dataset1.table1', {
        'name': 'beam', 'language': 'go'
    }),
    ('project1:dataset1.table1', {
        'name': 'flink', 'language': 'java'
    }),
    ('project1:dataset1.table1', {
        'name': 'flink', 'language': 'scala'
    }),

    # DESTINATION 3
    ('project1:dataset1.table3', {
        'name': 'spark', 'language': 'scala'
    }),

    # DESTINATION 1
    ('project1:dataset1.table1', {
        'name': 'spark', 'language': 'py'
    }),
    ('project1:dataset1.table1', {
        'name': 'spark', 'language': 'scala'
    }),

    # DESTINATION 2
    ('project1:dataset1.table2', {
        'name': 'beam', 'foundation': 'apache'
    }),
    ('project1:dataset1.table2', {
        'name': 'flink', 'foundation': 'apache'
    }),
    ('project1:dataset1.table2', {
        'name': 'spark', 'foundation': 'apache'
    }),
]

_DISTINCT_DESTINATIONS = list({elm[0] for elm in _DESTINATION_ELEMENT_PAIRS})

_ELEMENTS = [elm[1] for elm in _DESTINATION_ELEMENT_PAIRS]

print(_ELEMENTS)


with beam.Pipeline() as p:
    input = p | beam.Create(_ELEMENTS, reshuffle=False)
    input | "print data1" >> beam.Map(print)

    # schema_map_pcv = beam.pvalue.AsDict(
    #     p | "MakeSchemas" >> beam.Create(schema_kv_pairs))

    # table_record_pcv = beam.pvalue.AsDict(
    #     p | "MakeTables" >> beam.Create([('table1', output_table_1),
    #                                      ('table2', output_table_2)]))

    # Get all input in same machine
    input = (
        input
        | beam.Map(lambda x: (None, x))
        | beam.GroupByKey()
        | beam.FlatMap(lambda elm: elm[1]))
     
    input | "print data2" >> beam.Map(print)


    # input | "WriteWithMultipleDests" >> bigquery.WriteToBigQuery(
    (input | "WriteWithMultipleDests" >> beam.Map(
            table=lambda x:
            (output_table_3 if 'language' in x else output_table_4)
            )
        | "print data3" >> beam.Map(print)
    )