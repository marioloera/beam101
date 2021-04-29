#!/usr/bin/env python3
import datetime
import apache_beam as beam
from apache_beam.io.gcp.bigquery import WriteToBigQuery
import sys


def run(argv):

    project_id = "trustly-ds-test-1"
    dataset_id = "mario24"

    tables_kv_pairs = [
        ("error", f"{project_id}:{dataset_id}.error_table3"),
        ("user_log", f"{project_id}:{dataset_id}.query_table3"),
        ("event", f"{project_id}:{dataset_id}.event_table3"),
    ]

    data = [
        {
            "type": "error",
            "timestamp": str(datetime.datetime.now()),
            "message": "bad"
        },
        {
            "type": "user_log",
            "timestamp": str(datetime.datetime.now()),
            "message": "flu symptom"
        },
        {
            "type": "event",
            "timestamp": str(datetime.datetime.now()),
            "message": "loggin"
        },
    ]

    schema_ = {
        "fields": [
            {"name": "type", "type": "STRING", "mode": "NULLABLE"},
            {"name": "timestamp", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "message", "type": "STRING", "mode": "NULLABLE"},
        ]
    }
   
    with beam.Pipeline() as p:

        # create p collection from data
        elements = p | "add data" >> beam.Create(data)
        # elements | "print data" >> beam.Map(print)

        table_record_pcv = beam.pvalue.AsDict(
            p | "MakeTables" >> beam.Create(tables_kv_pairs))

        elements | WriteToBigQuery(
            # option 1 wokrs
            table=lambda row, table_dict: table_dict[row["type"]],
            table_side_inputs=(table_record_pcv,),
            method=WriteToBigQuery.Method.STREAMING_INSERTS,
            schema=schema_
        )


        # opcion 1
        # table=lambda x,
        # tables:
        # (tables['table1'] if 'language' in x else tables['table2']),
        # table_side_inputs=(table_record_pcv, ),
        
        # opcion 2
        # table=lambda x:
        # (output_table_3 if 'language' in x else output_table_4), 


        # s = (elements 
        #     # | "get_schema" >> beam.Map(lambda row: (
        #     #     row["type"], 
        #     #     schemas[row["type"]])
        #     # )
        #     | "get_schema" >> beam.Map(lambda row: 1 
        #                                            if row["type"] in "error"
        #                                            else 0)
        #     | "print table_names" >> beam.Map(print)
        # )


if __name__ == "__main__":
    run(sys.argv)
