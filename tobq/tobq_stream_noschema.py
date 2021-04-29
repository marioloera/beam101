#!/usr/bin/env python3
import datetime
import sys

import apache_beam as beam
from apache_beam.io.gcp.bigquery import WriteToBigQuery

def run(argv):

    project_id = "trustly-ds-test-1"
    dataset_id = "mario24"

    tables = [
        ("error", f"{project_id}:{dataset_id}.error_table3"),
        ("user_log", f"{project_id}:{dataset_id}.query_table3"),
    ]

    data = [
        {
            "type": "error",
            # "timestamp": "2021-01-01 12:34:56",
            "timestamp": str(datetime.datetime.now()),
            "message": "bad"
        },
        {
            "type": "user_log",
            "timestamp": str(datetime.datetime.now()),
            "message": "flu symptom"
        },
    ]

    # tables should exists and match the schema
    # schema_ = {
    #     "fields": [
    #         {"name": "type", "type": "STRING", "mode": "NULLABLE"},
    #         {"name": "timestamp", "type": "TIMESTAMP", "mode": "NULLABLE"},
    #         {"name": "message", "type": "STRING", "mode": "NULLABLE"},
    #     ]
    # }
   
    with beam.Pipeline() as p:

        # create p collection from data
        elements = p | "add data" >> beam.Create(data)
        # elements | "print data" >> beam.Map(print)

        table_names = p | "add tables" >> beam.Create(tables)
        # table_names | "print table_names" >> beam.Map(print)

        table_names_dict = beam.pvalue.AsDict(table_names)

        # -custom_gcs_temp_location, or pass method="STREAMING_INSERTS" to WriteToBigQuery.
        elements | WriteToBigQuery(
            table=lambda row, table_dict: table_dict[row["type"]],
            table_side_inputs=(table_names_dict,),
            method=WriteToBigQuery.Method.STREAMING_INSERTS,
        )

if __name__ == "__main__":
    run(sys.argv)
