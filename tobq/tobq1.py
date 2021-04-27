#!/usr/bin/env python3
import apache_beam as beam
from apache_beam.io.gcp.bigquery import WriteToBigQuery
import sys


def run(argv):

    project_id = "trustly-ds-test-1"
    dataset_id = "mario24"

    tables = [
        ("error", f"{project_id}:{dataset_id}.error_table"),
        ("user_log", f"{project_id}:{dataset_id}.query_table"),
    ]

    data = [
        {
            "type": "error",
            "timestamp": "12:34:56",
            "message": "bad"
        },
        {
            "type": "user_log",
            "timestamp": "12:34:59",
            "message": "flu symptom"
        },
    ]

    schema_ = {
        "fields": [
            {"name": "type", "type": "STRING", "mode": "NULLABLE"},
            {"name": "timestamp", "type": "STRING", "mode": "NULLABLE"},
            {"name": "message", "type": "STRING", "mode": "NULLABLE"},
        ]
    }
   
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
            schema=schema_
        )

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
