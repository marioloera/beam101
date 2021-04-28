#!/usr/bin/env python3
import apache_beam as beam
from apache_beam.io.gcp.bigquery import WriteToBigQuery
import sys


def run(argv):

    project_id = "trustly-ds-test-1"
    dataset_id = "mario24"
    gcs_bucket = "gs://trustly-ds-test-1_mario24"

    tables = [
        ("error", f"{project_id}:{dataset_id}.error_table5"),
        ("user_log", f"{project_id}:{dataset_id}.query_table5"),
    ]

    data = [
        {
            "type": "error",
            "timestamp": "2021-01-01 12:34:56",
            "message5": "bad"
        },
        {
            "type": "user_log",
            "timestamp": "2020-11-21 12:34:59",
            "message5": "flu symptom"
        },
    ]

    schema_ = {
        "fields": [
            {"name": "type", "type": "STRING", "mode": "NULLABLE"},
            {"name": "timestamp", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "message", "type": "STRING", "mode": "NULLABLE"},
            {"name": "message5", "type": "STRING", "mode": "NULLABLE"},
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
            # method=WriteToBigQuery.Method.STREAMING_INSERTS,
            custom_gcs_temp_location=gcs_bucket,
            # withSchemaUpdateOptions(Set.of(SchemaUpdateOption.ALLOW_FIELD_ADDITION java not in python?
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
