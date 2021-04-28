#!/usr/bin/env python3
import apache_beam as beam
from apache_beam.io.gcp.bigquery import WriteToBigQuery
import sys


def run(argv):

    project_id = "trustly-ds-test-1"
    dataset_id = "mario24"

    tables = [
        ("error", f"{project_id}:{dataset_id}.error_table6"),
        ("user_log", f"{project_id}:{dataset_id}.query_table6"),
    ]

    data = [
        {
            "type": "error",
            "timestamp": "2021-01-01 12:34:56",
            "message": "bad"
        },
        {
            "type": "user_log",
            "timestamp": "2020-11-21 12:34:59",
            "query": "flu symptom"
        },
    ]

    schema_error = {
        "fields": [
            {"name": "type", "type": "STRING", "mode": "NULLABLE"},
            {"name": "timestamp", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "message", "type": "STRING", "mode": "NULLABLE"},
        ]
    }

    schema_log = {
        "fields": [
            {"name": "type", "type": "STRING", "mode": "NULLABLE"},
            {"name": "timestamp", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "query", "type": "STRING", "mode": "NULLABLE"},
        ]
    }

    schemas = [
        (tables[0][1], schema_error),
        (tables[0][1], schema_log),
    ]

    with beam.Pipeline() as p:

        # create p collection from data
        elements = p | "add data" >> beam.Create(data)
        # elements | "print data" >> beam.Map(print)

        table_names = p | "add tables" >> beam.Create(tables)
        # table_names | "print table_names" >> beam.Map(print)

        table_names_dict = beam.pvalue.AsDict(table_names)

        schemas_pcollection = p | "add schemas" >> beam.Create(schemas)
        schemas_dict = beam.pvalue.AsDict(schemas_pcollection)


        # -custom_gcs_temp_location, or pass method="STREAMING_INSERTS" to WriteToBigQuery.
        elements | WriteToBigQuery(
            table=lambda row, table_dict: table_dict[row["type"]],
            table_side_inputs=(table_names_dict,),
            method=WriteToBigQuery.Method.STREAMING_INSERTS,

            # doesnt work
            # schema=lambda row: schemas_dict[row["type"]]

            # doesnt work 
            schema=lambda dest, schema_map: schema_map.get(dest),
            schema_side_inputs=(schemas_dict,)
        )

if __name__ == "__main__":
    run(sys.argv)
