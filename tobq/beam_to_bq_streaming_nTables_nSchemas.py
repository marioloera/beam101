"""
    This beam pipeline takes data elements and stream insert in bigquery 
    there different records with three different tables with own schema
    schema is optional if thables exits
"""

#!/usr/bin/env python3
import datetime
import apache_beam as beam
from apache_beam.io.gcp.bigquery import WriteToBigQuery
import sys


def run(argv):

    project_id = "trustly-ds-test-1"
    dataset_id = "mario24"
    gcs_bucket = "gs://trustly-ds-test-1_mario24"

    tables_kv_pairs = [
        ("error", f"{project_id}:{dataset_id}.error_table9"),
        ("user_log", f"{project_id}:{dataset_id}.query_table9"),
        ("event", f"{project_id}:{dataset_id}.event_table9"),
    ]

    data = [
        {
            "type": "error",
            "timestamp": str(datetime.datetime.now()),
            "message1": "bad"
        },
        {
            "type": "user_log",
            "timestamp": str(datetime.datetime.now()),
            "message2": "flu symptom"
        },
        {
            "type": "event",
            "timestamp": str(datetime.datetime.now()),
            "message3": "loggin"
        },
    ]

    schema1 = {
        "fields": [
            {"name": "type", "type": "STRING", "mode": "NULLABLE"},
            {"name": "timestamp", "type": "STRING", "mode": "NULLABLE"},
            {"name": "message1", "type": "STRING", "mode": "NULLABLE"},
        ]
    }

    schema2 = {
        "fields": [
            {"name": "type", "type": "STRING", "mode": "NULLABLE"},
            {"name": "timestamp", "type": "STRING", "mode": "NULLABLE"},
            {"name": "message2", "type": "STRING", "mode": "NULLABLE"},
        ]
    }

    schema3 = {
        "fields": [
            {"name": "type", "type": "STRING", "mode": "NULLABLE"},
            {"name": "timestamp", "type": "STRING", "mode": "NULLABLE"},
            {"name": "message3", "type": "STRING", "mode": "NULLABLE"},
        ]
    }

    schema_kv_pairs = [
        (tables_kv_pairs[0][1], schema1),
        (tables_kv_pairs[1][1], schema2),
        (tables_kv_pairs[2][1], schema3),
    ]

    # check mapping
    for s in schema_kv_pairs:
        print(s[0], s[1]["fields"][2]["name"])

    with beam.Pipeline() as p:

        # create p collection from data
        elements = p | "add data" >> beam.Create(data)
        # elements | "print data" >> beam.Map(print)

        schema_map_pcv = beam.pvalue.AsDict(
            p | "MakeSchemas" >> beam.Create(schema_kv_pairs))

        table_record_pcv = beam.pvalue.AsDict(
            p | "MakeTables" >> beam.Create(tables_kv_pairs))

        elements | WriteToBigQuery(
            table=lambda row, table_dict: table_dict[row["type"]],
            table_side_inputs=(table_record_pcv,),
            schema=lambda dest,
            schema_map: schema_map.get(dest, None),
            schema_side_inputs=(schema_map_pcv, ),
            # method=WriteToBigQuery.Method.STREAMING_INSERTS
            custom_gcs_temp_location=gcs_bucket
        )


if __name__ == "__main__":
    run(sys.argv)
