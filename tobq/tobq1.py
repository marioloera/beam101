#!/usr/bin/env python3
import apache_beam as beam
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
            "query": "flu symptom"
        },
    ]

    with beam.Pipeline() as p:

        # create p collection from data
        elements = p | "add data" >> beam.Create(data)
        # elements | "print data" >> beam.Map(print)

        table_names = p | "add tables" >> beam.Create(tables)
        # table_names | "print table_names" >> beam.Map(print)

        table_names_dict = beam.pvalue.AsDict(table_names)

        # -temp_location, or pass method="STREAMING_INSERTS" to WriteToBigQuery.
        elements | beam.io.gcp.bigquery.WriteToBigQuery(
            table=lambda row, table_dict: table_dict[row["type"]],
            table_side_inputs=(table_names_dict,)
        )


if __name__ == "__main__":
    run(sys.argv)
