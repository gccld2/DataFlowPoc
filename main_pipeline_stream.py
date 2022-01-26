import argparse
import json
from datetime import datetime
import logging

import apache_beam as beam
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


class DateFormatter(beam.DoFn):
    def process(self, element: bytes):
        """
        Simple processing function to parse the data and add a timestamp
        """
        parsed = json.loads(element.decode("utf8"))
        parsed["date_time"] = datetime.fromtimestamp(parsed['ts']).strftime("%Y-%m-%d %H:%M:%S")
        yield parsed


def run(argv=None, save_main_session=True):
    """Main entry point; defines and runs the wordcount pipeline."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input_topic',
        dest='input_topic',
        help='Input topic to read from')
    parser.add_argument(
        '--output_table',
        dest='output_table',
        required=True,
        help='Output bigQuery table to write results to')
    parser.add_argument(
        "--output_schema",
        dest='output_schema',
        help="Output BigQuery Schema in text format",
        required=True,
    )
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    # The pipeline will be run on exiting the with block.
    with beam.Pipeline(options=pipeline_options) as p:
        (
                p
                | "Read raw data from Topic" >> ReadFromPubSub(topic=known_args.input_topic).with_output_types(bytes)
                | "Format Date" >> beam.ParDo(DateFormatter())
                | "WriteToBigQuery" >> beam.io.WriteToBigQuery(known_args.output_table,
                                                               schema=known_args.output_schema,
                                                               write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,)
        )

    return p.run()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    try:
        pipeline = run()
        print("\n PIPELINE RUNNING \n")
    except (KeyboardInterrupt, SystemExit):
        raise
    # except:
    #     print("\n PIPELINE FAILED")
