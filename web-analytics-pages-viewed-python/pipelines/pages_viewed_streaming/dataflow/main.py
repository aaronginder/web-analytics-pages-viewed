# dataflow/main.py
import os
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.transforms.window import Sessions
import json
import logging
import grpc

from options import CustomPipelineOptions
from transforms.validate import ValidateMessage
from transforms.filter import FilterByCustomerId
from transforms.aggregate import CalculateTimeOnPage


def run_pipeline(argv=None):
    pipeline_options = PipelineOptions(argv)
    custom_options = pipeline_options.view_as(CustomPipelineOptions)
    custom_options.view_as(StandardOptions).streaming = True
    
    # Configure gRPC channel options for proxy
    channel_options = [
        ('grpc.max_receive_message_length', 4 * 1024 * 1024),
        ('grpc.max_send_message_length', 4 * 1024 * 1024),
    ]
    
    # Set gRPC global channel options
    grpc.channel_ready_future = lambda channel: channel
    
    with beam.Pipeline(options=custom_options) as pipeline:
        
        # Read from Pub/Sub
        messages = (pipeline 
                   | 'Read from PubSub' >> ReadFromPubSub(
                       subscription="projects/aaronginder/subscriptions/pages_view_events_dp"
                       )
                   )
        
        # Validate messages
        validation_results = (messages 
                            | 'Validate Messages' >> beam.ParDo(ValidateMessage())
                              .with_outputs('valid', 'invalid')
                            )
        
        valid_messages = validation_results.valid
        invalid_messages = validation_results.invalid
        

        # Print to stdout
        # (valid_messages 
        #  | 'Format for Print' >> beam.Map(lambda x: json.dumps(x, indent=2))
        #  | 'Print to Stdout' >> beam.Map(print)
        # )
        
        # Log invalid messages
        (invalid_messages 
         | 'Log Invalid' >> beam.Map(lambda x: logging.warning(f"Invalid message: {x}"))
        )

        # Windowing
        aggregate_sessions = (
            valid_messages
            | 'Aggregate Sessions' >> CalculateTimeOnPage(window_size=60)
            | 'Print to Stdout' >>beam.Map(print)
        )


if __name__ == '__main__':
    debug = True

    if debug:
        # Set gRPC environment variables for proxy
        os.environ['GRPC_DEFAULT_SSL_ROOTS_FILE_PATH'] = ''
        os.environ['GRPC_ENABLE_FORK_SUPPORT'] = '1'
        logging.getLogger().setLevel(logging.INFO)
    else:
        logging.getLogger().setLevel(logging.INFO)
    
    run_pipeline()
