from apache_beam.options.pipeline_options import PipelineOptions

class CustomPipelineOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--input_topic', required=True, help='Input topic to read from.', type=str, default=None, dest='input_topic')
        parser.add_argument('--output_topic', required=True, help='Output topic to write to.', type=str, default=None, dest='output_topic')
        parser.add_argument('--runner', required=False, help='Runner to use for the pipeline.', type=str, default='DirectRunner', dest='runner')

    