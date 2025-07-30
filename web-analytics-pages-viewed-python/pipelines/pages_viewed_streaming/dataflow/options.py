# dataflow/options.py
from apache_beam.options.pipeline_options import PipelineOptions

class CustomPipelineOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--input_subscription', required=False, default='projects/aaronginder/subscriptions/pages_view_events_dp')
        parser.add_argument('--customer_id_filter', default=None)
