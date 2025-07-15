import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.kafka import ReadFromKafka

def decode_utf8(element) -> str:
    key, value = element
    return f"Key: {key.decode('utf-8') if key else None}, Value: {value.decode('utf-8') if value else None}"

def run():
    options = PipelineOptions(
        runner='DirectRunner',
        streaming=True
    )

