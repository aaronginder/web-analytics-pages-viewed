# dataflow/transforms/validate.py
import apache_beam as beam
import json
import logging

class ValidateMessage(beam.DoFn):
    def process(self, element):
        try:
            data = json.loads(element.decode('utf-8'))
            
            # Required field validation
            if not data.get('user_pseudo_id'):
                yield beam.pvalue.TaggedOutput('invalid', f"Missing user_pseudo_id: {element}")
                return
                
            if not data.get('event_name'):
                yield beam.pvalue.TaggedOutput('invalid', f"Missing event_name: {element}")
                return
                
            # Format validation
            if data.get('event_timestamp') and not isinstance(data.get('event_timestamp'), (int, str)):
                yield beam.pvalue.TaggedOutput('invalid', f"Invalid timestamp format: {element}")
                return
                
            yield beam.pvalue.TaggedOutput('valid', data)
            
        except json.JSONDecodeError:
            yield beam.pvalue.TaggedOutput('invalid', f"Invalid JSON: {element}")
        except Exception as e:
            logging.error(f"Validation error: {e}")
            yield beam.pvalue.TaggedOutput('invalid', f"Validation error: {element}")
