# dataflow/transforms/filter.py
import apache_beam as beam

class FilterByCustomerId(beam.DoFn):
    def __init__(self, customer_id_filter=None):
        self.customer_id_filter = customer_id_filter
        
    def process(self, element):
        # if not self.customer_id_filter:
        #     yield element
        #     return
            
        if (element.get('customer_id') is not None and element.get('subscriber_number') is not None):
            yield element
