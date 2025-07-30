import time
import apache_beam as beam
from apache_beam.transforms.window import Sessions
from apache_beam.transforms import window, trigger
from datetime import datetime, timezone

class CalculateTimeOnPage(beam.PTransform):
    """Composite transform to calculate time spent on pages within user sessions.
    
    This transform windows events by session, groups them by session ID,
    and calculates the time difference between consecutive page views.
    """
    
    def __init__(self, window_size=60):
        """Initialize the transform with session window size.
        
        Args:
            window_size: Session gap size in seconds (default: 60)
        """
        self.window_size = window_size
    
    def expand(self, pcoll):
        """Apply the transform pipeline to calculate time on page.
        
        Args:
            pcoll: Input PCollection of page view events
            
        Returns:
            PCollection of time-on-page calculations
        """
        return (pcoll
                # Set event timestamp for windowing
                | 'Set Event Timestamp' >> beam.Map(self._set_event_timestamp)
                # Create session windows with triggers
                | 'Window' >> beam.WindowInto(
                    Sessions(gap_size=self.window_size),
                    trigger=trigger.Repeatedly(
                        trigger.AfterWatermark(
                            early=trigger.AfterProcessingTime(30)   # Trigger results every 30 seconds before the watermark closes
                        )
                    ),
                    accumulation_mode=trigger.AccumulationMode.ACCUMULATING,
                    allowed_lateness=0      # Allow 0 seconds of late data. No firing happens
                )
                # Group events by session ID
                | 'Key by Session' >> beam.Map(lambda x: (x['session_id'], x))
                | 'Group by Session' >> beam.GroupByKey()
                # Calculate time spent between consecutive events
                | 'Calculate Time' >> beam.ParDo(self._calculate_time_spent_with_window)
                )
    
    def _set_event_timestamp(self, element):
        """Set the event timestamp for windowing.
        
        Args:
            element: Event element with ga_event_dtm field
            
        Returns:
            TimestampedValue with proper timestamp
        """
        # Parse ISO timestamp and convert to Unix timestamp
        timestamp = datetime.fromisoformat(element['ga_event_dtm'].replace('Z', '+00:00'))
        return window.TimestampedValue(element, timestamp=timestamp.timestamp())

    def _calculate_time_spent_with_window(self, session_data, window=beam.DoFn.WindowParam, timestamp=beam.DoFn.TimestampParam):
        """Calculate time spent on each page within a session with window info.
        
        Args:
            session_data: Tuple of (session_id, list of events)
            window: Window parameter from Beam
            timestamp: Timestamp parameter from Beam
            
        Returns:
            List of time-on-page calculations with window info
        """
        session_id, events = session_data
        # Sort events by timestamp to ensure correct order
        events = sorted(events, key=lambda x: x['ga_event_dtm'])
        
        # Calculate time between consecutive events
        events_counter = len(events) +1
        for i in range(1, events_counter):
            
            # Only one event in session
            if (i == len(events)):
                previous = events[i-1]

                # Calculate time difference in seconds
                window_end_utc = window.end.to_utc_datetime().replace(tzinfo=timezone.utc)
                time_spent = (window_end_utc - datetime.fromisoformat(previous['ga_event_dtm'])).total_seconds()
                
                # Create result record with window info
                yield {
                    'session_id': session_id,
                    'page': f"{previous.get('page_title', '')}",
                    'time_spent_seconds': time_spent,
                    'timestamp': previous['ga_event_dtm'],
                    'window_start': window.start.to_utc_datetime().isoformat(),
                    'window_end': window.end.to_utc_datetime().isoformat(),
                    'element_timestamp': timestamp.to_utc_datetime().isoformat()
                }
            
            # Last event in session of multiple events
            elif (i == events_counter):
                previous = events[i-1]

                # Calculate time difference in seconds
                time_spent = (window.end.to_utc_datetime().isoformat() - 
                            datetime.fromisoformat(previous['ga_event_dtm'])).total_seconds()
                
                # Create result record with window info
                yield {
                    'session_id': session_id,
                    'page': f"{previous.get('page_title', '')}",
                    'time_spent_seconds': time_spent,
                    'timestamp': previous['ga_event_dtm'],
                    'window_start': window.start.to_utc_datetime().isoformat(),
                    'window_end': window.end.to_utc_datetime().isoformat(),
                    'element_timestamp': timestamp.to_utc_datetime().isoformat()
                }
            # Any event between the first and the last during a session
            elif i != events_counter:
                current = events[i]
                previous = events[i-1]

                # Calculate time difference in seconds
                time_spent = (datetime.fromisoformat(current['ga_event_dtm']) - 
                            datetime.fromisoformat(previous['ga_event_dtm'])).total_seconds()
                
                # Create result record with window info
                yield {
                    'session_id': session_id,
                    'page': f"{previous.get('page_title', '')}",
                    'time_spent_seconds': time_spent,
                    'timestamp': previous['ga_event_dtm'],
                    'window_start': window.start.to_utc_datetime().isoformat(),
                    'window_end': window.end.to_utc_datetime().isoformat(),
                    'element_timestamp': timestamp.to_utc_datetime().isoformat()
                }        
