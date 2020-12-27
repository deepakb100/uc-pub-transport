"""Contains functionality related to Weather"""
import logging
import json

logger = logging.getLogger(__name__)


class Weather:
    """Defines the Weather model"""

    def __init__(self):
        """Creates the weather model"""
        self.temperature = 70.0
        self.status = "sunny"

    def process_message(self, message):
        """Handles incoming weather data"""
        #
        #
        # Done: Process incoming weather messages. Set the temperature and status.
        #
        #
        weather_dict = json.loads(message.value())
        self.temperature = weather_dict['temperature']
        self.status = weather_dict['status']
