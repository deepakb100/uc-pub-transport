"""Defines trends calculations for stations"""
import logging
from dataclasses import dataclass

import faust


logger = logging.getLogger(__name__)


# Faust will ingest records from Kafka in this format
@dataclass
class Station(faust.Record):
    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: bool
    blue: bool
    green: bool


# Faust will produce records to Kafka in this format
@dataclass
class TransformedStation(faust.Record):
    station_id: int
    station_name: str
    order: int
    line: str


# Done: Define a Faust Stream that ingests data from the Kafka Connect stations topic and
#   places it into a new topic with only the necessary information.
app = faust.App("stations-stream", broker="kafka://localhost:9092")

# Done: Define the input Kafka Topic. Hint: What topic did Kafka Connect output to?
topic = app.topic("connect-stations", value_type=Station)
# Done: Define the output Kafka Topic
out_topic = app.topic("transformed-stations", partitions=1)
# Done: Define a Faust Table
table = app.Table("tbl-transformed-stations",
                  default=TransformedStation,
                  partitions=1,
                  changelog_topic=out_topic,
                  )


#
#
# Done: Using Faust, transform input `Station` records into `TransformedStation` records. Note that
# "line" is the color of the station. So if the `Station` record has the field `red` set to true,
# then you would set the `line` of the `TransformedStation` record to the string `"red"`
#
#
@app.agent(topic, consumer_auto_offset_reset='earliest')
async def station_event_processer(events):

    async for se in events:
        trenaformeed_station = TransformedStation(
            station_id=se.station_id,
            station_name=se.station_name,
            order=se.order,
            line=None
        )

        if se.red:
            trenaformeed_station.line = "red"
        elif se.blue:
            trenaformeed_station.line = "blue"
        else:
            trenaformeed_station.line = "green"

        table[se.station_id] = trenaformeed_station

if __name__ == "__main__":
    app.main()
