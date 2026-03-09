from abc import ABC, abstractmethod
from typing import Any, List, Dict, Union, Optional


class DataStream(ABC):
    @abstractmethod
    def process_batch(self, data_batch: List[Any]) -> str:
        self.data_batch = data_batch
        return self.get_stats()

    def filter_data(self, data_batch: List[Any],
                    criteria: Optional[str] = None) -> List[Any]:
        if criteria is None:
            return data_batch
        return [item for item in data_batch if criteria in item]

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        return {
           "len": len(self.data_batch)
        }


class SensorStream(DataStream):
    def __init__(self, stream_id: str) -> None:
        self.stream_id = stream_id
        self.data_batch = None

    def process_batch(self, data_batch: List[Any]) -> str:
        self.data_batch = data_batch
        stats = self.get_stats()
        return (f"Sensor analysis: {stats['count']} readings"
                f" processed, avg temp: {stats['avg_tmp']}°C")

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        avg_tmp = [float(item.split(":")[1])
                   for item in self.data_batch if "temp" in item][0]
        return {
            "stream_type": "readings",
            "count": len(self.data_batch),
            "avg_tmp": avg_tmp,
            "data_type": "Sensor"
        }


class TransactionStream(DataStream):
    def __init__(self, stream_id: str) -> None:
        self.stream_id = stream_id
        self.data_batch = None

    def process_batch(self, data_batch: List[Any]) -> str:
        self.data_batch = data_batch
        stats = self.get_stats()
        return (f"Transaction analysis: {stats['count']}"
                f" operations, net flow: {stats['net_flow']} units")

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        buy_units = sum(int(unit.split(":")[1])
                        for unit in self.data_batch if "buy" in unit)
        sell_units = sum(int(unit.split(":")[1])
                         for unit in self.data_batch if "sell" in unit)
        if buy_units > sell_units:
            net_flow = buy_units - sell_units
            net_flow = f"+{net_flow}"
        else:
            net_flow = sell_units - buy_units
            net_flow = f"-{net_flow}"
        return {
            "stream_type": "operations",
            "count": len(self.data_batch),
            "net_flow": net_flow,
            "data_type": "Transaction"
        }


class EventStream(DataStream):
    def __init__(self, stream_id: str) -> None:
        self.stream_id = stream_id
        self.data_batch = None

    def process_batch(self, data_batch: List[Any]) -> str:
        self.data_batch = data_batch
        stats = self.get_stats()
        return (f"Event analysis: {stats['count']} events,"
                f"{stats['errors']} error detected")

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        errors = [error for error in self.data_batch if error == "error"]
        logs = [error for error in self.data_batch if error == "login"]
        logouts = [error for error in self.data_batch if error == "logout"]
        return {
            "stream_type": "events",
            "count": len(self.data_batch),
            "errors": len(errors),
            "logs": len(logs),
            "logouts": len(logouts),
            "data_type": "Event"
        }


class StreamProcessor():
    def __init__(self) -> None:
        self.streams: List[DataStream] = []

    def add_stream(self, data_stream: object):
        self.streams.append(data_stream)

    def process_all_batches(self, batches: List[List[Any]]) -> None:
        for stream, batch in zip(self.streams, batches):
            print(f"- {stream.get_stats()['data_type']} data: "
                  f"{stream.get_stats()['count']} "
                  f"{stream.get_stats()['stream_type']} processed")


def main() -> None:
    print("=== CODE NEXUS - POLYMORPHIC STREAM SYSTEM ===\n")

    sensor_batch = ["temp: 22.5", "humidity: 65", "pressure: 1013"]
    print("Initializing Sensor Stream...")
    stream_id = "SENSOR_001"
    print(f"Stream ID: {stream_id}, Type: Environmental Data")
    sensor_stream = SensorStream(stream_id)
    print(f"Processing sensor batch: [{', '.join(sensor_batch)}]")
    print(sensor_stream.process_batch(sensor_batch))

    print()
    transaction_batch = ["buy: 100", "sell: 150", "buy: 75"]
    print("Initializing Transaction Stream...")
    stream_id = "TRANS_001"
    print(f"Stream ID: {stream_id}, Type:  Financial Data")
    transaction_stream = TransactionStream(stream_id)
    print(f"Processing transaction batch: [{', '.join(transaction_batch)}]")
    print(transaction_stream.process_batch(transaction_batch))

    print()
    event_batch = ["login", "error", "logout"]
    print("Initializing Event Stream...")
    stream_id = "EVENT_001"
    event_stream = EventStream(stream_id)
    print(f"Stream ID: {stream_id}, Type:  System Events")
    print(f"Processing sensor batch: [{', '.join(event_batch)}]")
    print(event_stream.process_batch(event_batch))

    print()
    print("=== Polymorphic Stream Processing ===")
    print("Processing mixed stream types through unified interface...\n")

    print("Batch 1 Results:")
    sensor_batch = ["temp: 22.5", "temp: -10"]
    sensor_stream = SensorStream("SENSOR_001")
    sensor_stream.process_batch(sensor_batch)

    transaction_batch = ["buy: 100", "sell: 150", "buy: 75", "buy:150"]
    transaction_stream = TransactionStream(" TRANS_001")
    transaction_stream.process_batch(transaction_batch)

    event_batch = ["login", "error", "logout"]
    event_stream = EventStream("EVENT_001")
    event_stream.process_batch(event_batch)

    processor = StreamProcessor()
    processor.add_stream(sensor_stream)
    processor.add_stream(transaction_stream)
    processor.add_stream(event_stream)
    processor.process_all_batches([sensor_batch,
                                   transaction_batch, event_batch])

    print()
    print("Stream filtering active: High-priority data only")
    sensor_filtered = sensor_stream.filter_data(sensor_batch, "temp")
    transaction_filtered = transaction_stream.filter_data(
        transaction_batch, "sell")

    print(f"Filtered results: {len(sensor_filtered)} critical sensor alerts, "
          f"{len(transaction_filtered)} large transaction")

    print("\nAll streams processed successfully. Nexus throughput optimal.")


main()
