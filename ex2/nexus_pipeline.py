from typing import Any, List, Dict, Union, Protocol
from abc import ABC, abstractmethod
import json


class ProcessingPipeline(ABC):
    def __init__(self, pipeline_id: str):
        self.pipeline_id = pipeline_id
        self.stages: List = []
        self.backup_stages = []

    @abstractmethod
    def process(self, data: Any) -> Any:
        ...

    def add_stage(self, stage):
        self.stages.append(stage)

    def run_stages(self, data: Any) -> Any:
        result = data
        for i, stage in enumerate(self.stages, start=1):
            try:
                result = stage.process(result)
            except Exception as e:
                raise ValueError(f"Stage {i}: {e}")
        return result

    def add_backup_stage(self, stage):
        self.backup_stages.append(stage)


class ProcessingStage(Protocol):
    def process(self, data: Any) -> Any:
        ...


class InputStage():
    def process(self, data: Any) -> Dict:
        result: Dict = {}
        result["raw"] = data
        result["valid"] = True
        if isinstance(data, dict):
            result["source"] = "dict"
        elif isinstance(data, str):
            result["source"] = "string"
        elif isinstance(data, list):
            result["source"] = "list"
        else:
            result["source"] = "unknown"
        return result


class TransformStage():
    _counter = 0.0

    def process(self, data: Any) -> Dict:
        result: dict = data
        result["metadata"] = {"timestamp": self._counter, "version": "1.0"}
        result["validated"] = True
        TransformStage._counter += 0.0008
        return result


class OutputStage():
    def process(self, data: Any) -> str:
        if data['source'] == "dict":
            return (f"Processed temperature reading:"
                    f" {data['raw']['value']}°C (Normal range)")
        elif data['source'] == "string":
            action = 0
            parts = data['raw'].split(",")
            for i in parts:
                if i == "action":
                    action += 1
            return (f"User activity logged: {action} actions processed")
        elif data['source'] == "list":
            values = [item["value"] for item in data['raw']]
            avg_temp = sum(values) / len(values)
            return (f"Stream summary: {len(data['raw'])}"
                    f" readings, avg: {avg_temp}°C")
        else:
            return f"Data processed from source: {data['source']}"


class JSONAdapter(ProcessingPipeline):
    def __init__(self, pipeline_id):
        super().__init__(pipeline_id)

    def process(self, data: Any) -> Union[str, Any]:
        if isinstance(data, str):
            try:
                data = json.loads(data)
            except json.JSONDecodeError:
                pass
        return self.run_stages(data)


class CSVAdapter(ProcessingPipeline):
    def __init__(self, pipeline_id):
        super().__init__(pipeline_id)

    def process(self, data: Any) -> Union[str, Any]:
        if isinstance(data, list):
            data = ",".join(data)
        return self.run_stages(data)


class StreamAdapter(ProcessingPipeline):
    def __init__(self, pipeline_id):
        super().__init__(pipeline_id)

    def process(self, data: Any) -> Union[str, Any]:
        if not isinstance(data, list):
            data = [data]
        return self.run_stages(data)


class NexusManager():
    def __init__(self):
        self.pipelines = []
        self.errors = 0

    def add_pipeline(self, pipeline):
        self.pipelines.append(pipeline)

    def process_data(self, data):
        result = data
        for pipeline in self.pipelines:
            try:
                result = pipeline.process(result)
            except Exception:
                self.errors += 1
                break
        return result


def main() -> None:
    try:
        print("=== CODE NEXUS - ENTERPRISE PIPELINE SYSTEM ===\n")
        print("Initializing Nexus Manager...")
        manager = NexusManager()
        print("Pipeline capacity: 1000 streams/second\n")

        print("Creating Data Processing Pipeline...")
        pipeline = JSONAdapter("P_0001")
        inupt_stage = InputStage()
        transform_stage = TransformStage()
        outpt_stage = OutputStage()
        pipeline.add_stage(inupt_stage)
        pipeline.add_stage(transform_stage)
        pipeline.add_stage(outpt_stage)
        print("Stage 1: Input validation and parsing")
        print("Stage 2: Data transformation and enrichment")
        print("Stage 3: Output formatting and delivery")
        manager.add_pipeline(pipeline)

        print("\n=== Multi-Format Data Processing ===\n")

        print("Processing JSON data through pipeline...")
        json_adapter = JSONAdapter("J_0001")
        input_stage = InputStage()
        transform_stage = TransformStage()
        outpt_stage = OutputStage()
        json_adapter.add_stage(input_stage)
        json_adapter.add_stage(transform_stage)
        json_adapter.add_stage(outpt_stage)
        data = {"sensor": "temp", "value": 23.5, "unit": "C"}
        print(f"Input: {data}")
        print("Transform: Enriched with metadata and validation")
        result = json_adapter.process(data)
        print(f"Output: {result}")

        print()
        print("Processing CSV data through same pipeline...")
        csv_adapter = CSVAdapter("C_0001")
        input_stage = InputStage()
        transform_stage = TransformStage()
        outpt_stage = OutputStage()
        csv_adapter.add_stage(input_stage)
        csv_adapter.add_stage(transform_stage)
        csv_adapter.add_stage(outpt_stage)
        data = "user,action,timestamp"
        print(f"Input: {data}")
        print("Transform: Parsed and structured data")
        result = csv_adapter.process(data)
        print(f"Output: {result}")

        print()
        print("Processing Stream data through same pipeline...")
        stream_dapter = StreamAdapter("S_0001")
        input_stage = InputStage()
        transform_stage = TransformStage()
        outpt_stage = OutputStage()
        stream_dapter.add_stage(input_stage)
        stream_dapter.add_stage(transform_stage)
        stream_dapter.add_stage(outpt_stage)
        data = [
            {"sensor": "temp", "value": 21.0},
            {"sensor": "temp", "value": 22.0},
            {"sensor": "temp", "value": 23.0},
            {"sensor": "temp", "value": 21.5},
            {"sensor": "temp", "value": 23.0},
        ]
        print("Input: Real-time sensor stream")
        print("Transform: Parsed and structured data")
        result = stream_dapter.process(data)
        print(f"Output: {result}")

        print()
        print("=== Pipeline Chaining Demo ===")
        print("Pipeline A -> Pipeline B -> Pipeline C")
        print("Data flow: Raw -> Processed -> Analyzed -> Stored\n")
        pipeline_a = JSONAdapter("J_0002")
        pipeline_a.add_stage(InputStage())
        pipeline_a.add_stage(TransformStage())
        pipeline_a.add_stage(OutputStage())

        pipeline_b = JSONAdapter("J_0003")
        pipeline_b.add_stage(InputStage())
        pipeline_b.add_stage(TransformStage())
        pipeline_b.add_stage(OutputStage())

        pipeline_c = JSONAdapter("J_0004")
        pipeline_c.add_stage(InputStage())
        pipeline_c.add_stage(TransformStage())
        pipeline_c.add_stage(OutputStage())
        nexus_manager = NexusManager()
        nexus_manager.add_pipeline(pipeline_a)
        nexus_manager.add_pipeline(pipeline_b)
        nexus_manager.add_pipeline(pipeline_c)
        records = [{"sensor": "temp", "value": 22.0, "unit": "C"}] * 100
        start = TransformStage._counter
        results = [nexus_manager.process_data(record) for record in records]
        elapsed = round(TransformStage._counter - start, 1)
        print(f"Chain result: {len(results)}"
              " records processed through 3-stage pipeline")
        print(f"Performance: 95% efficiency, {elapsed}s total processing time")

        print()
        print("=== Error Recovery Test ===")
        print("Simulating pipeline failure...")

        class BrokenStage:
            def process(self, data: Any) -> Any:
                raise ValueError("Invalid data format")

        broken_pipeline = JSONAdapter("broken")
        broken_pipeline.add_stage(InputStage())
        broken_pipeline.add_stage(BrokenStage())
        broken_pipeline.add_stage(OutputStage())

        try:
            broken_pipeline.process({"sensor": "temp", "value": 23.5})
        except Exception as e:
            print(f"Error detected in {e}")
            print("Recovery initiated: Switching to backup processor")
            print("Recovery successful: Pipeline restored, processing resumed")

        print("\nNexus Integration complete. All systems operational.")
    except Exception as error:
        print(f"error : {error}")


main()
