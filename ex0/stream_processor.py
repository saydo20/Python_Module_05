from abc import ABC, abstractmethod
from typing import Any, List


class DataProcessor(ABC):
    @abstractmethod
    def process(self, data: Any) -> str:
        pass

    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass


class NumericProcessor():
    def process(self, data: Any) -> str:
        return f"Processing data: {data}"

    def validate(self, data: Any) -> bool:
        try:
            if isinstance(data, list) and all(
               isinstance(x, int) for x in data):
                print("Validation: Numeric data verified")
                return True
            else:
                print("the data has not be verified")
                return False
        except Exception as error:
            print(f"error : {error}")
            return False

    def format_output(self, result: str) -> str:
        try:
            avg = sum(result) / len(result)
            str1 = f"Output: Processed {len(result)} "
            str2 = f"numeric values, sum={sum(result)}, avg={avg}"
            return str1 + str2
        except Exception as error:
            print(f"error: {error}")


class TextProcessor():
    def process(self, data: Any) -> str:
        return f"Processing data: {data}"

    def validate(self, data: List[int]) -> bool:
        try:
            if isinstance(data, str):
                print("Validation: Text data verified")
                return True
            else:
                print("the data has not be verified")
                return False
        except Exception as error:
            print(f"error: {error}")

    def format_output(self, result: str) -> str:
        try:
            words = len(result.split())
            str1 = f"Output: Processed text: {len(result)}"
            str2 = f"characters, {words} words"
            return str1 + str2
        except Exception as error:
            print(f"error: {error}")


class LogProcessor():
    def process(self, data: Any) -> str:
        return f"Processing data: {data}"

    def validate(self, data: Any) -> bool:
        if data.startswith(("ERROR", "INFO")):
            print("Validation: Log entry verified")
            return True
        else:
            print("the data has not be verified")
            return False

    def format_output(self, result: str) -> str:
        try:
            if result.upper().startswith("ERROR"):
                message = result.split("ERROR")[1]
                str1 = "Output: [ALERT] ERROR level detected: "
                str2 = f"{message.strip()}"
                return str1 + str2
            elif result.upper().startswith("INFO"):
                message = result.split("INFO")[1]
                str1 = "Output: [INFO] INFO level detected: "
                str2 = f"{result.strip()}"
                return str1 + str2
        except Exception as error:
            print(f"error: {error}")


def main() -> None:
    print("=== CODE NEXUS - DATA PROCESSOR FOUNDATION ===\n")
    print("Initializing Numeric Processor...")
    numric_data = [1, 2, 3, 4, 5]
    numric_proces = NumericProcessor()
    print(numric_proces.process(numric_data))
    numric_proces.validate(numric_data)
    print(numric_proces.format_output(numric_data))
    print()
    print("Initializing Text Processor...")
    text_data = "Hello Nexus World"
    text_proces = TextProcessor()
    print(text_proces.process(text_data))
    text_proces.validate(text_data)
    print(text_proces.format_output(text_data))
    print()
    print("Initializing Log Processor...")
    log_data = "ERROR: Connection timeout"
    log_proces = LogProcessor()
    print(log_proces.process(log_data))
    log_proces.validate(log_data)
    print(log_proces.format_output(log_data))
    print("\n=== Polymorphic Processing Demo ===")
    print("Processing multiple data types through same interface...")


main()
