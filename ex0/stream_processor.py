from abc import ABC, abstractmethod
from typing import Any, List


class DataProcessor(ABC):
    @abstractmethod
    def process(self, data: Any) -> str:
        pass

    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    def format_output(self, result: str) -> str:
        return result

class NumericProcessor(DataProcessor):
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

    def process(self, data: Any) -> str:
        try:
            avg = sum(data) / len(data)
            output = (f"Output: Processed {len(data)} "
                      f"numeric values, sum={sum(data)}, avg={avg}")
            return super().format_output(output)
        except Exception as error:
            print(f"error: {error}")

    
class TextProcessor(DataProcessor):
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

    def process(self, data: Any) -> str:
        try:
            words = len(data.split())
            output = (f"Output: Processed text: {len(data)} "
                      f"characters, {words} words")
            return super().format_output(output)
            
        except Exception as error:
            print(f"error: {error}")


class LogProcessor(DataProcessor):
    def process(self, data: Any) -> str:
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

    def validate(self, data: Any) -> bool:
        if data.startswith(("ERROR", "INFO")):
            print("Validation: Log entry verified")
            return True
        else:
            print("the data has not be verified")
            return False


def main() -> None:
    print("=== CODE NEXUS - DATA PROCESSOR FOUNDATION ===\n")
    print("Initializing Numeric Processor...")
    numric_data = [1, 2, 3, 4, 5]
    print(f"Processing data: {numric_data}")
    numeric_proces = NumericProcessor()
    numeric_proces.validate(numric_data)
    print(numeric_proces.process(numric_data))
    print()
    print("Initializing Text Processor...")
    text_data = "Hello Nexus World"
    print(f'Processing data: "{text_data}"')
    text_proces = TextProcessor()
    text_proces.validate(text_data)
    print(text_proces.process(text_data))
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
