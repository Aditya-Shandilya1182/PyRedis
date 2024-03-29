import json

class RedisParser:

    def parse(self, data):

        lines = data.split("\r\n")
        print(lines)
        return self._parse(lines)
    
    def _parse(self, lines):

        line = lines.pop(0)
        prefix = line[0]

        if prefix == "+":
            return line[1:]
        
        elif prefix == "-":
            return Exception(line[1:])
        
        elif prefix == ":":
            return int(line[1:])
        
        elif prefix == "$":
            length = int(line[1:])
            if length == -1:
                return None
            else:
                return lines.pop(0)
        
        elif prefix == "*":
            length = int(line[1:])
            if length == -1:
                return None
            else:
                return [self._parse(lines) for _ in range(length)]

    def serialize(self, data):

        if isinstance(data, str):
            return f"+{data}\r\n"
        
        elif isinstance(data, Exception):
            return f"-{data}\r\n"
        
        elif isinstance(data, int):
            return f":{data}\r\n"
        
        elif data is None:
            return "$-1\r\n"
        
        elif isinstance(data, list):
            serialized_list = "".join(self.serialize(item) for item in data)
            return f"*{len(data)}\r\n{serialized_list}"
        
        else:
            str_data = str(data)
            return f"${len(str_data)}\r\n{str_data}\r\n"
        
class JSONParser:
    def parse(self, msg: str) -> str | list | None:
        if not msg:
            return None
        return json.loads(msg)
    def serialize(self, response: str | list) -> str:
        return json.dumps(response)