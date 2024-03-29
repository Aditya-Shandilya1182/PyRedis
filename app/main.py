import asyncio
import itertools
import time

from app.parsers import RedisParser, JSONParser

redisParser = RedisParser()
jsonParser = JSONParser()

store = {}

async def handle_message(msg: str | list):
    now = time.time()
    if isinstance(msg, str):
        if msg.lower() == "ping":
            yield "PONG"
    elif isinstance(msg, list):
        cmd, *args = msg
        cmd = cmd.lower()
        if cmd == "echo":
            yield "" if not args else args[0]
        elif cmd == "ping":
            yield "PONG"
        elif cmd == "set":
            key, val, *other = args
            set_args = dict(itertools.batched(other, 2))
            if "px" in set_args:
                exp = now + int(set_args["px"]) / 1000
            else:
                exp = None
            store[key] = (val, exp)
            yield "OK"
        elif cmd == "get":
            key, *__ = args
            if key not in store:
                yield None
                return
            val, exp = store[key]
            if exp and exp < now:
                del store[key]
                yield None
            else:
                yield val
        else:
            yield f"Unknown command: {cmd}"

async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    
    parser = redisParser

    try:
        while True:
            clientData = writer.get_extra_info("peername")
            data = await reader.read(1024)
            message = data.decode()

            if message.strip() == "hdnQUIT":
                break
            elif message.strip() == "hdnJSON":
                parser = jsonParser
                print("Switched to JSON parser for {clientData!r}")
                continue
            elif message.strip() == "hdnREDIS":
                parser = redisParser
                print("Switched to Redis parser for {clientData!r}")
                continue
            
            print(f"Received: {message!r} from {clientData!r}")
            parsedMessage = parser.parse(message)
            print(f"Parsed message: {parsedMessage!r}")

            async for response in handle_message(parsedMessage):
                print(f"Response: {response!r}")
                responseSerialized = parser.serialize(response)
                print(f"Sent response: {response!r} to {clientData!r}")
                responseData = responseSerialized.encode()
                writer.write(responseData)
                await writer.drain()
    
    finally:
        print("Close the connection")
        writer.close()
        await writer.wait_closed()

async def main():

    server = await asyncio.start_server(handle, "0.0.0.0", 6379, reuse_port=True)

    address = ", ".join(str(sock.getsockname()) for sock in server.sockets)
    print(f"Serving on {address}")
    async with server:
        await server.serve_forever()

if __name__ == "__main__":
    asyncio.run(main())