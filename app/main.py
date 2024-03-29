import asyncio
import itertools
import time
import argparse
import functools

from app.parsers import RedisParser, JSONParser

redisParser = RedisParser()
jsonParser = JSONParser()

store = {}

async def handle_message(msg: str | list, node_config: dict):
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
        elif cmd == "info":
            kind = args[0] if args else None
            if kind == "replication":
                yield f"""
# Replication
role:{node_config['role']}
                """
            else:
                print(f"unknown INFO kind {kind!r}")
                yield f"ERR unknown or unsupported subcommand or invalid argument for 'INFO'."
        else:
            yield f"Unknown command: {cmd}"

async def handle_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter, node_config: dict):
    
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

            async for response in handle_message(parsedMessage, node_config):
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

    parser = argparse.ArgumentParser(description="Run the server")
    
    parser.add_argument(
        "--port", type=int, default=6379, help="The port to run the server on"
    )

    parser.add_argument(
        "--host", type = str, default = "0.0.0.0", help = "The host to run the server on"
    )

    parser.add_argument(
        "--replicaof", nargs=2, help="The master host and port to replicate from"
    )

    args = parser.parse_args()

    node_config = {
        "host": args.host,
        "port": args.port,
        "role": "master",
        "master_host": "",
        "master_port": "",
    }

    if args.replicaof:
        master_host, master_port = args.replicaof
        node_config["role"] = "slave"
        node_config["master_host"] = master_host
        node_config["master_port"] = master_port
        print(f"Replica of {master_host}:{master_port}")

    server = await asyncio.start_server(
        functools.partial(handle_client, node_config=node_config),
        "0.0.0.0",
        args.port,
        reuse_port=True,
    )

    address = ", ".join(str(sock.getsockname()) for sock in server.sockets)
    print(f"Serving on {address}")
    async with server:
        await server.serve_forever()

if __name__ == "__main__":
    asyncio.run(main())