import asyncio

async def handle_message(message: str):
    yield "+PONG\r\n"

async def handle_request(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    
    try:
        while True:
            data = await reader.read(1024)
            message = data.decode()
            address = writer.get_extra_info("peername")
            print(f"Received {message!r} from {address!r}")
            
            async for response in handle_message(message):
                print(f"Send: {response!r}")
                response_data = response.encode()
                writer.write(response_data)
                await writer.drain()
    
    finally:
        print("Close the connection")
        writer.close()
        await writer.wait_closed()

async def main():

    server = await asyncio.start_server(
        handle_request, "0.0.0.0", 6379, reuse_port=True
    )

    address = ", ".join(str(sock.getsockname()) for sock in server.sockets)
    print(f"Serving on {address}")
    async with server:
        await server.serve_forever()

if __name__ == "__main__":
    asyncio.run(main())