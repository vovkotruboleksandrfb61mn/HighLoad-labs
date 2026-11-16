#!/usr/bin/env python3
"""The network between the Cassandra nodes, with a switch to cut it.

Every node binds its internode port on 127.0.1.N but tells the others
(broadcast_address) to reach it at 127.0.0.(10+N). This proxy listens on the
broadcast addresses and forwards each connection to the node, so all traffic
between nodes passes through it, while clients still talk to the nodes
directly (the native port is not proxied).

    netsplit.py serve           run the proxy (scripts/cassandra.sh starts it)
    netsplit.py split           cut the network: no node reaches another
    netsplit.py heal            restore it
    netsplit.py status          print the state

Splitting closes the open internode connections and refuses new ones, which
every node sees as lost peers, as if the cables were pulled. The cut is all
or nothing: Cassandra does not bind its outgoing connections to its own
address, so the proxy cannot tell which node opened a connection.

Commands reach the serving proxy through a unix socket, $CASSANDRA_DATA_DIR/netsplit.sock.
"""
import asyncio
import os
import sys

NODES = (1, 2, 3)
PORT = int(os.environ.get("CASSANDRA_STORAGE_PORT", "21450"))
SOCKET = os.path.join(os.environ.get("CASSANDRA_DATA_DIR", ".cassandradata"), "netsplit.sock")


def public(node):
    return f"127.0.0.{10 + node}"


def private(node):
    return f"127.0.1.{node}"


class Proxy:
    def __init__(self):
        self.cut = False
        self.open = set()  # (client writer, node writer)

    async def accept(self, target, reader, writer):
        if self.cut:
            writer.transport.abort()
            return
        try:
            up_reader, up_writer = await asyncio.open_connection(private(target), PORT)
        except OSError:
            writer.close()
            return
        entry = (writer, up_writer)
        self.open.add(entry)
        try:
            await asyncio.gather(pipe(reader, up_writer), pipe(up_reader, writer))
        finally:
            self.open.discard(entry)
            for w in entry:
                w.close()

    def split(self):
        self.cut = True
        for down, up in list(self.open):
            down.transport.abort()
            up.transport.abort()

    async def command(self, reader, writer):
        word = (await reader.readline()).decode().strip()
        if word == "split":
            self.split()
        elif word == "heal":
            self.cut = False
        writer.write((self.describe() + "\n").encode())
        await writer.drain()
        writer.close()

    def describe(self):
        return "split: no node reaches another" if self.cut else f"links up, {len(self.open)} connections"


async def pipe(reader, writer):
    try:
        while data := await reader.read(65536):
            writer.write(data)
            await writer.drain()
    except (ConnectionError, OSError):
        pass
    finally:
        writer.close()


async def serve():
    proxy = Proxy()
    servers = []
    for node in NODES:
        servers.append(await asyncio.start_server(
            lambda r, w, n=node: proxy.accept(n, r, w), public(node), PORT))
    if os.path.exists(SOCKET):
        os.unlink(SOCKET)
    servers.append(await asyncio.start_unix_server(proxy.command, SOCKET))
    print(f"netsplit: proxying 127.0.0.11-13:{PORT} -> 127.0.1.1-3:{PORT}", flush=True)
    await asyncio.gather(*(s.serve_forever() for s in servers))


async def send(line):
    reader, writer = await asyncio.open_unix_connection(SOCKET)
    writer.write((line + "\n").encode())
    await writer.drain()
    print((await reader.read()).decode().strip())


def main():
    if len(sys.argv) < 2 or sys.argv[1] not in ("serve", "split", "heal", "status"):
        print(__doc__.strip(), file=sys.stderr)
        sys.exit(2)
    if sys.argv[1] == "serve":
        asyncio.run(serve())
    else:
        asyncio.run(send(sys.argv[1]))


if __name__ == "__main__":
    main()
