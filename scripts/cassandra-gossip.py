#!/usr/bin/env python3
"""Print when the nodes saw each other go down and come back.

    scripts/cassandra-gossip.py SINCE [UNTIL]

SINCE and UNTIL are UTC times, `YYYY-MM-DD HH:MM:SS`. Reads the system.log
of every node in $CASSANDRA_DATA_DIR (the nodes log in UTC) and prints the
gossip lines `InetAddress ... is now DOWN|UP` between those times, in time
order, each with the node that logged it.
"""
import os
import re
import sys
from pathlib import Path

data = Path(os.environ.get("CASSANDRA_DATA_DIR", ".cassandradata"))
since = sys.argv[1]
until = sys.argv[2] if len(sys.argv) > 2 else "9999"
line_re = re.compile(r"^INFO +\[[^\]]*\] (\S+ \S+) Gossiper\.java:\d+ - InetAddress /([\d.]+):\d+ is now (DOWN|UP)")
events = []
for n in (1, 2, 3):
    log = data / f"node{n}" / "logs" / "system.log"
    for line in log.read_text(encoding="utf-8", errors="replace").splitlines():
        if (m := line_re.match(line)) and since <= m[1] < until:
            peer = int(m[2].rsplit(".", 1)[1]) - 10
            events.append((m[1], f"node{n}: node{peer} is now {m[3]}"))
for when, what in sorted(events):
    print(f"{when}  {what}")
