#!/usr/bin/env python3
"""The mongod log lines of one failover, merged from all nodes in time order.

    mongo-failover-excerpt.py --title TEXT --data-dir .mongodata \
        --log 1=reports/logs/task4/mongo-node1.log:1234 --log 2=...:567 ...

Each --log names a node, its log and the line the run began at (1-based); only
later lines are read. Kept: restarts, elections, state transitions, the first
failed heartbeat to a node, crash recovery (oplog truncation, replay) and
every ROLLBACK line. Rollback data files (<dbpath>/rollback/**/*.bson)
written after the run began are listed and decoded with a small BSON reader
(the image has no bsondump).

Prints the excerpt to standard output; the last line is a summary for the
calling script: "rollback_lines=N rollback_documents=M".
"""
import argparse
import datetime
import json
import os
import re
import struct
import sys

KEEP_IDS = {
    20698,  # ***** SERVER RESTARTED *****
    8423403,  # mongod startup complete
    4615652,  # Starting an election, since we've seen no PRIMARY in election timeout period
    21438,  # Conducting a dry run election
    21444,  # Dry election run succeeded, running for election
    21450,  # Election succeeded, assuming primary role
    23980,  # Responding to vote request
    21216,  # Member is now in state DOWN
    21215,  # Member is in new state
    21358,  # Replica set state transition
    21359,  # Entering primary catch-up mode
    21364,  # Caught up to the latest optime known via heartbeats
    21331,  # Transition to primary complete; database writes are now permitted
    501401,  # Incrementing the rollback ID after unclean shutdown
    21557,  # Removing unapplied oplog entries after oplogTruncateAfterPoint
    21553,  # Truncating oplog from truncateAfterOplogEntryTimestamp
    21544,  # Recovering from stable timestamp
    21536,  # Completed oplog application for recovery
    4280512,  # No initial sync required. Attempting to begin steady replication
    21088,  # Changed sync source
}
FIRST_ONLY_IDS = {23974}  # Heartbeat failed after max retries (every 2 s)
DROP_ATTRS = {"durationMillis", "isInitialSyncFlagSet", "replicaSetMonitorProtocol"}
ATTR_WIDTH = 220  # attributes longer than this are cut (ROLLBACK lines never)


def simplify(value):
    """Extended JSON to something short: timestamps as t:i, dates as text."""
    if isinstance(value, dict):
        if set(value) == {"$timestamp"}:
            return f"{value['$timestamp']['t']}:{value['$timestamp']['i']}"
        if set(value) == {"$date"}:
            return value["$date"]
        if set(value) == {"$numberLong"}:
            return int(value["$numberLong"])
        return {k: simplify(v) for k, v in value.items() if k not in DROP_ATTRS}
    if isinstance(value, list):
        return [simplify(v) for v in value]
    return value


def read_events(node, path, start_line):
    events, seen_first = [], set()
    with open(path, encoding="utf-8", errors="replace") as log:
        for number, line in enumerate(log, 1):
            if number < start_line:
                continue
            try:
                entry = json.loads(line)
            except json.JSONDecodeError:
                continue
            message_id = entry.get("id")
            component = entry.get("c")
            keep = component == "ROLLBACK" or message_id in KEEP_IDS
            if message_id in FIRST_ONLY_IDS and message_id not in seen_first:
                seen_first.add(message_id)
                keep = True
            if keep:
                events.append((entry["t"]["$date"], node, component, entry))
    return events


def render(event):
    stamp, node, component, entry = event
    attrs = simplify(entry.get("attr", {}))
    text = json.dumps(attrs, ensure_ascii=False, separators=(",", ":")) if attrs else ""
    if component != "ROLLBACK" and len(text) > ATTR_WIDTH:
        text = text[: ATTR_WIDTH - 3] + "..."
    return f"{stamp[11:23]}  node{node}  {component:<8} {entry['msg']}" + (f"  {text}" if text else "")


# A BSON reader for the rollback files: enough for the documents of this lab.
def bson_document(data, offset=0):
    (length,) = struct.unpack_from("<i", data, offset)
    end, position, result = offset + length - 1, offset + 4, {}
    while position < end:
        kind = data[position]
        position += 1
        name_end = data.index(b"\x00", position)
        name = data[position:name_end].decode()
        position = name_end + 1
        value, position = bson_value(kind, data, position)
        result[name] = value
    return result, offset + length


def bson_value(kind, data, position):
    if kind == 0x01:
        return struct.unpack_from("<d", data, position)[0], position + 8
    if kind == 0x02:
        (length,) = struct.unpack_from("<i", data, position)
        return data[position + 4 : position + 3 + length].decode(), position + 4 + length
    if kind in (0x03, 0x04):
        document, after = bson_document(data, position)
        return (list(document.values()) if kind == 0x04 else document), after
    if kind == 0x05:
        (length,) = struct.unpack_from("<i", data, position)
        return data[position + 5 : position + 5 + length].hex(), position + 5 + length
    if kind == 0x07:
        return data[position : position + 12].hex(), position + 12
    if kind == 0x08:
        return bool(data[position]), position + 1
    if kind == 0x09:
        (millis,) = struct.unpack_from("<q", data, position)
        moment = datetime.datetime.fromtimestamp(millis / 1000, datetime.timezone.utc)
        return moment.isoformat(timespec="milliseconds"), position + 8
    if kind == 0x0A:
        return None, position
    if kind == 0x10:
        return struct.unpack_from("<i", data, position)[0], position + 4
    if kind == 0x11:
        increment, seconds = struct.unpack_from("<II", data, position)
        return f"{seconds}:{increment}", position + 8
    if kind == 0x12:
        return struct.unpack_from("<q", data, position)[0], position + 8
    raise ValueError(f"BSON type 0x{kind:02x} is not supported")


def rollback_files(data_dir, since):
    found = []
    for root, _, files in os.walk(data_dir):
        if os.sep + "rollback" not in root + os.sep:
            continue
        for name in files:
            path = os.path.join(root, name)
            if name.endswith(".bson") and os.path.getmtime(path) >= since:
                found.append(path)
    return sorted(found)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--title", required=True)
    parser.add_argument("--log", action="append", required=True, metavar="N=PATH:LINE")
    parser.add_argument("--data-dir", required=True)
    parser.add_argument("--since", type=float, required=True, help="Unix time the run began (for rollback files)")
    args = parser.parse_args()

    events = []
    for spec in args.log:
        match = re.fullmatch(r"(\d+)=(.+):(\d+)", spec)
        if not match:
            sys.exit(f"bad --log {spec!r}")
        events += read_events(int(match[1]), match[2], int(match[3]))
    events.sort(key=lambda event: (event[0], event[1]))

    print(args.title)
    print("Рядки журналів mongod усіх трьох вузлів від початку прогону, за часом (UTC).")
    print()
    for event in events:
        print(render(event))

    rollback_lines = sum(1 for event in events if event[2] == "ROLLBACK")
    documents = 0
    files = rollback_files(args.data_dir, args.since)
    print()
    if files:
        print("Файли rollback:")
        for path in files:
            data = open(path, "rb").read()
            offset, decoded = 0, []
            while offset < len(data):
                document, offset = bson_document(data, offset)
                decoded.append(document)
            documents += len(decoded)
            print(f"  {os.path.relpath(path, args.data_dir)}: {len(decoded)} документ(и)")
            for document in decoded:
                print("    " + json.dumps(document, ensure_ascii=False))
    else:
        print("Файлів rollback немає.")
    print()
    print(f"rollback_lines={rollback_lines} rollback_documents={documents}")


if __name__ == "__main__":
    main()
