#!/usr/bin/env python3
"""Run cqlsh interactively and print what the terminal would show.

    scripts/cql-session.py HOST PORT [--timeout S] < statements.cql

Reads CQL from stdin. A statement may span lines and ends with `;`; cqlsh's
own commands (CONSISTENCY, DESCRIBE, USE, ...) may also end at the line end.
Lines starting with `--` are comments: they go into the transcript as `# `
notes and are not sent. Every statement is typed into cqlsh running on a
pseudo-terminal, so the transcript shows the real prompt (`cqlsh:ks> `) in
front of the input and the output as cqlsh printed it, and session state
such as CONSISTENCY carries over from one statement to the next, which
`cqlsh -e` cannot do.
"""
import argparse
import fcntl
import os
import pty
import re
import select
import shutil
import struct
import sys
import termios
import time

PROMPT = re.compile(rb"\r?\n?(cqlsh(:\w+)?> |\s+\.\.\. )$")
CONTINUATION = "   ... "
ANSI = re.compile(rb"\x1b\[[0-9;?]*[A-Za-z]")
CQLSH_COMMANDS = ("CONSISTENCY", "SERIAL", "DESCRIBE", "DESC", "USE", "TRACING", "EXPAND", "PAGING", "SHOW", "EXIT")


def statements(text):
    """Splits stdin into ("note", text) and ("cql", statement) items."""
    pending = []
    for line in text.splitlines():
        stripped = line.strip()
        if not pending and stripped.startswith("--"):
            yield "note", stripped[2:].strip()
            continue
        if not pending and not stripped:
            yield "gap", ""
            continue
        pending.append(line)
        joined = " ".join(l.strip() for l in pending)
        first = joined.split(None, 1)[0].upper().rstrip(";")
        if joined.endswith(";") or (first in CQLSH_COMMANDS and len(pending) == 1):
            yield "cql", [l.rstrip() for l in pending]
            pending = []
    if pending:
        yield "cql", [l.rstrip() for l in pending]


class Cqlsh:
    def __init__(self, host, port, timeout):
        self.timeout = timeout
        self.pid, self.fd = pty.fork()
        if self.pid == 0:
            os.environ["TERM"] = "dumb"
            os.environ["COLUMNS"] = "200"
            cqlsh = shutil.which("cqlsh")
            os.execv(cqlsh, ["cqlsh", "--no-color", f"--request-timeout={int(timeout)}", host, str(port)])
        # A wide terminal, so cqlsh never scrolls a long input line sideways.
        fcntl.ioctl(self.fd, termios.TIOCSWINSZ, struct.pack("HHHH", 50, 1000, 0, 0))
        self.prompt = self.read_until_prompt()[1]

    def read_until_prompt(self):
        buffer = b""
        deadline = time.monotonic() + self.timeout + 10
        while time.monotonic() < deadline:
            ready, _, _ = select.select([self.fd], [], [], 0.5)
            if not ready:
                continue
            try:
                chunk = os.read(self.fd, 65536)
            except OSError:
                break
            if not chunk:
                break
            buffer += chunk
            clean = ANSI.sub(b"", buffer).replace(b"\r", b"")
            match = PROMPT.search(clean)
            if match:
                return clean[: match.start()].decode(errors="replace"), match.group(1).decode()
        return ANSI.sub(b"", buffer).replace(b"\r", b"").decode(errors="replace"), None

    def run(self, lines):
        """Types the statement line by line; returns the output after the last line."""
        for line in lines:
            os.write(self.fd, line.encode() + b"\n")
            output, prompt = self.read_until_prompt()
            self.prompt = prompt
            if prompt is None:
                break
        # The terminal echoes what was typed; drop the echo.
        rows = output.split("\n")
        if rows and rows[0].strip() == lines[-1].strip():
            rows.pop(0)
        return "\n".join(rows).strip("\n")

    def close(self):
        try:
            os.write(self.fd, b"EXIT\n")
        except OSError:
            pass
        os.waitpid(self.pid, 0)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("host")
    parser.add_argument("port")
    parser.add_argument("--timeout", type=float, default=20)
    args = parser.parse_args()
    shell = Cqlsh(args.host, args.port, args.timeout)
    if shell.prompt is None:
        print(f"cqlsh could not connect to {args.host}:{args.port}")
        shell.close()
        sys.exit(1)
    out = []
    for kind, text in statements(sys.stdin.read()):
        if kind == "note":
            out.append(f"# {text}")
        elif kind == "gap":
            out.append("")
        else:
            prompt = shell.prompt
            result = shell.run(text)
            out.append(prompt + text[0].strip())
            out.extend(CONTINUATION + line.rstrip() if line.strip() else CONTINUATION.rstrip() for line in text[1:])
            if result.strip():
                out.append(result)
            if shell.prompt is None:
                out.append("(cqlsh stopped)")
                break
    shell.close()
    print("\n".join(out))


if __name__ == "__main__":
    main()
