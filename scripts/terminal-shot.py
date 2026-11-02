#!/usr/bin/env python3
"""Render a terminal transcript as a PNG that looks like a terminal window.

    scripts/terminal-shot.py TRANSCRIPT.txt OUT.png [--width 1400]
                             [--title TEXT] [--blocks FIRST-LAST]

The transcript is plain text as the Task 4 scripts write it:
  # comment                          a note, dimmed
  [12:00:00.000] $ command           a shell command with its UTC time
  rs0 [primary] lab4> input          a mongosh prompt with the typed input
  anything else                      output; errors are shown in red

Blocks are the runs of lines between blank lines; --blocks 4-6 renders only
the 4th to the 6th of them, so that a long transcript can go into a report
as several pictures of a readable size.

Headless Chromium draws the page twice: once to measure the height the text
needs at the given width, then to take the screenshot at exactly that size.
Only the standard library and a `chromium` on PATH are needed.
"""

from __future__ import annotations

import argparse
import html
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

SCALE = 2  # device pixels per CSS pixel, for a sharp picture in the PDF

SHELL = re.compile(r"^\[(\d\d:\d\d:\d\d\.\d{3})\] \$ (.*)$")
MONGOSH = re.compile(r"^(rs0 \[[^\]]*\] [\w.-]+> )(.*)$")
ERROR = re.compile(
    r"^(Uncaught:|Mongo\w*Error|MongoServerError|\(exit \d+\)|no PRIMARY|.*did not catch up)"
)

CSS = """
html, body { margin: 0; padding: 0; background: #14161a; }
.window {
  margin: 0; background: #1e2127; border-radius: 0;
  font-family: "Fira Code", "Source Code Pro", "Noto Sans Mono", "Liberation Mono", monospace;
  font-variant-ligatures: none; font-size: 15px; line-height: 1.42;
}
.bar {
  display: flex; align-items: center; gap: 8px; padding: 10px 14px;
  background: #2a2e36; border-bottom: 1px solid #3a3f4b;
}
.dot { width: 12px; height: 12px; border-radius: 50%; }
.title { color: #a4abb8; font-size: 13px; margin-left: 10px; }
pre {
  margin: 0; padding: 14px 18px 18px; color: #c8ccd4;
  white-space: pre-wrap; overflow-wrap: anywhere; font: inherit;
}
.comment { color: #7f8c98; font-style: italic; }
.time { color: #6b7380; }
.dollar { color: #98c379; font-weight: 600; }
.cmd { color: #ffffff; font-weight: 600; }
.prompt { color: #56b6c2; }
.input { color: #e5c07b; font-weight: 600; }
.error { color: #ef6b73; }
"""

MEASURE = """
<script>
window.addEventListener('load', () => {
  document.fonts.ready.then(() => {
    document.body.setAttribute('data-height',
      Math.ceil(document.querySelector('.window').getBoundingClientRect().height));
  });
});
</script>
"""


def render_line(line: str) -> str:
    if line.startswith("# ") or line == "#":
        return f'<span class="comment">{html.escape(line)}</span>'
    if m := SHELL.match(line):
        return (
            f'<span class="time">[{m[1]}]</span> <span class="dollar">$</span> '
            f'<span class="cmd">{html.escape(m[2])}</span>'
        )
    if m := MONGOSH.match(line):
        return f'<span class="prompt">{html.escape(m[1])}</span><span class="input">{html.escape(m[2])}</span>'
    if ERROR.match(line):
        return f'<span class="error">{html.escape(line)}</span>'
    return html.escape(line)


def page(text: str, title: str, measure: bool) -> str:
    body = "\n".join(render_line(line) for line in text.rstrip("\n").split("\n"))
    dots = "".join(
        f'<span class="dot" style="background:{c}"></span>' for c in ("#ff5f57", "#febc2e", "#28c840")
    )
    return (
        '<!DOCTYPE html><html><head><meta charset="utf-8">'
        f"<style>{CSS}</style>{MEASURE if measure else ''}</head><body>"
        f'<div class="window"><div class="bar">{dots}<span class="title">{html.escape(title)}</span></div>'
        f"<pre>{body}</pre></div></body></html>"
    )


def chromium() -> str:
    for name in ("chromium", "chromium-browser", "google-chrome"):
        if path := shutil.which(name):
            return path
    sys.exit("terminal-shot: chromium is not on PATH (run under direnv)")


def run_chromium(args: list[str], profile: Path) -> str:
    cmd = [
        chromium(), "--headless=new", "--no-sandbox", "--disable-gpu", "--hide-scrollbars",
        "--no-first-run", "--disable-extensions", f"--user-data-dir={profile}", *args,
    ]
    done = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
    if done.returncode != 0:
        sys.exit(f"terminal-shot: chromium failed ({done.returncode}):\n{done.stderr[-2000:]}")
    return done.stdout


def select_blocks(text: str, spec: str) -> str:
    """The blank-line-separated blocks FIRST..LAST (1-based) of `text`."""
    first, _, last = spec.partition("-")
    lo, hi = int(first), int(last or first)
    blocks = [b for b in re.split(r"\n\s*\n", text.strip("\n")) if b.strip()]
    if not 1 <= lo <= hi <= len(blocks):
        sys.exit(f"terminal-shot: --blocks {spec} is outside 1-{len(blocks)}")
    return "\n\n".join(blocks[lo - 1:hi]) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("transcript", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--width", type=int, default=1400, help="CSS pixels (default 1400)")
    parser.add_argument("--title", help="window title (default: the transcript's file name)")
    parser.add_argument("--blocks", help="render only blocks FIRST-LAST (blank-line separated, 1-based)")
    args = parser.parse_args()

    text = args.transcript.read_text(encoding="utf-8")
    if args.blocks:
        text = select_blocks(text, args.blocks)
    title = args.title or args.transcript.name
    with tempfile.TemporaryDirectory(prefix="terminal-shot-") as tmp:
        tmpdir = Path(tmp)
        measure = tmpdir / "measure.html"
        measure.write_text(page(text, title, measure=True), encoding="utf-8")
        dom = run_chromium(
            [f"--window-size={args.width},800", "--virtual-time-budget=5000", "--dump-dom", measure.as_uri()],
            tmpdir / "profile",
        )
        found = re.search(r'data-height="(\d+)"', dom)
        if not found:
            sys.exit("terminal-shot: could not measure the page height")
        height = int(found[1])

        shot = tmpdir / "shot.html"
        shot.write_text(page(text, title, measure=False), encoding="utf-8")
        args.output.parent.mkdir(parents=True, exist_ok=True)
        run_chromium(
            [
                f"--window-size={args.width},{height}",
                f"--force-device-scale-factor={SCALE}",
                f"--screenshot={args.output.resolve()}",
                shot.as_uri(),
            ],
            tmpdir / "profile",
        )
    print(f"wrote {args.output} ({args.width}x{height} CSS px, scale {SCALE})", file=sys.stderr)


if __name__ == "__main__":
    main()
