"""Build minified static assets for faster mobile cold start.

Usage:
  python scripts/minify_web.py

Writes app/web/app.min.js and app/web/style.min.css (safe whitespace/comment strip).
index.html prefers *.min.* when present (see cache-bust query still on path).
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
WEB = ROOT / "app" / "web"


def minify_css(src: str) -> str:
    src = re.sub(r"/\*.*?\*/", "", src, flags=re.S)
    src = re.sub(r"\s+", " ", src)
    src = re.sub(r"\s*([{};:,>~+])\s*", r"\1", src)
    src = re.sub(r";}", "}", src)
    return src.strip()


def minify_js(src: str) -> str:
    # Strip block comments; keep strings intact via coarse pass
    out: list[str] = []
    i = 0
    n = len(src)
    while i < n:
        if src.startswith("/*", i):
            j = src.find("*/", i + 2)
            i = n if j < 0 else j + 2
            continue
        if src.startswith("//", i):
            # avoid http://
            if i == 0 or src[i - 1] not in (":",):
                j = src.find("\n", i)
                i = n if j < 0 else j
                continue
        ch = src[i]
        if ch in ('"', "'", "`"):
            quote = ch
            out.append(ch)
            i += 1
            while i < n:
                c = src[i]
                out.append(c)
                if c == "\\" and i + 1 < n:
                    out.append(src[i + 1])
                    i += 2
                    continue
                i += 1
                if c == quote:
                    break
            continue
        out.append(ch)
        i += 1
    text = "".join(out)
    lines = []
    for line in text.splitlines():
        s = line.strip()
        if s:
            lines.append(s)
    return "\n".join(lines) + "\n"


def main() -> int:
    css_in = WEB / "style.css"
    js_in = WEB / "app.js"
    if not css_in.exists() or not js_in.exists():
        print("missing style.css or app.js", file=sys.stderr)
        return 1
    css_out = WEB / "style.min.css"
    js_out = WEB / "app.min.js"
    css_out.write_text(minify_css(css_in.read_text(encoding="utf-8")), encoding="utf-8")
    js_out.write_text(minify_js(js_in.read_text(encoding="utf-8")), encoding="utf-8")
    print(
        f"wrote {css_out.name} ({css_out.stat().st_size} bytes), "
        f"{js_out.name} ({js_out.stat().st_size} bytes)"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
