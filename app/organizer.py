"""Rules for mapping group captions into nested / merged download folders."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Iterable, Optional

from telethon.tl.custom.message import Message
from telethon.tl.types import (
    DocumentAttributeAudio,
    DocumentAttributeFilename,
    DocumentAttributeVideo,
    MessageMediaDocument,
    MessageMediaPhoto,
)

INVALID_PATH_CHARS = re.compile(r'[<>:"/\\|?*\x00-\x1f]')
WHITESPACE = re.compile(r"\s+")

# #风流狗尾巴 或 #1#2（逐个匹配，# 始终在前）
# 在【】[]（）等括号处截断，避免把「#泡泡咕】11-26标题」整段当成标签
HASHTAG_RE = re.compile(r"#([^\s#@/\\<>:|?*【】［］〖〗〔〕〈〉《》\[\]（）()『』「」]+)")
# 7.18 / 07.18
DATE_DOT_RE = re.compile(r"(?<!\d)(\d{1,2}\.\d{1,2})(?!\d)")
# 7.24-8.25 / 7.24~8.25 / 7.24至8.25
DATE_DOT_RANGE_RE = re.compile(
    r"(?<!\d)(\d{1,2}\.\d{1,2})\s*[-~～—–至到]\s*(\d{1,2}\.\d{1,2})(?!\d)"
)
# 7.11-14 → same month as 7.11-7.14 (end day only; not 7.11-8.25)
DATE_DOT_SAME_MONTH_RANGE_RE = re.compile(
    r"(?<!\d)(\d{1,2})\.(\d{1,2})\s*[-~～—–至到]\s*(\d{1,2})(?!\d|\.\d)"
)
# 7月18日
DATE_CN_RE = re.compile(r"(?<!\d)(\d{1,2})月(\d{1,2})[日号]?")
# 7月24日-8月25日
DATE_CN_RANGE_RE = re.compile(
    r"(?<!\d)(\d{1,2})月(\d{1,2})[日号]?\s*[-~～—–至到]\s*(\d{1,2})月(\d{1,2})[日号]?"
)
# 7月11日-14日 / 7月11-14
DATE_CN_SAME_MONTH_RANGE_RE = re.compile(
    r"(?<!\d)(\d{1,2})月(\d{1,2})[日号]?\s*[-~～—–至到]\s*(\d{1,2})[日号]?(?![\d月])"
)

# 标签名清理：去掉误吸入的括号索引尾巴（含全角］等）
_TAG_CUT_RE = re.compile(r"[】］〗〕〉》\]）)』」].*$")
_TAG_OPEN_CUT_RE = re.compile(r"[【［〖〔〈《\[（(『「].*$")


def normalize_tag_name(raw: Optional[str]) -> str:
    """Strip # / brackets / trailing index junk → clean tag name."""
    tag = str(raw or "").strip().lstrip("#").strip()
    if not tag:
        return ""
    tag = _TAG_CUT_RE.sub("", tag)
    tag = _TAG_OPEN_CUT_RE.sub("", tag)
    # 再截一次「名字后紧跟日期索引」：泡泡咕11-26标题 / 泡泡咕12.01标题
    tag = re.split(r"(?<=[\u4e00-\u9fff])\d{1,2}[-./月]\d{1,2}", tag, maxsplit=1)[0]
    tag = tag.strip(" \t.-_·#，,")
    return tag


def sanitize_name(name: str, max_len: int = 120) -> str:
    name = name.strip().replace("\n", " ").replace("\r", " ")
    name = WHITESPACE.sub(" ", name)
    name = INVALID_PATH_CHARS.sub("_", name)
    name = name.strip(" .")
    if not name:
        return "_未命名"
    if len(name) > max_len:
        name = name[:max_len].rstrip(" .")
    return name


def message_text(message: Message) -> str:
    return (message.message or message.text or "").strip()


def has_media(message: Message) -> bool:
    if not message.media:
        return False
    return isinstance(message.media, (MessageMediaPhoto, MessageMediaDocument)) or bool(
        message.photo
        or message.document
        or message.video
        or message.audio
        or message.voice
        or message.video_note
        or message.gif
        or message.sticker
    )


def detect_media_type(message: Message) -> Optional[str]:
    if message.photo:
        return "photo"
    if message.voice:
        return "voice"
    if message.video_note:
        return "video_note"
    if message.video or message.gif:
        return "video"
    if message.audio:
        return "audio"
    if message.sticker:
        return "sticker"
    if message.document:
        doc = message.document
        for attr in doc.attributes or []:
            if isinstance(attr, DocumentAttributeAudio):
                if getattr(attr, "voice", False):
                    return "voice"
                return "audio"
            if isinstance(attr, DocumentAttributeVideo):
                if getattr(attr, "round_message", False):
                    return "video_note"
                return "video"
        return "document"
    return None


def _extension_for_message(message: Message, media_type: Optional[str]) -> str:
    if message.file and message.file.ext:
        return message.file.ext
    defaults = {
        "photo": ".jpg",
        "video": ".mp4",
        "video_note": ".mp4",
        "voice": ".ogg",
        "audio": ".mp3",
        "document": "",
        "sticker": ".webp",
    }
    return defaults.get(media_type or "", "")


def _original_filename(message: Message) -> Optional[str]:
    if message.file and message.file.name:
        return message.file.name
    if message.document:
        for attr in message.document.attributes or []:
            if isinstance(attr, DocumentAttributeFilename):
                return attr.file_name
    return None


def message_file_extension(message: Message, media_type: Optional[str] = None) -> str:
    """Return extension without leading dot, lowercased."""
    ext = _extension_for_message(message, media_type or detect_media_type(message))
    name = _original_filename(message) or ""
    if not ext and "." in name:
        ext = "." + name.rsplit(".", 1)[-1]
    return (ext or "").lstrip(".").lower()


def _normalize_format_set(raw: Optional[Iterable[str] | str]) -> set[str]:
    if raw is None:
        return set()
    if isinstance(raw, str):
        parts = re.split(r"[,，\s]+", raw.strip())
    else:
        parts = list(raw)
    return {str(x).strip().lstrip(".").lower() for x in parts if str(x).strip()}


def matches_file_formats(
    message: Message,
    formats: Optional[list[str] | dict[str, list[str]] | str],
    media_type: Optional[str] = None,
) -> bool:
    """
    Empty formats → accept all.
    list/str → global extension allow-list.
    dict → per media_type allow-list (missing/empty type key → accept that type).
    """
    if not formats:
        return True
    mt = media_type or detect_media_type(message) or ""
    if isinstance(formats, dict):
        # Missing type key → allow that media type
        if mt not in formats:
            return True
        allowed = _normalize_format_set(formats.get(mt))
        if not allowed:
            return True
    else:
        allowed = _normalize_format_set(formats)
        if not allowed:
            return True

    ext = message_file_extension(message, mt or None)
    if not ext:
        if mt == "photo":
            ext = "jpg"
    return ext in allowed


def matches_file_size(
    expected_size: int,
    *,
    min_bytes: int = 0,
    max_bytes: int = 0,
) -> bool:
    """min/max 0 = no bound. Unknown size (0) always passes."""
    size = int(expected_size or 0)
    if size <= 0:
        return True
    lo = max(0, int(min_bytes or 0))
    hi = max(0, int(max_bytes or 0))
    if lo and size < lo:
        return False
    if hi and size > hi:
        return False
    return True


def _valid_month_day(month: int, day: int) -> bool:
    return 1 <= int(month) <= 12 and 1 <= int(day) <= 31


def extract_date_token(text: str) -> Optional[str]:
    if not text:
        return None
    # Prefer ranges so「7.24-8.25」keeps both ends (not just 7.24)
    m = DATE_DOT_RANGE_RE.search(text)
    if m:
        return f"{m.group(1)}-{m.group(2)}"
    # Same-month shorthand「7.11-14」→「7.11-7.14」
    m = DATE_DOT_SAME_MONTH_RANGE_RE.search(text)
    if m:
        month, d1, d2 = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if _valid_month_day(month, d1) and 1 <= d2 <= 31:
            return f"{month}.{d1}-{month}.{d2}"
    m = DATE_CN_RANGE_RE.search(text)
    if m:
        return (
            f"{int(m.group(1))}.{int(m.group(2))}"
            f"-{int(m.group(3))}.{int(m.group(4))}"
        )
    m = DATE_CN_SAME_MONTH_RANGE_RE.search(text)
    if m:
        month, d1, d2 = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if _valid_month_day(month, d1) and 1 <= d2 <= 31:
            return f"{month}.{d1}-{month}.{d2}"
    m = DATE_DOT_RE.search(text)
    if m:
        return m.group(1)
    m = DATE_CN_RE.search(text)
    if m:
        return f"{int(m.group(1))}.{int(m.group(2))}"
    return None


# VID_20250711_xxx / 2025-07-11 / 2025.07.11 / 20250711
_FILENAME_YMD_RE = re.compile(
    r"(?<!\d)(20\d{2})[._\- ]?(0[1-9]|1[0-2])[._\- ]?(0[1-9]|[12]\d|3[01])(?!\d)"
)
_FILENAME_YEAR_CN_RE = re.compile(r"(?<!\d)(20\d{2})\s*年")
_FILENAME_YEAR_RE = re.compile(r"(?<!\d)(20\d{2})(?!\d)")
# Year already on a folder/token part: 2026.7.11
_YEAR_MD_PART_RE = re.compile(r"^(20\d{2})\.(\d{1,2})\.(\d{1,2})$")
_MD_PART_RE = re.compile(r"^(\d{1,2})\.(\d{1,2})$")


def extract_ymd_from_filename(name: Optional[str]) -> Optional[tuple[int, int, int]]:
    """Full calendar date (Y, M, D) from media filename; None if incomplete."""
    if not name:
        return None
    m = _FILENAME_YMD_RE.search(str(name).strip())
    if not m:
        return None
    y, month, day = int(m.group(1)), int(m.group(2)), int(m.group(3))
    if 2000 <= y <= 2100 and _valid_month_day(month, day):
        return y, month, day
    return None


def extract_years_from_filename(name: Optional[str]) -> list[int]:
    """Years found in a filename (YMD first, then 年 / bare 20xx)."""
    if not name:
        return []
    stem = str(name).strip()
    out: list[int] = []
    seen: set[int] = set()

    def _add(y: int) -> None:
        if 2000 <= y <= 2100 and y not in seen:
            seen.add(y)
            out.append(y)

    ymd = extract_ymd_from_filename(stem)
    if ymd:
        _add(ymd[0])
    for m in _FILENAME_YEAR_CN_RE.finditer(stem):
        _add(int(m.group(1)))
    for m in _FILENAME_YEAR_RE.finditer(stem):
        y = int(m.group(1))
        if 2010 <= y <= 2039:
            _add(y)
    return out


def years_from_filenames(names: Iterable[Optional[str]]) -> list[int]:
    from collections import Counter

    c: Counter[int] = Counter()
    for n in names or []:
        for y in extract_years_from_filename(n):
            c[y] += 1
    return [y for y, _ in c.most_common()]


def years_from_dir(dir_path: Path, *, sample: int = 80) -> list[int]:
    names: list[str] = []
    try:
        for p in dir_path.iterdir():
            if p.is_file():
                names.append(p.name)
            if len(names) >= sample:
                break
    except OSError:
        return []
    return years_from_filenames(names)


def extract_years_from_text(text: Optional[str]) -> list[int]:
    """Years mentioned in caption / nearby 文案 (2026年 / 2026.7.11 / bare 20xx)."""
    if not text:
        return []
    s = str(text)
    out: list[int] = []
    seen: set[int] = set()

    def _add(y: int) -> None:
        if 2000 <= y <= 2100 and y not in seen:
            seen.add(y)
            out.append(y)

    for m in re.finditer(r"(?<!\d)(20\d{2})\s*年", s):
        _add(int(m.group(1)))
    for m in re.finditer(
        r"(?<!\d)(20\d{2})\.(\d{1,2})\.(\d{1,2})(?!\d)", s
    ):
        _add(int(m.group(1)))
    for m in re.finditer(r"(?<!\d)(20\d{2})(?!\d)", s):
        y = int(m.group(1))
        if 2010 <= y <= 2039:
            _add(y)
    return out


def parse_md_part(part: str) -> Optional[tuple[Optional[int], int, int]]:
    """Parse「2026.7.11」or「7.11」→ (year|None, month, day)."""
    part = (part or "").strip()
    m = _YEAR_MD_PART_RE.match(part)
    if m:
        y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if _valid_month_day(mo, d):
            return y, mo, d
        return None
    m = _MD_PART_RE.match(part)
    if m:
        mo, d = int(m.group(1)), int(m.group(2))
        if _valid_month_day(mo, d):
            return None, mo, d
    return None


def strip_years_from_date_token(token: Optional[str]) -> Optional[str]:
    """2026.7.11-2026.7.14 → 7.11-7.14；2026.7.11 → 7.11."""
    if not token:
        return token
    if "-" in token:
        left, right = token.split("-", 1)
        pl, pr = parse_md_part(left), parse_md_part(right)
        if not pl or not pr:
            return token
        return f"{pl[1]}.{pl[2]}-{pr[1]}.{pr[2]}"
    p = parse_md_part(token)
    if not p:
        return token
    return f"{p[1]}.{p[2]}"


def apply_years_to_caption_date(
    token: Optional[str],
    years: Optional[Iterable[int]] = None,
    *,
    fallback_year: Optional[int] = None,
) -> Optional[str]:
    """
    Month/day from caption token; year(s) from files / hint.

    Cross-year caption「12.20-1.5」+ year 2025 →「2025.12.20-2026.1.5」.
    Multi-year files on a cross-year span use min→max.
    """
    if not token:
        return None
    bare = strip_years_from_date_token(token) or token
    ys = sorted(
        {
            int(y)
            for y in (years or [])
            if y is not None and 2000 <= int(y) <= 2100
        }
    )
    if not ys and fallback_year is not None and 2000 <= int(fallback_year) <= 2100:
        ys = [int(fallback_year)]
    if not ys:
        return bare

    if "-" not in bare:
        p = parse_md_part(bare)
        if not p:
            return bare
        _y0, mo, d = p
        return f"{ys[0]}.{mo}.{d}"

    left, right = bare.split("-", 1)
    pl, pr = parse_md_part(left), parse_md_part(right)
    if not pl or not pr:
        return bare
    _a, lm, ld = pl
    _b, rm, rd = pr
    cross = (rm, rd) < (lm, ld)
    if cross:
        if len(ys) >= 2:
            y1, y2 = ys[0], ys[-1]
            if y2 <= y1:
                y2 = y1 + 1
        else:
            y1 = ys[0]
            y2 = y1 + 1
    else:
        y1 = y2 = ys[0]
    return f"{y1}.{lm}.{ld}-{y2}.{rm}.{rd}"


def resolve_caption_date_token(
    caption: str,
    filenames: Optional[Iterable[Optional[str]]] = None,
    *,
    hint_year: Optional[int] = None,
    nearby_texts: Optional[Iterable[str]] = None,
) -> Optional[str]:
    """
    月日 ← 文案；年 ← 文件名；无年则附近文案 / hint 推算。
    """
    md = extract_date_token(caption or "")
    if not md:
        return None
    years = years_from_filenames(filenames or [])
    # Caption itself may carry a year
    years = list(dict.fromkeys(years + extract_years_from_text(caption)))
    fallback = hint_year
    if not years and not fallback:
        for t in nearby_texts or []:
            near = extract_years_from_text(t)
            if near:
                fallback = near[0]
                break
    return apply_years_to_caption_date(md, years, fallback_year=fallback)


def date_token_from_dir(
    dir_path: Path,
    *,
    caption_hint: Optional[str] = None,
    sample: int = 80,
) -> Optional[str]:
    """Rebuild folder date: MD from dir name/caption mapping hint + years from files."""
    md = strip_years_from_date_token(dir_path.name)
    if caption_hint:
        cap_md = extract_date_token(caption_hint)
        if cap_md:
            md = strip_years_from_date_token(cap_md) or cap_md
    years = years_from_dir(dir_path, sample=sample)
    return apply_years_to_caption_date(md, years)


def build_caption_batch_filenames(
    messages: Iterable[Any],
    captions_by_id: Optional[dict[int, str]] = None,
) -> tuple[dict[Any, list[str]], dict[str, list[str]]]:
    """
    Group original filenames by album grouped_id and by caption text.
    Used so one 文案 batch shares earliest~latest file dates.
    """
    album: dict[Any, list[str]] = {}
    by_caption: dict[str, list[str]] = {}
    caps = captions_by_id or {}
    for message in messages or []:
        if not message:
            continue
        name = _original_filename(message)
        if not name:
            continue
        gid = getattr(message, "grouped_id", None)
        if gid:
            album.setdefault(gid, []).append(name)
        mid = int(getattr(message, "id", 0) or 0)
        cap = (caps.get(mid) if mid else None) or message_text(message) or ""
        key = str(cap).strip()
        if key:
            by_caption.setdefault(key, []).append(name)
    return album, by_caption


def batch_filenames_for_message(
    message: Any,
    *,
    caption: str = "",
    album_batches: Optional[dict[Any, list[str]]] = None,
    caption_batches: Optional[dict[str, list[str]]] = None,
) -> list[str]:
    """Filenames for the same 文案/album batch; falls back to this file only."""
    gid = getattr(message, "grouped_id", None) if message else None
    if gid and album_batches and album_batches.get(gid):
        return list(album_batches[gid])
    key = str(caption or "").strip()
    if key and caption_batches and caption_batches.get(key):
        return list(caption_batches[key])
    name = _original_filename(message) if message else None
    return [name] if name else []


def looks_like_date_folder(name: str) -> bool:
    """7.11 / 7.11-7.14 / 2025.7.11 / 2025.7.11-2025.7.14 / 7.11-14"""
    return bool(
        re.match(
            r"^(?:20\d{2}\.)?\d{1,2}\.\d{1,2}"
            r"(?:-(?:(?:20\d{2}\.)?\d{1,2}(?:\.\d{1,2})?|\d{1,2}))?$",
            str(name or ""),
        )
    )


def extract_tags(text: str) -> list[str]:
    """
    Extract tags from caption. # always prefix: #1  #1#2  #风流狗尾巴
    Returns normalized tag names without #.
    """
    if not text:
        return []
    found: list[str] = []
    seen: set[str] = set()

    for m in HASHTAG_RE.finditer(text):
        tag = normalize_tag_name(m.group(1))
        if not tag:
            continue
        key = tag.lower()
        if key in seen:
            continue
        seen.add(key)
        found.append(tag)

    return found


def normalize_tag_list(raw: Optional[Iterable[str] | str]) -> list[str]:
    """Accept '#a,#b' / ['#a','b'] → ['a','b'] (order preserved, de-duped)."""
    if raw is None:
        return []
    if isinstance(raw, str):
        parts = re.split(r"[,，\s]+", raw.strip())
    else:
        parts = []
        for item in raw:
            s = str(item or "").strip()
            if not s:
                continue
            if "," in s or "，" in s or " " in s:
                parts.extend(re.split(r"[,，\s]+", s))
            else:
                parts.append(s)
    out: list[str] = []
    seen: set[str] = set()
    for p in parts:
        tag = normalize_tag_name(p)
        if not tag:
            continue
        key = tag.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(tag)
    return out


def normalize_keyword_list(raw: Optional[Iterable[str] | str]) -> list[str]:
    """Comma/空白分隔的文案关键词列表。"""
    if raw is None:
        return []
    if isinstance(raw, str):
        parts = re.split(r"[,，]+", raw.strip())
    else:
        parts = [str(x) for x in raw]
    out: list[str] = []
    seen: set[str] = set()
    for p in parts:
        kw = p.strip()
        if not kw:
            continue
        key = kw.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(kw)
    return out


def matches_include_tags(
    message_tags: Optional[Iterable[str]],
    include_tags: Optional[Iterable[str]],
    *,
    mode: str = "any",
) -> bool:
    """Empty include_tags → accept all. mode: any | all."""
    wanted = normalize_tag_list(include_tags)
    if not wanted:
        return True
    have = {
        normalize_tag_name(t).lower()
        for t in (message_tags or [])
        if normalize_tag_name(t)
    }
    need = {t.lower() for t in wanted}
    if mode == "all":
        return need.issubset(have)
    return bool(have & need)


def matches_caption_keywords(
    caption: Optional[str],
    keywords: Optional[Iterable[str]],
) -> bool:
    """Empty keywords → accept all. Match if caption contains any keyword (case-insensitive)."""
    kws = normalize_keyword_list(keywords)
    if not kws:
        return True
    text = (caption or "").lower()
    if not text:
        return False
    return any(k.lower() in text for k in kws)


def extract_hashtag(text: str) -> Optional[str]:
    tags = extract_tags(text)
    return tags[0] if tags else None


def _tag_sort_key(tag: str):
    if tag.isdigit():
        return (0, int(tag), "")
    return (1, 0, tag.lower())


def format_merged_folder_name(tags: Iterable[str]) -> str:
    """
    ['1','2','3'] → '#1 #2 #3'
    ['风流狗尾巴'] → '#风流狗尾巴'
    """
    uniq = sorted(
        {t.strip().lstrip("#") for t in tags if t and str(t).strip()},
        key=_tag_sort_key,
    )
    if not uniq:
        return "_未分类"
    name = " ".join(f"#{t}" for t in uniq)
    return sanitize_name(name, max_len=180)


def relation_blacklist_set(blacklist: Optional[Iterable[str]] = None) -> set[str]:
    return {
        str(b).strip().lstrip("#").lower()
        for b in (blacklist or [])
        if str(b).strip()
    }


def strip_relation_blacklist(
    tags: Optional[Iterable[str]],
    blacklist: Optional[Iterable[str]] = None,
) -> list[str]:
    """Normalize tags and drop relation-hub blacklist entries (order preserved)."""
    bl = relation_blacklist_set(blacklist)
    out: list[str] = []
    seen: set[str] = set()
    for t in normalize_tag_list(tags or []):
        key = t.lower()
        if key in bl or key in seen:
            continue
        seen.add(key)
        out.append(t)
    return out


# ---------------------------------------------------------------------------
# Tag relation / folder merge (single source of truth)
#
# Pipeline:
#   1) Index: caption tags stored per media (chat_media_index)
#   2) Graph: Union-Find on co-occurrence groups (same caption share = edge)
#      - Blacklist hubs never bridge components and never enter folder names
#      - Disk folders named #a #b also contribute edges (after blacklist strip)
#   3) Name: tag → '#1 #2 #3' via build_tag_folder_map_from_groups /
#      resolve_related_folder_name / canonical_tag_folder
#   4) Expand (download filter): same UF components (expand_tags_via_cooccurrence)
#   5) Physical merge: merge_related_tag_folders moves dirs into canonical names
#
# UI picker may add extra hubDegree heuristics; download/folder always use this UF.
# ---------------------------------------------------------------------------


class TagUnionFind:
    """
    Case-insensitive union-find for co-occurring tags.
    Keys are lowercase; display casing is first-seen.
    """

    def __init__(self) -> None:
        self.parent: dict[str, str] = {}
        self.display: dict[str, str] = {}

    def _key(self, tag: str) -> str:
        return str(tag or "").strip().lstrip("#").lower()

    def add(self, tag: str) -> str:
        key = self._key(tag)
        if not key:
            return ""
        if key not in self.parent:
            self.parent[key] = key
            self.display[key] = str(tag).strip().lstrip("#")
        return key

    def find(self, tag: str) -> str:
        key = self.add(tag)
        if not key:
            return ""
        while self.parent[key] != key:
            self.parent[key] = self.parent[self.parent[key]]
            key = self.parent[key]
        return key

    def union(self, a: str, b: str) -> None:
        ra, rb = self.find(a), self.find(b)
        if not ra or not rb or ra == rb:
            return
        self.parent[rb] = ra

    def union_group(self, tags: list[str]) -> None:
        cleaned = [t for t in tags if self._key(t)]
        if not cleaned:
            return
        self.add(cleaned[0])
        for t in cleaned[1:]:
            self.union(cleaned[0], t)

    def build_tag_to_folder(self) -> dict[str, str]:
        groups: dict[str, set[str]] = {}
        for key in self.parent:
            root = self.find(key)
            groups.setdefault(root, set()).add(self.display.get(key, key))
        folder_by_root = {
            root: format_merged_folder_name(members) for root, members in groups.items()
        }
        out: dict[str, str] = {}
        for key in self.parent:
            folder = folder_by_root[self.find(key)]
            out[self.display.get(key, key)] = folder
            out[key] = folder
        return out

    def components(self) -> dict[str, list[str]]:
        """root_key → sorted display tags."""
        groups: dict[str, set[str]] = {}
        for key in self.parent:
            root = self.find(key)
            groups.setdefault(root, set()).add(self.display.get(key, key))
        return {
            root: sorted(members, key=_tag_sort_key)
            for root, members in groups.items()
        }

    def related_map(self) -> dict[str, list[str]]:
        """display tag → other display tags in the same component."""
        out: dict[str, list[str]] = {}
        for members in self.components().values():
            for tag in members:
                out[tag] = [t for t in members if t.lower() != tag.lower()]
        return out

    def expand_seeds(self, seeds: Iterable[str]) -> list[str]:
        """Seeds first, then other members of the same components."""
        seed_list = normalize_tag_list(seeds)
        if not seed_list:
            return []
        seen = {t.lower(): t for t in seed_list}
        out = list(seed_list)
        for seed in seed_list:
            key = self._key(seed)
            if key not in self.parent:
                continue
            root = self.find(key)
            for member_key in list(self.parent.keys()):
                if self.find(member_key) != root:
                    continue
                name = self.display.get(member_key, member_key)
                lk = name.lower()
                if lk in seen:
                    continue
                seen[lk] = name
                out.append(name)
        return out


def build_tag_uf_from_tag_groups(
    groups: Iterable[Iterable[str]],
    *,
    blacklist: Optional[Iterable[str]] = None,
) -> TagUnionFind:
    """Union tags that co-occur in the same group (caption), transitively."""
    uf = TagUnionFind()
    for group in groups:
        tags = strip_relation_blacklist(group, blacklist)
        if tags:
            uf.union_group(tags)
    return uf


def build_tag_folder_map_from_texts(
    texts: Iterable[str],
    *,
    blacklist: Optional[Iterable[str]] = None,
) -> dict[str, str]:
    uf = TagUnionFind()
    for text in texts:
        tags = strip_relation_blacklist(extract_tags(text or ""), blacklist)
        if tags:
            uf.union_group(tags)
    return uf.build_tag_to_folder()


def build_tag_uf_from_group_dir(
    group_dir: Optional[Path],
    extra_groups: Optional[Iterable[list[str]]] = None,
    *,
    blacklist: Optional[Iterable[str]] = None,
) -> TagUnionFind:
    """Union tags from existing #… folders on disk + optional caption groups."""
    uf = TagUnionFind()
    if group_dir and group_dir.is_dir():
        for p in group_dir.iterdir():
            if not p.is_dir():
                continue
            tags = strip_relation_blacklist(extract_tags(p.name), blacklist)
            if tags:
                uf.union_group(tags)
    if extra_groups:
        for group in extra_groups:
            tags = strip_relation_blacklist(group, blacklist)
            if tags:
                uf.union_group(tags)
    return uf


def build_tag_folder_map_from_groups(
    groups: Iterable[Iterable[str]],
    *,
    group_dir: Optional[Path] = None,
    blacklist: Optional[Iterable[str]] = None,
) -> dict[str, str]:
    """
    tag → full related-folder name.
    Edges: caption co-occurrence + disk folder names; blacklist hubs never bridge.
    """
    uf = (
        build_tag_uf_from_group_dir(group_dir, None, blacklist=blacklist)
        if group_dir
        else TagUnionFind()
    )
    for group in groups:
        tags = strip_relation_blacklist(group, blacklist)
        if tags:
            uf.union_group(tags)
    return uf.build_tag_to_folder()


def expand_tags_via_cooccurrence(
    seeds: Iterable[str],
    groups: Iterable[Iterable[str]],
    *,
    blacklist: Optional[Iterable[str]] = None,
) -> list[str]:
    """
    Expand seeds with all tags in the same UF components (same rules as folders).
    Seeds keep original order first; related tags append afterward.
    """
    seed_list = normalize_tag_list(seeds)
    if not seed_list:
        return []
    uf = build_tag_uf_from_tag_groups(groups, blacklist=blacklist)
    return uf.expand_seeds(seed_list)


def _folder_map_lookup(tag_folder_map: dict[str, str], tag: str) -> Optional[str]:
    if not tag_folder_map or not tag:
        return None
    if tag in tag_folder_map:
        return tag_folder_map[tag]
    key = str(tag).strip().lstrip("#").lower()
    if key in tag_folder_map:
        return tag_folder_map[key]
    for k, v in tag_folder_map.items():
        if str(k).lower() == key:
            return v
    return None


def resolve_related_folder_name(
    tags: Iterable[str],
    *,
    tag_folder_map: Optional[dict[str, str]] = None,
    blacklist: Optional[Iterable[str]] = None,
) -> str:
    """
    Folder name = full related component when known, else this caption's tags.
    Hub/blacklist tags are omitted from the folder name.
    """
    ordered = strip_relation_blacklist(tags, blacklist)
    if not ordered:
        return "_未分类"

    best: Optional[str] = None
    best_n = -1
    if tag_folder_map:
        for t in ordered:
            mapped = _folder_map_lookup(tag_folder_map, t)
            if not mapped:
                continue
            n = len(extract_tags(mapped))
            if n > best_n:
                best_n = n
                best = mapped
    if best:
        members = strip_relation_blacklist(extract_tags(best), blacklist)
        have = {m.lower() for m in members}
        extra = [t for t in ordered if t.lower() not in have]
        if extra:
            return format_merged_folder_name(list(members) + extra)
        if not members:
            return "_未分类"
        return format_merged_folder_name(members)
    return format_merged_folder_name(ordered)


def _move_path_unique(src: Path, dst_dir: Path) -> Path:
    """Move file into dst_dir; on name conflict append _1, _2, …"""
    dst_dir.mkdir(parents=True, exist_ok=True)
    target = dst_dir / src.name
    if not target.exists():
        src.rename(target)
        return target
    stem, suffix = src.stem, src.suffix
    i = 1
    while True:
        alt = dst_dir / f"{stem}_{i}{suffix}"
        if not alt.exists():
            src.rename(alt)
            return alt
        i += 1


def _merge_tree_into(src: Path, dst: Path) -> None:
    """Move all contents of src into dst (recursive), then remove empty src."""
    if not src.is_dir():
        return
    dst.mkdir(parents=True, exist_ok=True)
    for item in list(src.iterdir()):
        if item.is_dir():
            _merge_tree_into(item, dst / item.name)
        else:
            _move_path_unique(item, dst)
    try:
        if src.exists() and not any(src.iterdir()):
            src.rmdir()
    except OSError:
        pass


def _rel_under_root(path: Path, root: Path) -> Optional[Path]:
    """Relative path of path under root, or None if outside."""
    try:
        rel = Path(path).resolve().relative_to(Path(root).resolve())
    except (ValueError, OSError):
        try:
            import os

            rel = Path(os.path.relpath(str(path), str(root)))
        except ValueError:
            return None
    if rel.is_absolute() or str(rel).startswith(".."):
        return None
    return rel


def prune_empty_dirs(root: Path, *, stop_at: Optional[Path] = None) -> int:
    """Remove empty directories under root (depth-first). Returns how many removed."""
    if not root.is_dir():
        return 0
    removed = 0
    stop = Path(stop_at).resolve() if stop_at is not None else None
    try:
        entries = list(root.iterdir())
    except OSError:
        return 0
    for item in entries:
        if item.is_dir():
            removed += prune_empty_dirs(item, stop_at=stop_at)
    try:
        if root.exists() and not any(root.iterdir()):
            if stop is not None and root.resolve() == stop:
                return removed
            root.rmdir()
            removed += 1
    except OSError:
        pass
    return removed


def mirror_merge_dirs(
    src: Path,
    dst: Path,
    *,
    download_root: Path,
    temp_root: Path,
) -> bool:
    """
    Apply the same directory merge under temp_dir (mirrors download_dir tree).
    Keeps .part layout in sync so renames do not leave zombie temp folders.
    """
    src_rel = _rel_under_root(src, download_root)
    dst_rel = _rel_under_root(dst, download_root)
    if src_rel is None or dst_rel is None:
        return False
    t_src = Path(temp_root) / src_rel
    t_dst = Path(temp_root) / dst_rel
    if t_src.is_dir():
        _merge_tree_into(t_src, t_dst)
        prune_empty_dirs(t_src, stop_at=temp_root)
        # Also prune empty parents up to temp root
        parent = t_src.parent
        try:
            stop = Path(temp_root).resolve()
            while parent.exists() and parent.resolve() != stop:
                if any(parent.iterdir()):
                    break
                nxt = parent.parent
                try:
                    parent.rmdir()
                except OSError:
                    break
                parent = nxt
        except OSError:
            pass
        return True
    # Source already gone — still drop empty zombie temp dirs at that path
    if t_src.exists():
        prune_empty_dirs(t_src, stop_at=temp_root)
    return False


def sync_download_temp_moves(
    moves: Iterable[tuple[Any, Path, Path]],
    *,
    download_root: Path,
    temp_root: Path,
    prune_under: Optional[Path] = None,
) -> int:
    """
    For each (log, src, dst) download-dir move, mirror into temp_dir.
    Optionally prune empty dirs under prune_under (download) and its temp mirror.
    """
    mirrored = 0
    for item in moves or []:
        if not item or len(item) < 3:
            continue
        _log, src, dst = item[0], item[1], item[2]
        if mirror_merge_dirs(
            Path(src), Path(dst), download_root=download_root, temp_root=temp_root
        ):
            mirrored += 1
    if prune_under is not None and Path(prune_under).is_dir():
        prune_empty_dirs(Path(prune_under), stop_at=prune_under)
        rel = _rel_under_root(Path(prune_under), download_root)
        if rel is not None:
            prune_empty_dirs(Path(temp_root) / rel, stop_at=temp_root)
    return mirrored


def canonical_tag_folder(
    group_dir: Optional[Path],
    tags: list[str],
    *,
    tag_folder_map: Optional[dict[str, str]] = None,
    blacklist: Optional[Iterable[str]] = None,
) -> str:
    """
    Folder name for this message's tags, considering related map + disk folders.

    Example: caption #1#2 with related #3 → '#1 #2 #3'
    Later caption #1 alone → still '#1 #2 #3'
    """
    if not tags:
        return "_未分类"
    mapping = dict(tag_folder_map or {})
    if group_dir:
        disk_map = build_tag_uf_from_group_dir(
            group_dir, [list(tags)], blacklist=blacklist
        ).build_tag_to_folder()
        for tag, folder in disk_map.items():
            prev = _folder_map_lookup(mapping, tag)
            if not prev or len(extract_tags(folder)) >= len(extract_tags(prev)):
                mapping[tag] = folder
    return resolve_related_folder_name(
        tags, tag_folder_map=mapping or None, blacklist=blacklist
    )


def merge_related_tag_folders(
    group_dir: Path,
    *,
    extra_tag_groups: Optional[Iterable[list[str]]] = None,
    blacklist: Optional[Iterable[str]] = None,
) -> list[str]:
    """
    Merge top-level #tag dirs that share tags transitively.
    #1 + #2 + related #3 → single folder #1 #2 #3
    Date subfolders (e.g. 7.18) are preserved under the merged name.
    """
    if not group_dir.is_dir():
        return []

    tag_dirs = [
        p for p in group_dir.iterdir() if p.is_dir() and extract_tags(p.name)
    ]
    if len(tag_dirs) < 2 and not extra_tag_groups:
        return []

    tag_to_folder = build_tag_folder_map_from_groups(
        extra_tag_groups or [],
        group_dir=group_dir,
        blacklist=blacklist,
    )
    if not tag_to_folder:
        return []

    plan: list[tuple[Path, str]] = []
    for d in tag_dirs:
        tags = extract_tags(d.name)
        target = resolve_related_folder_name(
            tags, tag_folder_map=tag_to_folder, blacklist=blacklist
        )
        if target and target != d.name and target != "_未分类":
            plan.append((d, target))

    if not plan:
        return []

    logs: list[str] = []
    for src, target_name in plan:
        if not src.exists():
            continue
        dst = group_dir / target_name
        _merge_tree_into(src, dst)
        logs.append(f"合并目录: {src.name} → {target_name}")
    return logs


def build_date_folder_repairs(captions: Iterable[str]) -> dict[str, str]:
    """
    Map legacy date folder names → canonical range names from captions.

    Old parser only kept the start (7.11 / 7.24). New parser expands
    「7.11-14」→「7.11-7.14」and keeps「7.24-8.25」.
    """
    from collections import Counter

    votes: dict[str, Counter[str]] = {}
    for raw in captions:
        token = extract_date_token(str(raw or ""))
        if not token or "-" not in token:
            continue
        left, right = token.split("-", 1)
        if not left or not right:
            continue
        votes.setdefault(left, Counter())[token] += 1
        # Same-month shorthand on disk: 7.11-14
        if "." in left and "." in right:
            lm, ld = left.split(".", 1)
            rm, rd = right.split(".", 1)
            if lm == rm:
                abbrev = f"{lm}.{ld}-{rd}"
                if abbrev != token:
                    votes.setdefault(abbrev, Counter())[token] += 1
    repair: dict[str, str] = {}
    for legacy, counter in votes.items():
        best, n = counter.most_common(1)[0]
        if best == legacy:
            continue
        # Unique target, or clear majority if several ranges share a start
        rest = sum(counter.values()) - n
        if len(counter) == 1 or n > rest:
            repair[legacy] = best
    return repair


def _strip_year_date_part(part: str) -> str:
    m = re.match(r"^20\d{2}\.(\d{1,2}\.\d{1,2})$", (part or "").strip())
    return m.group(1) if m else (part or "").strip()


def date_folder_legacy_aliases(token: str) -> set[str]:
    """Legacy / shorthand folder names that should merge into token."""
    token = str(token or "").strip()
    if not token:
        return set()
    out = {token}
    parts = token.split("-")
    left = parts[0]
    right = parts[1] if len(parts) > 1 else ""
    bl = _strip_year_date_part(left)
    br = _strip_year_date_part(right) if right else ""
    if bl:
        out.add(bl)
        out.add(left)
    if right:
        out.add(f"{left}-{right}")
        if bl and br:
            out.add(f"{bl}-{br}")
            if "." in bl and "." in br:
                lm, ld = bl.split(".", 1)
                rm, rd = br.split(".", 1)
                if lm == rm:
                    out.add(f"{lm}.{ld}-{rd}")
            out.add(bl)  # old start-only folder
            out.add(left)
    return {x for x in out if x}


def migrate_legacy_date_dirs(
    parent_dir: Path,
    canonical: str,
    *,
    extra_aliases: Optional[Iterable[str]] = None,
    caption: Optional[str] = None,
) -> list[tuple[str, Path, Path]]:
    """
    Move sibling legacy date folders into canonical under parent_dir.

    Pulls: name aliases of canonical, caption date folders (e.g. 7.11), and
    siblings whose file YMD range equals canonical. Old files move with rename.
    """
    canonical = sanitize_name(str(canonical or "").strip(), max_len=40)
    if not canonical or not parent_dir.is_dir():
        return []
    aliases = date_folder_legacy_aliases(canonical)
    for a in extra_aliases or []:
        a = str(a or "").strip()
        if a:
            aliases.add(a)
            aliases |= date_folder_legacy_aliases(a)
    cap_tok = extract_date_token(caption or "")
    if cap_tok:
        aliases.add(cap_tok)
        aliases |= date_folder_legacy_aliases(cap_tok)
        if "-" in cap_tok:
            aliases.add(cap_tok.split("-", 1)[0])
    aliases.discard(canonical)

    dst = parent_dir / canonical
    moves: list[tuple[str, Path, Path]] = []
    try:
        children = [p for p in parent_dir.iterdir() if p.is_dir()]
    except OSError:
        return []
    for child in children:
        if child.name == canonical:
            continue
        if not looks_like_date_folder(child.name) and child.name not in aliases:
            continue
        pull = child.name in aliases
        if not pull and looks_like_date_folder(child.name):
            # Same MD (ignoring year) or rebuilt token equals canonical
            rebuilt = date_token_from_dir(child, caption_hint=caption)
            pull = rebuilt == canonical or (
                strip_years_from_date_token(child.name)
                == strip_years_from_date_token(canonical)
            )
        if not pull:
            continue
        try:
            if child.resolve() == dst.resolve():
                continue
        except OSError:
            pass
        src_snap = Path(child)
        dst.mkdir(parents=True, exist_ok=True)
        _merge_tree_into(child, dst)
        line = f"日期目录: {parent_dir.name}/{src_snap.name} → {dst.name}"
        moves.append((line, src_snap, dst))
    return moves


def repair_date_folders(
    group_dir: Path, captions: Iterable[str]
) -> list[tuple[str, Path, Path]]:
    """
    Rename/merge date subfolders under tag dirs (and group root).

    Prefer full YMD range from files; else caption range. Always merge legacy
    alias folders into the canonical target so old data moves with the rename.
    """
    if not group_dir.is_dir():
        return []
    mapping = build_date_folder_repairs(captions or [])

    parents = [group_dir]
    try:
        parents.extend(p for p in group_dir.iterdir() if p.is_dir())
    except OSError:
        pass

    moves: list[tuple[str, Path, Path]] = []
    for parent in parents:
        try:
            children = [p for p in parent.iterdir() if p.is_dir()]
        except OSError:
            continue
        date_dirs = [
            p
            for p in children
            if looks_like_date_folder(p.name) or p.name in mapping
        ]
        if not date_dirs:
            continue

        # preferred: 月日←文案映射/夹名，年←夹内文件名
        preferred: dict[Path, str] = {}
        sibling_years: list[int] = []
        for child in date_dirs:
            sibling_years.extend(extract_years_from_text(child.name))
            sibling_years.extend(years_from_dir(child, sample=20))
        sibling_year = sibling_years[0] if sibling_years else None
        for child in date_dirs:
            md = mapping.get(child.name) or strip_years_from_date_token(child.name)
            if not md:
                md = child.name
            # If mapping expands start-only → range, prefer that MD
            md = mapping.get(child.name) or mapping.get(
                strip_years_from_date_token(child.name) or ""
            ) or md
            years = years_from_dir(child)
            token = apply_years_to_caption_date(
                md, years, fallback_year=sibling_year
            )
            preferred[child] = token or child.name


        # Group sources by canonical target; pull caption/file aliases too
        from collections import defaultdict

        groups: dict[str, list[Path]] = defaultdict(list)
        for child, target in preferred.items():
            groups[sanitize_name(target, max_len=40)].append(child)

        # Reverse caption map: canonical caption token → legacy names
        reverse_caption: dict[str, set[str]] = defaultdict(set)
        for legacy, canon in mapping.items():
            reverse_caption[canon].add(legacy)

        for target_name, sources in list(groups.items()):
            aliases = date_folder_legacy_aliases(target_name)
            aliases |= reverse_caption.get(target_name, set())
            # Also alias caption expansion that maps into this file target:
            # e.g. sources preferred 2026.4.11-… but disk still has 7.11 / 7.11-7.14
            for child in date_dirs:
                if child in sources:
                    continue
                if child.name in aliases or mapping.get(child.name) in (
                    target_name,
                    preferred.get(child),
                ):
                    child_pref = preferred.get(child) or child.name
                    if (
                        child_pref != target_name
                        and child.name not in aliases
                        and strip_years_from_date_token(child_pref)
                        != strip_years_from_date_token(target_name)
                    ):
                        continue
                    sources.append(child)
                    aliases |= date_folder_legacy_aliases(child_pref)
                    aliases.add(child.name)

            # Include any remaining alias-named dirs under parent
            for child in date_dirs:
                if child not in sources and child.name in aliases:
                    child_pref = preferred.get(child) or child.name
                    if (
                        child_pref != target_name
                        and strip_years_from_date_token(child_pref)
                        != strip_years_from_date_token(target_name)
                        and child.name not in date_folder_legacy_aliases(target_name)
                    ):
                        continue
                    sources.append(child)


            dst = parent / target_name
            # Dedupe sources
            seen_src: set[Path] = set()
            uniq_sources: list[Path] = []
            for s in sources:
                key = s.resolve() if s.exists() else s
                if key in seen_src:
                    continue
                seen_src.add(key)
                uniq_sources.append(s)

            for src in uniq_sources:
                if not src.exists():
                    continue
                try:
                    if src.resolve() == dst.resolve():
                        continue
                except OSError:
                    if src.name == dst.name:
                        continue
                src_snap = Path(src)
                dst.mkdir(parents=True, exist_ok=True)
                _merge_tree_into(src, dst)
                if parent == group_dir:
                    line = f"日期目录: {src_snap.name} → {dst.name}"
                else:
                    line = f"日期目录: {parent.name}/{src_snap.name} → {dst.name}"
                moves.append((line, src_snap, dst))

            # After creating/filling target, sweep leftover aliases once more
            moves.extend(
                migrate_legacy_date_dirs(
                    parent,
                    target_name,
                    extra_aliases=aliases,
                )
            )
    return moves


MENTION_RE = re.compile(r"@\S+")


def resolve_caption_text(
    message: Message,
    *,
    album_captions: Optional[dict] = None,
    caption_override: Optional[str] = None,
) -> str:
    """
    Read ONLY the message caption / album caption.
    Never uses Telegram original filename.
    Example: 「#风流狗尾巴 7.18自录 @csdkl333」
    """
    if caption_override is not None and str(caption_override).strip():
        text = str(caption_override).strip()
        grouped_id = getattr(message, "grouped_id", None)
        if album_captions is not None and grouped_id:
            album_captions[grouped_id] = text
        return text

    text = message_text(message)
    grouped_id = getattr(message, "grouped_id", None)

    if album_captions is not None and grouped_id:
        if text:
            album_captions[grouped_id] = text
        else:
            text = album_captions.get(grouped_id, "") or ""

    return text.strip()


def caption_filename_stem(caption: str) -> str:
    """
    File stem from caption body.
    「#风流狗尾巴 7.18自录 @csdkl333」→「自录」
    (tags / date / @mention already used for folders)
    """
    if not caption:
        return ""
    text = caption
    text = HASHTAG_RE.sub(" ", text)
    # Strip ranges before singles so「7.24-8.25」/「7.11-14」do not leave tails
    text = DATE_DOT_RANGE_RE.sub(" ", text)
    text = DATE_DOT_SAME_MONTH_RANGE_RE.sub(" ", text)
    text = DATE_CN_RANGE_RE.sub(" ", text)
    text = DATE_CN_SAME_MONTH_RANGE_RE.sub(" ", text)
    text = DATE_CN_RE.sub(" ", text)
    text = DATE_DOT_RE.sub(" ", text)
    text = MENTION_RE.sub(" ", text)
    return sanitize_name(WHITESPACE.sub(" ", text).strip(), max_len=80)


def resolve_media_subdir(
    message: Message,
    *,
    album_captions: Optional[dict] = None,
    use_caption_folders: bool = True,
    tag_folder_map: Optional[dict[str, str]] = None,
    group_dir: Optional[Path] = None,
    folder_mode: str = "caption",
    caption_override: Optional[str] = None,
    tag_blacklist: Optional[Iterable[str]] = None,
    batch_filenames: Optional[Iterable[str]] = None,
    hint_year: Optional[int] = None,
    nearby_texts: Optional[Iterable[str]] = None,
) -> str:
    """
    Resolve relative folder under group dir.

    folder_mode:
      - caption: #tags + date（月日←文案，年←文件名 / 附近文案）
      - media_type: photo/video/...
      - flat: no subfolder (files directly under group dir)
    """
    mode = (folder_mode or "caption").strip()
    if mode not in ("caption", "media_type", "flat"):
        mode = "caption" if use_caption_folders else "flat"

    if mode == "flat":
        return ""
    if mode == "media_type":
        mt = detect_media_type(message) or "file"
        if mt == "sticker":
            mt = "document"
        return mt
    if not use_caption_folders:
        return "_未分类"

    combined = resolve_caption_text(
        message,
        album_captions=album_captions,
        caption_override=caption_override,
    )
    tags = extract_tags(combined)
    names = list(batch_filenames) if batch_filenames is not None else []
    if not names:
        fn = _original_filename(message)
        if fn:
            names = [fn]
    # Also peek sibling year-prefixed date folders under the tag dir
    disk_hint = hint_year
    if disk_hint is None and group_dir and group_dir.is_dir() and tags:
        try:
            for p in group_dir.iterdir():
                if not p.is_dir() or not looks_like_date_folder(p.name):
                    # tag folders — look one level down later via nearby
                    continue
                ys = extract_years_from_text(p.name)
                if ys:
                    disk_hint = ys[0]
                    break
        except OSError:
            pass
    date_token = resolve_caption_date_token(
        combined,
        names,
        hint_year=disk_hint if hint_year is None else hint_year,
        nearby_texts=nearby_texts,
    )
    if date_token:
        date_token = sanitize_name(date_token, max_len=40)

    # Use full related multi-tag folder name (index co-occurrence + disk),
    # not only the tags present on this single caption.
    category: Optional[str] = None
    if tags:
        category = canonical_tag_folder(
            group_dir,
            tags,
            tag_folder_map=tag_folder_map,
            blacklist=tag_blacklist,
        )

    if category and date_token:
        return f"{category}/{date_token}"
    if category:
        return category
    if date_token:
        return f"_未分类/{date_token}"
    return "_未分类"


def build_filename(
    message: Message,
    media_type: Optional[str],
    *,
    album_captions: Optional[dict] = None,
    caption: Optional[str] = None,
) -> str:
    """
    Use Telegram original filename when present.
    Folder layout still comes from caption elsewhere; caption args kept for call-site compat.
    """
    _ = album_captions, caption  # unused — folders use caption, files use original name
    original = _original_filename(message)
    if original:
        return sanitize_name(original, max_len=180)

    ext = _extension_for_message(message, media_type)
    return f"{media_type or 'file'}_{message.id}{ext}"


def next_folder_state(
    current_folder: Optional[str],
    message: Message,
    *,
    use_text_as_folder: bool,
    min_len: int,
    album_captions: Optional[dict] = None,
    tag_folder_map: Optional[dict[str, str]] = None,
    group_dir: Optional[Path] = None,
    folder_mode: str = "caption",
    tag_blacklist: Optional[Iterable[str]] = None,
) -> tuple[Optional[str], Optional[str], bool]:
    if not has_media(message):
        return current_folder, None, True

    subdir = resolve_media_subdir(
        message,
        album_captions=album_captions,
        use_caption_folders=use_text_as_folder,
        tag_folder_map=tag_folder_map,
        group_dir=group_dir,
        folder_mode=folder_mode,
        tag_blacklist=tag_blacklist,
    )
    return subdir, subdir, False


def file_looks_complete(path: Path, expected_size: int = 0) -> bool:
    """True if path exists and size looks like a finished download (not .part)."""
    try:
        if not path or not path.exists() or not path.is_file():
            return False
        if str(path).endswith(".part"):
            return False
        size = path.stat().st_size
        if size <= 0:
            return False
        if expected_size and expected_size > 0 and size != int(expected_size):
            return False
        return True
    except OSError:
        return False


def collect_local_message_ids(
    group_dir: Path,
    known_message_ids: Optional[set[int]] = None,
) -> set[int]:
    """
    Scan group_dir for filenames that embed a Telegram message id.

    Matches: ``photo_123.jpg``, ``name_123.mp4``, ``name_123_1.mp4``.
    Only ids present in ``known_message_ids`` (index) are kept — avoids false hits.
    """
    found: set[int] = set()
    root = Path(group_dir) if group_dir else None
    if not root or not root.exists():
        return found
    pat = re.compile(r"_(\d+)(?:_\d+)?\.[^.]+$", re.IGNORECASE)
    try:
        for p in root.rglob("*"):
            try:
                if not p.is_file():
                    continue
                name = p.name
                if name.endswith(".part"):
                    continue
                m = pat.search(name)
                if not m:
                    continue
                mid = int(m.group(1))
                if known_message_ids is not None and mid not in known_message_ids:
                    continue
                found.add(mid)
            except (OSError, ValueError):
                continue
    except OSError:
        return found
    return found


def build_identical_file_index(group_dir: Path) -> dict[tuple[str, int], Path]:
    """
    Index finished files under group_dir by (filename_lower, size).
    Used to skip re-downloading identical media already saved in any subfolder.
    """
    index: dict[tuple[str, int], Path] = {}
    if not group_dir or not group_dir.exists():
        return index
    try:
        for p in group_dir.rglob("*"):
            try:
                if not p.is_file():
                    continue
                name = p.name
                if name.endswith(".part"):
                    continue
                size = p.stat().st_size
                if size <= 0:
                    continue
                key = (name.lower(), int(size))
                if key not in index:
                    index[key] = p
            except OSError:
                continue
    except OSError:
        return index
    return index


def find_identical_file(
    filename: str,
    expected_size: int,
    *,
    dup_index: Optional[dict[tuple[str, int], Path]] = None,
    group_dir: Optional[Path] = None,
) -> Optional[Path]:
    """
    Find an already-saved file with the same name and size (一模一样).
    Prefers dup_index; falls back to scanning group_dir when size is known.
    """
    name = (filename or "").strip()
    if not name or name.endswith(".part"):
        return None
    size = int(expected_size or 0)
    if size <= 0:
        return None
    key = (name.lower(), size)
    if dup_index is not None:
        hit = dup_index.get(key)
        if hit is not None and file_looks_complete(hit, size):
            return hit
        return None
    if not group_dir or not group_dir.exists():
        return None
    try:
        for p in group_dir.rglob(name):
            if p.is_file() and file_looks_complete(p, size):
                return p
    except OSError:
        return None
    return None


def resolve_download_path(
    directory: Path,
    filename: str,
    message_id: int,
    expected_size: int = 0,
) -> tuple[Path, bool]:
    """
    Pick a target path. Returns (path, already_complete).
    Reuses existing complete files (original name or name_msgid) instead of re-downloading.
    """
    directory.mkdir(parents=True, exist_ok=True)
    primary = directory / filename
    stem = primary.stem
    suffix = primary.suffix
    with_id = directory / f"{stem}_{message_id}{suffix}"

    for candidate in (primary, with_id):
        if file_looks_complete(candidate, expected_size):
            return candidate, True

    # Incomplete primary → overwrite it on next download
    if primary.exists() and not file_looks_complete(primary, expected_size):
        return primary, False
    if not primary.exists():
        return primary, False
    # Primary exists but size unknown / treated incomplete above; prefer msgid name
    if not with_id.exists():
        return with_id, False
    i = 1
    while True:
        alt = directory / f"{stem}_{message_id}_{i}{suffix}"
        if file_looks_complete(alt, expected_size):
            return alt, True
        if not alt.exists():
            return alt, False
        i += 1


def unique_path(directory: Path, filename: str, message_id: int) -> Path:
    path, _done = resolve_download_path(directory, filename, message_id, expected_size=0)
    return path
