#!/usr/bin/env python3
"""Whiteout ALL non-title text from each song's top black-bordered header box.

Keeps only the song title (number + title, incl. a wrapped 2nd line and psalm refs like
"(Salmo 18)"); white-boxes every other bit of TEXT in the header rectangle — credits,
Rev/date stamps, author names, category tags, station ordinals. The friar/tau/A.C.T.S.
figure is a raster image (no text) and is left alone; anything OUTSIDE the box (chords,
TONO/CAPO/INTRO, lyrics) is untouched.

Usage:
  python3 scripts/clean-header-boxes.py [--in assets/alvernia_manual_2.pdf] [--out <path>]

Notes on this specific PDF (pypdf-assembled): pdftotext y-extents are unreliable (big-font
parentheticals over-report), so font-SIZE classification uses bbox height while rectangle
PLACEMENT is pixel-snapped from a render. Box borders are thin/faint on some pages, so they
are detected by the longest continuous dark run per row, not a dark-pixel fraction.

Requires: poppler (pdftoppm, pdftotext), pikepdf, Pillow.
"""
import re, os, sys, argparse, tempfile, subprocess
import pikepdf
from PIL import Image

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ap = argparse.ArgumentParser()
ap.add_argument("--in", dest="src", default=os.path.join(ROOT, "assets/alvernia_manual_2.pdf"))
ap.add_argument("--out", dest="out", default=None)
ap.add_argument("--index", default=os.path.join(ROOT, "src/alverniaManual2SongIndex.js"))
ap.add_argument("--dpi", type=int, default=110)
args = ap.parse_args()
SRC = args.src
OUT = args.out or SRC  # default: edit in place
DPI = float(args.dpi); SC = DPI / 72.0

TITLE_FRAC = 0.62
FIGURE_X = 0.97
DARK = 130
PAREN_KEEP = re.compile(r'salmo', re.I)
ANNOT = re.compile(r'estaci|\bcero\b|ofertorio', re.I)
REVWORD = re.compile(r'^rev\d*$', re.I)
ORDINAL = re.compile(r'^\d{1,3}(a|o|er|ra|do|to|ta|ma|va|na|os|as|º|ª|°)$', re.I)
DATEISH = re.compile(r'^[\d/.\-~:]+$')

tmp = tempfile.mkdtemp(prefix="cleanbox-")
# bbox (word coordinates) + page renders, both from the SOURCE pdf
bbox_html = subprocess.run(["pdftotext", "-bbox", SRC, "-"], capture_output=True, text=True).stdout
subprocess.run(["pdftoppm", "-png", "-r", str(int(DPI)), SRC, os.path.join(tmp, "p")], check=True)

blocks = re.split(r'<page\b', bbox_html)[1:]
word_re = re.compile(r'<word xMin="([\d.]+)" yMin="([\d.]+)" xMax="([\d.]+)" yMax="([\d.]+)">(.*?)</word>', re.S)
def words_of(pageno):
    return [(float(a), float(b), float(c), float(d), e) for a, b, c, d, e in word_re.findall(blocks[pageno - 1])]

_cache = {}
def render(pageno):
    if pageno not in _cache:
        _cache[pageno] = Image.open(os.path.join(tmp, f"p-{pageno:03d}.png")).convert("L")
    return _cache[pageno]

def detect_box(im):
    W, H = im.size; px = im.load()
    xa, xb = int(0.05 * W), int(0.95 * W); span = xb - xa
    def longest_run(y):
        best = cur = 0
        for x in range(xa, xb):
            if px[x, y] < 195:
                cur += 1; best = cur if cur > best else best
            else:
                cur = 0
        return best
    rows = [y for y in range(int(0.42 * H)) if longest_run(y) > 0.55 * span]
    if len(rows) < 2:
        return None
    groups = []
    for y in rows:
        if groups and y - groups[-1][-1] <= 3:
            groups[-1].append(y)
        else:
            groups.append([y])
    if len(groups) < 2:
        return None
    return groups[0][-1] + 1, groups[1][0] - 1

def ink_span(im, x0, x1, y0, y1):
    px = im.load()
    xa, xb = max(0, int(x0 * SC)), min(im.width, int(x1 * SC))
    ya, yb = max(0, int((y0 - 5) * SC)), min(im.height, int((y1 + 5) * SC))
    if xb <= xa:
        return None
    rows = [y for y in range(ya, yb) if any(px[x, y] < DARK for x in range(xa, xb, 2))]
    return (min(rows), max(rows)) if rows else None

canon = [(int(a), int(b)) for a, b in re.findall(r'\[(\d+),\s*(\d+)\]', open(args.index).read())]
pdf = pikepdf.open(SRC)
n_pages = n_rects = 0

for song, pageno in canon:
    obj = pdf.pages[pageno - 1].obj
    mb = [float(v) for v in obj["/MediaBox"]]
    if abs(mb[0]) > 0.5 or abs(mb[1]) > 0.5:
        continue
    mw, mh = mb[2], mb[3]
    im = render(pageno)
    box = detect_box(im)
    if not box:
        continue
    top_px, bot_px = box
    top_pt, bot_pt = top_px / SC, bot_px / SC
    words = [w for w in words_of(pageno)
             if top_pt - 1 <= (w[1] + w[3]) / 2 <= bot_pt + 1 and (w[0] + w[2]) / 2 < FIGURE_X * mw]
    inked = []
    for w in words:
        sp = ink_span(im, w[0], w[2], w[1], w[3])
        if sp:
            inked.append((w, sp))
    if not inked:
        continue
    def bh(t): return t[0][3] - t[0][1]
    max_h = max(bh(t) for t in inked)
    if max_h < 8:
        continue
    inked.sort(key=lambda t: (t[1][0], t[0][0]))
    lines = []
    for t in inked:
        if lines and abs(lines[-1][-1][1][0] - t[1][0]) < 12 * SC:
            lines[-1].append(t)
        else:
            lines.append([t])
    for L in lines:
        L.sort(key=lambda t: t[0][0])

    def paren_spans(L):
        i = 0
        while i < len(L):
            if "(" in L[i][0][4]:
                j = i
                while j < len(L) and ")" not in L[j][0][4]:
                    j += 1
                end = min(j, len(L) - 1)
                yield i, end, " ".join(L[k][0][4] for k in range(i, end + 1))
                i = end + 1
            else:
                i += 1
    def paren_credit_idx(L):
        out = set()
        for a, b, grp in paren_spans(L):
            if not PAREN_KEEP.search(grp):
                out.update(range(a, b + 1))
        return out
    def all_paren_idx(L):
        out = set()
        for a, b, _ in paren_spans(L):
            out.update(range(a, b + 1))
        return out
    def credit_start(L, pc):
        for idx, t in enumerate(L):
            word = t[0][4]
            if idx in pc or ANNOT.search(word) or ORDINAL.match(word) or REVWORD.match(word):
                return idx
            if DATEISH.match(word) and bh(t) < 0.7 * max_h:
                return idx
        return None

    clutter = []
    for L in lines:
        aps = all_paren_idx(L)
        nonparen = [bh(t) for idx, t in enumerate(L) if idx not in aps]
        if not (nonparen and max(nonparen) >= TITLE_FRAC * max_h):
            clutter.extend(L); continue
        cs = credit_start(L, paren_credit_idx(L))
        if cs is not None:
            clutter.extend(L[cs:])
    if not clutter:
        continue

    clutter.sort(key=lambda t: (t[1][0], t[0][0]))
    groups = []
    for t in clutter:
        if groups and abs(groups[-1][-1][1][0] - t[1][0]) < 10 * SC and (t[0][0] - groups[-1][-1][0][2]) < 40:
            groups[-1].append(t)
        else:
            groups.append([t])
    rects = []
    for grp in groups:
        gx0 = min(t[0][0] for t in grp) - 3
        gx1 = max(t[0][2] for t in grp) + 3
        gtop = max(min(t[1][0] for t in grp) - 2, top_px)
        gbot = min(max(t[1][1] for t in grp) + 2, bot_px)
        if gbot <= gtop:
            continue
        rects.append((gx0, mh - gbot / SC, gx1 - gx0, (gbot - gtop) / SC))
    if not rects:
        continue
    page = pdf.pages[pageno - 1]
    ops = "Q\nq\n1 1 1 rg\n" + "".join(f"{x:.3f} {y:.3f} {w:.3f} {h:.3f} re\n" for x, y, w, h in rects) + "f\nQ\n"
    page.contents_add(pikepdf.Stream(pdf, b"q\n"), prepend=True)
    page.contents_add(pikepdf.Stream(pdf, ops.encode()), prepend=False)
    n_pages += 1; n_rects += len(rects)

pdf.save(OUT)
print(f"cleaned {n_pages} pages ({n_rects} whiteout rects) -> {OUT}")
