#!/usr/bin/env python3
"""splice-song-pages.py — put a new or revised song into the songbook at a given page number.

Song number IS page number (web/build.mjs derives the index from the PDF), so adding song 372
means making the book's page 372 be that song's sheet. This is the tool for that.

    python3 scripts/splice-song-pages.py \
        --page 371 "~/Downloads/371. Santo Espanol.pdf" \
        --page 372 "~/Downloads/372. Piedad (Alamitos).pdf" \
        --expect-pages 372

It does three things to each incoming sheet, IN THIS ORDER, and the order is the point:

  1. NORMALISE THE GEOMETRY to the book's own page box. The sheets come out of PowerPoint at
     540x720pt; the book is 768x1024pt. web/build.mjs renders at a fixed 115 DPI, NOT to a fixed
     pixel width — so a 540x720 page spliced in raw renders 863px wide next to its neighbours'
     1227px and is visibly softer on the same iPad. That is not hypothetical: page 371 shipped
     that way in v425, the only page of 371 with a different box.
  2. CLEAN THE HEADER BOX, by calling scripts/clean-header-boxes.py --pages 1 — the same code
     that cleaned the other ~290 pages (build 377 / PR #257). A raw sheet keeps its
     "(Autor) Rev 00 05 08 2026" credit line inside the top bordered box; every page already in
     the book has had that removed. Splicing a raw sheet is a VISIBLE REGRESSION, and it has
     already happened twice — 369 and 370 went in raw.
  3. SPLICE, replacing an existing page or appending a contiguous new one — into a temp file.
  4. VERIFY WITH POPPLER before the temp file becomes the book: every spliced page is rendered
     from the final file with pdftoppm (what web/build.mjs uses) and must contain ink and match
     the render of the sheet that was prepared for it. This exists because a valid-looking splice
     CAN render blank under poppler and green under every other gate — see normalise() for the
     empty-stream case the mutation harness found. Only a verified file is moved into place.

Everything else in the book is left alone, and `--expect-pages` makes the final count an
assertion rather than a hope. There is deliberately NO way to leave a gap: appending page 374
to a 371-page book is refused, because the missing 372/373 would be blank pages that a singer
typing "372" would land on.

Options:
    --page N <pdf>       repeatable; N <= current count REPLACES, N == count+1.. APPENDS
    --book <pdf>         default assets/songbook.pdf
    --out <pdf>          default: edit the book in place
    --src-page N         which page of the source sheet to take (default 1)
    --expect-pages N     refuse to write unless the result has exactly N pages
    --no-clean           skip the header-box cleanup (raw splice — you almost never want this)
    --dry-run            report what would happen; write nothing

Requires: pikepdf, Pillow, poppler (pdftoppm, pdftotext).
"""
import argparse
import os
import re
import shutil
import subprocess
import sys
import tempfile

import pikepdf

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CLEANER = os.path.join(ROOT, "scripts", "clean-header-boxes.py")


def die(msg):
    print(f"✖ {msg}", file=sys.stderr)
    sys.exit(1)


def show(path):
    """Repo-relative when it is in the repo, absolute otherwise — never ../../../.. noise."""
    rel = os.path.relpath(path, ROOT)
    return path if rel.startswith("..") else rel


def page_box(page):
    """(x0, y0, w, h) of a page's MediaBox, inheritance resolved by pikepdf."""
    mb = [float(v) for v in page.mediabox]
    return mb[0], mb[1], mb[2] - mb[0], mb[3] - mb[1]


def book_geometry(pdf):
    """The book's own page box = the most common MediaBox size, rounded to whole points.

    Taken from the book rather than hardcoded so this keeps working if the songbook is ever
    re-exported at another size. Rounded because ~35 pages measure 768.24 x 1024.2 — the same
    box with a float artefact, not a different one.
    """
    counts = {}
    for p in pdf.pages:
        _, _, w, h = page_box(p)
        key = (round(w), round(h))
        counts[key] = counts.get(key, 0) + 1
    return max(counts.items(), key=lambda kv: kv[1])[0]


def normalise(src_pdf_path, src_page_no, target_w, target_h, out_path):
    """Rewrite a one-page sheet onto the book's page box, preserving vector text.

    Scales the CONTENT with a `cm` matrix rather than re-distilling through ghostscript: the text
    stays selectable vector text (pdftotext feeds the search index) and the raster figure is not
    re-encoded. Aspect ratio is preserved and the result centred, so a sheet whose proportions do
    not match is letterboxed rather than stretched.
    """
    pdf = pikepdf.open(src_pdf_path)
    if src_page_no < 1 or src_page_no > len(pdf.pages):
        die(f"{src_pdf_path} has {len(pdf.pages)} page(s); --src-page {src_page_no} is out of range")
    page = pdf.pages[src_page_no - 1]
    x0, y0, w, h = page_box(page)

    scale = min(target_w / w, target_h / h)
    tx = (target_w - w * scale) / 2.0 - x0 * scale
    ty = (target_h - h * scale) / 2.0 - y0 * scale

    # q ... Q around the ORIGINAL content, so its own unbalanced graphics state cannot leak into
    # our transform (and ours cannot leak into it).
    page.contents_add(
        pikepdf.Stream(pdf, f"q\n{scale:.6f} 0 0 {scale:.6f} {tx:.6f} {ty:.6f} cm\n".encode()),
        prepend=True,
    )
    page.contents_add(pikepdf.Stream(pdf, b"\nQ\n"), prepend=False)

    page.mediabox = [0, 0, target_w, target_h]
    # A stale CropBox would re-crop the page to the OLD box and undo all of the above.
    for extra in ("/CropBox", "/TrimBox", "/BleedBox", "/ArtBox"):
        if extra in page.obj:
            del page.obj[extra]

    # ONE content stream, not an array. Found 2026-08-15 by the mutation harness, not by luck:
    # when a page's /Contents array contains an EMPTY stream (pikepdf's add_blank_page makes
    # one; other producers can too), the copy into the book can come out serialised as
    # `<< /Length 0 /Filter /FlateDecode >>` — zero bytes declared as Flate data, which is not a
    # zlib stream. Ghostscript shrugs; POPPLER fails the inflate and abandons the whole content
    # array, and poppler is what web/build.mjs renders the book with. The page reaches every
    # iPad BLANK, and every gate downstream (page count, manifest, consistency) reads green.
    # The PowerPoint sheets never hit it only because their content is a single non-empty
    # stream. Coalescing removes the empty member and the array boundaries in one move.
    page.contents_coalesce()

    out = pikepdf.Pdf.new()
    out.pages.append(page)
    out.save(out_path)
    out.close()
    pdf.close()
    return scale


def render_png(pdf_path, page_no, out_png, dpi=72):
    """Render one page with pdftoppm — the SAME renderer web/build.mjs feeds the devices from."""
    prefix = out_png[:-4] if out_png.endswith(".png") else out_png
    res = subprocess.run(
        ["pdftoppm", "-f", str(page_no), "-l", str(page_no), "-r", str(dpi), "-png",
         "-singlefile", pdf_path, prefix],
        capture_output=True, text=True,
    )
    if res.returncode != 0 or not os.path.exists(prefix + ".png"):
        die(f"pdftoppm could not render page {page_no} of {show(pdf_path)}: {res.stderr.strip()}")
    return prefix + ".png"


def verify_rendered(book_path, prepared, tmp):
    """The last word: render every spliced page from the FINAL file with poppler and require it to
    (a) contain ink and (b) match the render of the sheet we prepared. Anything the splice lost or
    mangled on the way in — an empty-stream quirk, a dropped resource, a wrong index — shows up
    here as blank or different, and this is the only check that looks at what a device will see
    rather than at what the file claims.
    """
    from PIL import Image, ImageChops
    for n, staged in prepared:
        got = render_png(book_path, n, os.path.join(tmp, f"final-{n}.png"))
        want = render_png(staged, 1, os.path.join(tmp, f"staged-{n}.png"))
        a = Image.open(got).convert("L")
        b = Image.open(want).convert("L")
        if Image.eval(a, lambda p: 255 - p).getbbox() is None:
            die(f"page {n} renders BLANK under poppler after the splice — refusing to write it. "
                f"(The prepared sheet {show(staged)} is what was spliced; inspect it with pdftoppm.)")
        if a.size != b.size or ImageChops.difference(a, b).getbbox() is not None:
            die(f"page {n} does not render the same as the sheet prepared for it — refusing to write. "
                f"({show(book_path)} p{n} vs {show(staged)})")


def clean_header_box(path):
    """Run the shared header-box cleaner over this single page. Returns True if it changed it."""
    res = subprocess.run(
        [sys.executable, CLEANER, "--in", path, "--out", path, "--pages", "1"],
        capture_output=True, text=True,
    )
    if res.returncode != 0:
        die(f"clean-header-boxes.py failed on {path}:\n{res.stderr.strip()}")
    m = re.search(r"cleaned (\d+) pages \((\d+) whiteout rects\)", res.stdout)
    return (int(m.group(1)), int(m.group(2))) if m else (0, 0)


def main():
    ap = argparse.ArgumentParser(add_help=True)
    ap.add_argument("--page", nargs=2, action="append", metavar=("N", "PDF"), default=[],
                    help="page number and the one-page sheet to put there (repeatable)")
    ap.add_argument("--book", default=os.path.join(ROOT, "assets/songbook.pdf"))
    ap.add_argument("--out", default=None)
    ap.add_argument("--src-page", type=int, default=1)
    ap.add_argument("--expect-pages", type=int, default=None)
    ap.add_argument("--no-clean", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    if not args.page:
        die("nothing to do: pass at least one --page N <pdf>")

    book_path = os.path.abspath(os.path.expanduser(args.book))
    out_path = os.path.abspath(os.path.expanduser(args.out)) if args.out else book_path
    if not os.path.exists(book_path):
        die(f"no such book: {book_path}")

    targets = []
    for n, src in args.page:
        if not re.fullmatch(r"\d+", str(n)):
            die(f"--page takes a page NUMBER first, got {n!r}")
        src = os.path.abspath(os.path.expanduser(src))
        if not os.path.exists(src):
            die(f"no such file: {src}")
        targets.append((int(n), src))
    targets.sort(key=lambda t: t[0])
    if len({n for n, _ in targets}) != len(targets):
        die("the same page number was given twice")

    # allow_overwriting_input so `--out` can be omitted and the book edited in place, which is the
    # normal call. pikepdf buffers the original rather than reading it lazily during save.
    book = pikepdf.open(book_path, allow_overwriting_input=True)
    current = len(book.pages)
    tw, th = book_geometry(book)

    # NO GAPS. Appends must be contiguous from current+1, or the book grows blank pages that a
    # singer typing that number lands on.
    appends = [n for n, _ in targets if n > current]
    expected = list(range(current + 1, current + 1 + len(appends)))
    if appends != expected:
        die(
            f"that would leave a gap: the book has {current} pages, so new pages must be "
            f"{', '.join(map(str, expected)) or '(none)'} — got {', '.join(map(str, appends))}."
        )

    final_pages = current + len(appends)
    if args.expect_pages is not None and final_pages != args.expect_pages:
        die(f"--expect-pages {args.expect_pages} but this would produce {final_pages} pages")

    print(f"book: {show(book_path)} — {current} pages, page box {tw}x{th}pt")
    for n, src in targets:
        verb = "REPLACE" if n <= current else "APPEND "
        print(f"  {verb} page {n:>4}  <-  {os.path.basename(src)}")
    print(f"result: {final_pages} pages")

    if args.dry_run:
        print("\n--dry-run: nothing written.")
        book.close()
        return

    tmp = tempfile.mkdtemp(prefix="splice-")
    try:
        prepared = []
        for n, src in targets:
            staged = os.path.join(tmp, f"page-{n}.pdf")
            scale = normalise(src, args.src_page, tw, th, staged)
            note = f"scaled x{scale:.4f}"
            if not args.no_clean:
                pages_cleaned, rects = clean_header_box(staged)
                note += f", header box: {rects} rect(s)" if pages_cleaned else ", header box: nothing to clean"
            print(f"  prepared page {n}: {note}")
            prepared.append((n, staged))

        # Every staged Pdf must stay OPEN until after save(): pikepdf resolves a foreign page copy
        # lazily, so closing the source first writes a book with a hole where the new song was.
        open_sources = []
        # Replacements first — `targets` is sorted ascending, so appends land in page order and
        # replacements never shift an index out from under a later one.
        for n, staged in prepared:
            src = pikepdf.open(staged)
            open_sources.append(src)
            if n <= current:
                book.pages[n - 1] = src.pages[0]
            else:
                book.pages.append(src.pages[0])

        if len(book.pages) != final_pages:
            die(f"internal error: assembled {len(book.pages)} pages, expected {final_pages}")

        # Write to a sibling temp file, verify THAT with the real renderer, and only then move it
        # over the target. Two things fall out of the ordering: the book on disk is never a
        # half-written file, and a splice that poppler would render blank never becomes the book.
        pending = out_path + ".splicing"
        book.save(pending)
        for src in open_sources:
            src.close()
        try:
            verify_rendered(pending, prepared, tmp)
            os.replace(pending, out_path)
        finally:
            if os.path.exists(pending):
                os.remove(pending)
        print(f"\n✅ wrote {show(out_path)} — {final_pages} pages (spliced pages verified under poppler)")
    finally:
        book.close()
        shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    main()
