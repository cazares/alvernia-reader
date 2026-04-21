#!/usr/bin/env python3

import argparse
import json
import re
import subprocess
import sys
import tempfile
from pathlib import Path

from PIL import Image, ImageEnhance, ImageOps


def _run(cmd: list[str]) -> str:
  p = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
  if p.returncode != 0:
    raise RuntimeError(f"command failed: {' '.join(cmd)}\n{p.stderr.strip()}")
  return p.stdout


LETTER_RE = re.compile(r"[A-Za-zÁÉÍÓÚÜÑáéíóúüñ]")


def _plausible_title(s: str) -> bool:
  s = s.strip()
  if len(s) < 5:
    return False
  letters = LETTER_RE.findall(s)
  if len(letters) < 4:
    return False
  alnum_space_ratio = len(re.findall(r"[A-Za-zÁÉÍÓÚÜÑáéíóúüñ0-9 ]", s)) / max(1, len(s))
  if alnum_space_ratio < 0.78:
    return False
  return True


def _clean_line(s: str) -> str:
  s = s.strip()
  s = s.replace("’", "'").replace("“", "\"").replace("”", "\"")
  s = re.sub(r"[_~`^|{}<>\\[\\]]", " ", s)
  s = re.sub(r"\\s{2,}", " ", s).strip()
  return s


def _extract_song_and_title(ocr_text: str) -> tuple[int, str] | None:
  lines = [_clean_line(ln) for ln in ocr_text.splitlines()]
  lines = [ln for ln in lines if ln]
  lines = lines[:12]

  # Pattern A: "123. TITLE" or "123 - TITLE"
  pat_inline = re.compile(r"^(\d{1,4})\s*[.\-:)]\s*(.+)$")
  # Pattern B: "123    TITLE"
  pat_spaces = re.compile(r"^(\d{1,4})\s{2,}(.+)$")

  for i, ln in enumerate(lines):
    m = pat_inline.match(ln) or pat_spaces.match(ln)
    if m:
      song = int(m.group(1))
      title = _clean_line(m.group(2))
      if 0 < song < 5000 and _plausible_title(title):
        return (song, title)

    # Pattern C: a bare number line followed by a title line
    if re.fullmatch(r"\d{1,4}", ln):
      song = int(ln)
      if 0 < song < 5000 and i + 1 < len(lines):
        title = _clean_line(lines[i + 1])
        if _plausible_title(title):
          return (song, title)

  return None


def _ocr_crop(page_path: Path, crop_ratio: float) -> str:
  with Image.open(page_path) as im:
    im = im.convert("RGB")
    w, h = im.size
    crop_h = max(1, int(h * crop_ratio))
    cropped = im.crop((0, 0, w, crop_h))
    cropped = ImageOps.grayscale(cropped)
    cropped = ImageEnhance.Contrast(cropped).enhance(2.2)
    cropped = ImageEnhance.Sharpness(cropped).enhance(2.0)
    cropped = cropped.resize((w * 2, crop_h * 2))

    with tempfile.TemporaryDirectory() as td:
      tmp_img = Path(td) / "crop.png"
      cropped.save(tmp_img)
      out_base = Path(td) / "out"
      _run(["tesseract", str(tmp_img), str(out_base), "--psm", "6", "-l", "eng"])
      return (out_base.with_suffix(".txt")).read_text("utf-8", errors="ignore")


def main() -> int:
  ap = argparse.ArgumentParser()
  ap.add_argument("--id", required=True)
  ap.add_argument("--pages-dir", required=True)
  ap.add_argument("--pages-json", required=True)
  ap.add_argument("--out-dir", required=True)
  ap.add_argument("--crop-ratio", type=float, default=0.24)
  ap.add_argument("--max-pages", type=int, default=0)
  args = ap.parse_args()

  pages_dir = Path(args.pages_dir)
  pages_json_path = Path(args.pages_json)
  out_dir = Path(args.out_dir)

  pages_meta = json.loads(pages_json_path.read_text("utf-8"))
  total = int(pages_meta.get("totalPages") or 0)
  if total <= 0:
    raise RuntimeError("pages.json.totalPages missing/invalid")

  max_pages = int(args.max_pages or 0)
  if max_pages > 0:
    total = min(total, max_pages)

  seen: set[int] = set()
  song_index: list[dict] = []
  song_titles: dict[str, str] = {}
  song_search: list[dict] = []

  for page in range(1, total + 1):
    page_path = pages_dir / f"page-{page:03d}.jpg"
    if not page_path.exists():
      continue
    try:
      raw = _ocr_crop(page_path, float(args.crop_ratio))
      hit = _extract_song_and_title(raw)
    except Exception:
      hit = None
    if hit:
      song, title = hit
      if song not in seen:
        seen.add(song)
        song_index.append({"song": song, "page": page})
        song_titles[str(song)] = title
        song_search.append({
          "song": song,
          "page": page,
          "title": title,
          "normalized": f"{song}. {title}".lower(),
          "lyrics": "",
        })
    if page % 25 == 0:
      print(f"[{args.id}] OCR {page}/{total} (songs={len(song_index)})", file=sys.stderr)

  song_index.sort(key=lambda x: int(x["song"]))

  # Update pages.json with the derived songIndex for navigation.
  pages_meta["songIndex"] = song_index
  pages_json_path.write_text(json.dumps(pages_meta, ensure_ascii=False, indent=2) + "\n", "utf-8")

  (out_dir / "song-titles.json").write_text(json.dumps(song_titles, ensure_ascii=False, indent=2) + "\n", "utf-8")
  (out_dir / "song-search-index.json").write_text(json.dumps(song_search, ensure_ascii=False, indent=2) + "\n", "utf-8")

  print(f"Wrote songs={len(song_index)} to {out_dir}/song-search-index.json", file=sys.stderr)
  return 0


if __name__ == "__main__":
  sys.exit(main())
