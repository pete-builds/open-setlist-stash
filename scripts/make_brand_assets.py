"""Generate the Tweezer Picks brand mark: favicon set + social card.

Single-sources the geometry so the shipped SVG and the rasters cannot drift.
The SVG is emitted as plain rects/paths with NO font reference: a favicon is
rendered by browsers, OS docks and link unfurlers on machines that will not
have the site's display face installed, so a font-name reference would render
differently or not at all.

PALETTE PROVENANCE, and why the stylesheet alone was not enough. The header
wordmark is set in Honk, which is a COLRv1 *chromatic* font: it carries its
own colour layers inside the font binary and paints them regardless of the
CSS ``color`` property. So ``lot-poster.css`` setting ``color: var(--orange)``
on ``.brand`` is dead code, and reading the stylesheet gives a confidently
wrong answer. The glyph is actually a vertical yellow-to-magenta gradient
with a heavy black outline. The values below were sampled pixel-by-pixel from
a screenshot of the live header. The ``text-shadow`` layers in the CSS are
real and do render, so those two colours come from the stylesheet.

Run from the repo root:
    python3 scripts/make_brand_assets.py

Writes into src/setlist_stash/static/. Regenerating is idempotent.
"""

from __future__ import annotations

import itertools
import pathlib

from PIL import Image, ImageDraw, ImageFilter, ImageFont

# --- Sampled from the live header (see module docstring) ---------------------
# Vertical gradient across the glyph face, top to bottom.
GRADIENT = [
    (0.00, (0xFF, 0xFF, 0xB2)),  # pale-yellow highlight on the very top edge
    (0.20, (0xFF, 0xFF, 0x8C)),
    (0.48, (0xFF, 0xC7, 0x53)),  # amber
    (0.68, (0xFF, 0x7A, 0x5E)),  # coral
    (1.00, (0xFF, 0x3E, 0xAF)),  # hot pink
]
OUTLINE = (0x00, 0x00, 0x00)
PAPER = (0xF9, 0xEC, 0xD5)
INK = (0x3A, 0x25, 0x20)   # --ink, first text-shadow layer
GOLD = (0xE9, 0xA8, 0x32)  # --gold, second text-shadow layer
RULE = (0xC8, 0xAD, 0x85)

STATIC = pathlib.Path(__file__).resolve().parent.parent / "src/setlist_stash/static"

# --- Mark geometry, in a 100x100 unit square ---------------------------------
# A chunky, inflated slab T built as the union of two rounded rectangles. This
# is an original letterform in the spirit of Honk, NOT a trace: the font is not
# available here, and a bad trace reads worse than a clean original.
BAR = (8.0, 12.0, 92.0, 48.0)    # crossbar: x0, y0, x1, y1
STEM = (35.0, 12.0, 65.0, 88.0)  # stem
CORNER = 7.0


def _hex(rgb: tuple[int, int, int]) -> str:
    return "#{:02X}{:02X}{:02X}".format(*rgb)


def _lerp(a: int, b: int, t: float) -> int:
    return round(a + (b - a) * t)


def gradient_at(t: float) -> tuple[int, int, int]:
    """Colour at position `t` (0 top, 1 bottom) along the glyph gradient."""
    t = min(max(t, 0.0), 1.0)
    for (p0, c0), (p1, c1) in itertools.pairwise(GRADIENT):
        if t <= p1:
            k = 0.0 if p1 == p0 else (t - p0) / (p1 - p0)
            return (_lerp(c0[0], c1[0], k), _lerp(c0[1], c1[1], k),
                    _lerp(c0[2], c1[2], k))
    return GRADIENT[-1][1]


def svg_markup() -> str:
    """The scalable mark, matching the chosen raster variant.

    Ink tile plus gradient glyph, no font reference and no black outline (see
    render_mark for why the outline is dropped).
    """
    stops = "".join(
        f'\n      <stop offset="{p * 100:g}%" stop-color="{_hex(c)}"/>'
        for p, c in GRADIENT
    )
    glyph = "".join(
        f'\n  <rect x="{x0:g}" y="{y0:g}" width="{x1 - x0:g}"'
        f' height="{y1 - y0:g}" rx="{CORNER:g}" fill="url(#g)"/>'
        for x0, y0, x1, y1 in (BAR, STEM)
    )
    return (
        '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 100 100"'
        ' width="100" height="100" role="img" aria-label="Tweezer Picks">\n'
        "  <defs>\n"
        '    <linearGradient id="g" gradientUnits="userSpaceOnUse"'
        f' x1="0" y1="{BAR[1]:g}" x2="0" y2="{STEM[3]:g}">{stops}\n'
        "    </linearGradient>\n"
        "  </defs>\n"
        f'  <rect x="0" y="0" width="100" height="100" rx="18"'
        f' fill="{_hex(INK)}"/>'
        + glyph
        + "\n</svg>\n"
    )


# --- Raster rendering --------------------------------------------------------
SS = 8  # supersample factor; a gradient plus an outline cannot be pixel-snapped


def _shape_mask(px: int, inset_units: float) -> Image.Image:
    """8-bit mask of the T, optionally inflated by `inset_units` (0-100 space)."""
    m = Image.new("L", (px, px), 0)
    d = ImageDraw.Draw(m)
    for x0, y0, x1, y1 in (BAR, STEM):
        d.rounded_rectangle(
            [(x0 - inset_units) / 100 * px, (y0 - inset_units) / 100 * px,
             (x1 + inset_units) / 100 * px, (y1 + inset_units) / 100 * px],
            radius=(CORNER + inset_units) / 100 * px,
            fill=255,
        )
    return m


def _gradient_image(w: int, h: int, top_px: float, bot_px: float,
                    stops: list | None = None) -> Image.Image:
    """Vertical gradient sized `w` x `h`, ramping between two PIXEL rows.

    Bounds are pixels rather than a normalised square: building a square and
    resizing it to a wide box squashes the ramp, which silently flattened the
    wordmark to its end colour.
    """
    img = Image.new("RGB", (w, h))
    d = ImageDraw.Draw(img)
    for y in range(h):
        t = (y - top_px) / max(bot_px - top_px, 1e-6)
        if stops is not None:
            t = min(max(t, 0.0), 1.0)
            c = tuple(_lerp(stops[0][i], stops[1][i], t) for i in range(3))
        else:
            c = gradient_at(t)
        d.line([(0, y), (w, y)], fill=c)
    return img


def render_mark(size: int, variant: str = "no_outline_ink") -> Image.Image:
    """The mark at `size` px.

    The default drops the header's black outline and sets the glyph on an ink
    tile. Chosen by rendering the candidates and looking at them at actual size
    against a paper background, which is where the alternatives failed: a paper
    tile has no edge against light browser chrome, so the mark dissolves into
    the page instead of reading as an icon. On an ink tile the black outline is
    both invisible and redundant, because the tile already does the containing.
    The other variants are kept selectable so this comparison can be redone.
    """
    px = size * SS
    two_stop = [GRADIENT[1][1], GRADIENT[-1][1]]

    if variant == "outline_paper":
        tile, stops, outline = PAPER, None, True
    elif variant == "no_outline_ink":
        tile, stops, outline = INK, None, False
    elif variant == "two_stop":
        tile, stops, outline = INK, two_stop, False
    elif variant == "two_stop_outline":
        tile, stops, outline = PAPER, two_stop, True
    else:
        raise ValueError(f"unknown variant {variant!r}")

    img = Image.new("RGB", (px, px), tile)
    if outline:
        img.paste(OUTLINE, (0, 0), _shape_mask(px, 3.0))
    img.paste(
        _gradient_image(px, px, BAR[1] / 100 * px, STEM[3] / 100 * px, stops),
        (0, 0), _shape_mask(px, 0.0))

    out = img.resize((size, size), Image.LANCZOS)
    # Round the tile itself only where it is big enough to read as a corner.
    if size >= 48:
        rounded = Image.new("RGBA", (size, size), (0, 0, 0, 0))
        corner = Image.new("L", (px, px), 0)
        ImageDraw.Draw(corner).rounded_rectangle(
            [0, 0, px - 1, px - 1], radius=0.18 * px, fill=255
        )
        rounded.paste(out, (0, 0), corner.resize((size, size), Image.LANCZOS))
        return rounded
    return out.convert("RGBA")


# --- Social card -------------------------------------------------------------
OG_W, OG_H = 1200, 630
BLACK_FONT = "/System/Library/Fonts/Supplemental/Arial Black.ttf"
BOLD_FONT = "/System/Library/Fonts/Supplemental/Arial Bold.ttf"


def _fit_font(path: str, text: str, max_w: int, start: int) -> ImageFont.FreeTypeFont:
    """Largest size at which `text` still fits `max_w`.

    Measured rather than assumed: a hardcoded size silently overran the safe
    margin and pushed the wordmark into the card's keyline.
    """
    size = start
    while size > 24:
        font = ImageFont.truetype(path, size)
        if font.getbbox(text)[2] <= max_w:
            return font
        size -= 2
    return ImageFont.truetype(path, 24)


def _wordmark(text: str, font: ImageFont.FreeTypeFont, pad: int = 40
              ) -> Image.Image:
    """Wordmark with the header's full stack: gold and ink offsets, black
    outline, gradient face. Drawn back to front, matching the live header.
    """
    bb = font.getbbox(text)
    w, h = bb[2] + pad * 2, bb[3] + pad * 2
    face = Image.new("L", (w, h), 0)
    ImageDraw.Draw(face).text((pad, pad), text, font=font, fill=255)
    outline = face.filter(ImageFilter.MaxFilter(9))

    img = Image.new("RGBA", (w, h), (0, 0, 0, 0))
    img.paste(GOLD, (10, 12), outline)   # furthest offset, --gold
    img.paste(INK, (5, 6), outline)      # --ink
    img.paste(OUTLINE, (0, 0), outline)  # the font's own heavy black outline
    img.paste(_gradient_image(w, h, bb[1] + pad, bb[3] + pad), (0, 0), face)
    return img


def render_og(title: str, taglines: list[str]) -> Image.Image:
    """1200x630 link-unfurl card. Carries the full poster treatment.

    A social card is not a big favicon: it needs the wordmark and a line saying
    what the thing is, because most impressions are a thumbnail next to a URL.
    """
    img = Image.new("RGB", (OG_W, OG_H), PAPER)
    d = ImageDraw.Draw(img)

    band_h = 18
    for x in range(0, OG_W, 60):
        colour = [GRADIENT[2][1], GRADIENT[4][1], GOLD, INK][(x // 60) % 4]
        d.rectangle([x, 0, x + 60, band_h], fill=colour)
        d.rectangle([x, OG_H - band_h, x + 60, OG_H], fill=colour)

    d.rectangle(
        [36, band_h + 22, OG_W - 36, OG_H - band_h - 22], outline=RULE, width=3
    )

    mark_size = 248
    mark = render_mark(mark_size).rotate(
        1.5, resample=Image.BICUBIC, expand=True
    )
    mark_x = 104
    img.paste(mark, (mark_x, (OG_H - mark.height) // 2), mark)

    text_x = mark_x + mark_size + 56
    avail_w = OG_W - text_x - 84
    title_font = _fit_font(BLACK_FONT, title, avail_w, 104)
    word = _wordmark(title, title_font)
    tag_font = ImageFont.truetype(BOLD_FONT, 32)

    line_h = 44
    block_h = word.height + line_h * len(taglines)
    ty = (OG_H - block_h) // 2
    img.paste(word, (text_x - 40, ty), word)

    for i, line in enumerate(taglines):
        d.text((text_x + 3, ty + word.height + i * line_h), line,
               font=tag_font, fill=INK)
    return img


def main() -> None:
    STATIC.mkdir(parents=True, exist_ok=True)

    (STATIC / "logo-tweezer.svg").write_text(svg_markup())

    render_mark(192).save(STATIC / "logo-tweezer-192.png")
    render_mark(512).save(STATIC / "logo-tweezer-512.png")
    render_mark(180).save(STATIC / "apple-touch-icon-tweezer.png")

    # Each frame rendered NATIVELY at its own size. Handing Pillow one large
    # image plus `sizes=` makes it downsample internally, which visibly softens
    # the 16px frame. The largest must be the base image: Pillow drops any
    # requested size bigger than the image it is called on.
    frames = [render_mark(s) for s in (48, 32, 16)]
    frames[0].save(
        STATIC / "favicon-tweezer.ico",
        format="ICO",
        sizes=[(16, 16), (32, 32), (48, 48)],
        append_images=frames[1:],
    )

    render_og(
        "Tweezer Picks",
        ["Phish setlist prediction game.", "Pick 5 before the lights go down."],
    ).save(STATIC / "og-tweezer.png")

    for name in [
        "logo-tweezer.svg", "logo-tweezer-192.png", "logo-tweezer-512.png",
        "apple-touch-icon-tweezer.png", "favicon-tweezer.ico", "og-tweezer.png",
    ]:
        p = STATIC / name
        print(f"{name:34} {p.stat().st_size:>8} bytes")


if __name__ == "__main__":
    main()
