"""Generate the Tweezer Picks brand mark: favicon set + social card.

Single-sources the geometry so the shipped SVG and the rasters cannot drift.
The SVG is emitted as plain rects/paths with NO font reference: a favicon is
rendered by browsers, OS docks and link unfurlers on machines that will not
have the site's display face installed, so a font-name reference would render
differently or not at all.

Palette is the live Lot Poster theme (the header wordmark is orange on paper
with an ink/gold letterpress offset), NOT the retired teal-green of the
original platform placeholder.

Run from the repo root:
    python3 scripts/make_brand_assets.py

Writes into src/setlist_stash/static/. Regenerating is idempotent.
"""

from __future__ import annotations

import pathlib

from PIL import Image, ImageDraw, ImageFont

# --- Lot Poster palette (from the deployed theme's CSS custom properties) ---
PAPER = "#f4e6cc"
INK = "#3a2520"
ORANGE = "#d6691f"
GOLD = "#e9a832"
BRICK = "#b03d1a"
RULE = "#c8ad85"

STATIC = pathlib.Path(__file__).resolve().parent.parent / "src/setlist_stash/static"

# --- Mark geometry, in a 100x100 unit square ---------------------------------
# A heavy slab T that fills its tile. Sized so the 16x16 favicon keeps a ~3px
# stem: anything lighter dissolves into the tab strip.
TILE_RADIUS = 18.0
BAR_X0, BAR_X1 = 12.0, 88.0     # crossbar horizontal extent
BAR_Y0, BAR_Y1 = 18.0, 40.0     # crossbar vertical extent (22 thick)
STEM_X0, STEM_X1 = 39.0, 61.0   # stem horizontal extent (22 wide)
STEM_Y1 = 86.0                  # stem foot


def svg_markup() -> str:
    """The scalable mark. Flat two-colour by design.

    The wordmark's letterpress offset (ink at 2px, gold at 4px) is deliberately
    NOT reproduced here. Layered offsets turn to mud below ~32px, and this one
    file is what a browser renders at 16px in a tab. The full treatment lives on
    the 180px touch icon and the social card, which are never shown small.
    """
    path = (
        f"M{BAR_X0:g} {BAR_Y0:g}H{BAR_X1:g}V{BAR_Y1:g}H{STEM_X1:g}"
        f"V{STEM_Y1:g}H{STEM_X0:g}V{BAR_Y1:g}H{BAR_X0:g}Z"
    )
    return (
        '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 100 100"'
        ' width="100" height="100" role="img" aria-label="Tweezer Picks">\n'
        f'  <rect x="0" y="0" width="100" height="100"'
        f' rx="{TILE_RADIUS:g}" fill="{ORANGE}"/>\n'
        f'  <path d="{path}" fill="{PAPER}"/>\n'
        "</svg>\n"
    )


def _rounded_tile(size: int, fill: str, supersample: int = 16) -> Image.Image:
    """Tile with smooth corners, drawn big and downsampled."""
    big = size * supersample
    img = Image.new("RGBA", (big, big), (0, 0, 0, 0))
    ImageDraw.Draw(img).rounded_rectangle(
        [0, 0, big - 1, big - 1], radius=TILE_RADIUS / 100.0 * big, fill=fill
    )
    return img.resize((size, size), Image.LANCZOS)


def _snap(v: float, size: int) -> int:
    """Map a 0-100 unit coordinate to an integer pixel at `size`.

    The T is snapped to whole pixels rather than supersampled with the tile.
    Downsampling a small glyph softens every edge; snapping keeps the stem a
    hard-edged run of pixels, which is the whole game at 16x16.
    """
    return round(v / 100.0 * size)


def render_mark(size: int, letterpress: bool = False) -> Image.Image:
    """The mark at `size` px. Smooth tile corners, pixel-crisp T."""
    img = _rounded_tile(size, ORANGE)
    d = ImageDraw.Draw(img)

    def draw_t(dx: int, dy: int, fill: str) -> None:
        d.rectangle(
            [_snap(BAR_X0, size) + dx, _snap(BAR_Y0, size) + dy,
             _snap(BAR_X1, size) + dx - 1, _snap(BAR_Y1, size) + dy - 1],
            fill=fill,
        )
        d.rectangle(
            [_snap(STEM_X0, size) + dx, _snap(BAR_Y0, size) + dy,
             _snap(STEM_X1, size) + dx - 1, _snap(STEM_Y1, size) + dy - 1],
            fill=fill,
        )

    if letterpress:
        # Only at sizes where two offsets stay separable.
        off = max(1, size // 45)
        draw_t(off * 2, off * 2, GOLD)
        draw_t(off, off, INK)
    draw_t(0, 0, PAPER)
    return img


# --- Social card -------------------------------------------------------------
OG_W, OG_H = 1200, 630
MARGIN = 72  # safe margin: keeps content clear of platform corner crops
BLACK_FONT = "/System/Library/Fonts/Supplemental/Arial Black.ttf"
BOLD_FONT = "/System/Library/Fonts/Supplemental/Arial Bold.ttf"


def _rotated_mark(size: int, degrees: float) -> Image.Image:
    """Rotate the mark without smearing the letterpress offsets.

    Rotating at final size resamples the 4-5px ink/gold offsets into mush.
    Rendering 4x, rotating, then downsampling keeps them as clean edges.
    """
    hi = render_mark(size * 4, letterpress=True)
    hi = hi.rotate(degrees, resample=Image.BICUBIC, expand=True)
    return hi.resize(
        (round(hi.width / 4), round(hi.height / 4)), Image.LANCZOS
    )


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


def render_og(title: str, taglines: list[str]) -> Image.Image:
    """1200x630 link-unfurl card. Carries the full poster treatment.

    A social card is not a big favicon: it needs the wordmark and a line saying
    what the thing is, because most impressions are a thumbnail next to a URL.
    """
    img = Image.new("RGB", (OG_W, OG_H), PAPER)
    d = ImageDraw.Draw(img)

    # Poster stripes top and bottom, echoing the theme's .brand-rule bands.
    band_h = 18
    for x in range(0, OG_W, 60):
        colour = [ORANGE, BRICK, GOLD, INK][(x // 60) % 4]
        d.rectangle([x, 0, x + 60, band_h], fill=colour)
        d.rectangle([x, OG_H - band_h, x + 60, OG_H], fill=colour)

    # Inner keyline, the letterpress border.
    d.rectangle(
        [36, band_h + 22, OG_W - 36, OG_H - band_h - 22], outline=RULE, width=3
    )

    # Mark, tilted like the header wordmark (which sits at rotate(-1.5deg)).
    mark_size = 250
    mark = _rotated_mark(mark_size, 1.5)
    mark_x = 104
    img.paste(mark, (mark_x, (OG_H - mark.height) // 2), mark)

    text_x = mark_x + mark_size + 62
    avail_w = OG_W - text_x - 84
    title_font = _fit_font(BLACK_FONT, title, avail_w, 100)
    tag_font = ImageFont.truetype(BOLD_FONT, 32)

    title_h = title_font.getbbox(title)[3]
    line_h = 44
    block_h = title_h + 26 + line_h * len(taglines)
    ty = (OG_H - block_h) // 2

    # Wordmark with the ink/gold letterpress offset from .brand.
    d.text((text_x + 5, ty + 5), title, font=title_font, fill=GOLD)
    d.text((text_x + 2, ty + 2), title, font=title_font, fill=INK)
    d.text((text_x, ty), title, font=title_font, fill=ORANGE)

    for i, line in enumerate(taglines):
        d.text(
            (text_x + 3, ty + title_h + 26 + i * line_h),
            line,
            font=tag_font,
            fill=INK,
        )
    return img


def main() -> None:
    STATIC.mkdir(parents=True, exist_ok=True)

    (STATIC / "logo-tweezer.svg").write_text(svg_markup())

    render_mark(192).save(STATIC / "logo-tweezer-192.png")
    render_mark(512, letterpress=True).save(STATIC / "logo-tweezer-512.png")
    render_mark(180, letterpress=True).save(STATIC / "apple-touch-icon-tweezer.png")

    # Multi-size ICO for browsers and bookmark bars that ignore the SVG.
    # Each frame is rendered NATIVELY at its own size. Handing Pillow one large
    # image plus `sizes=` makes it downsample internally, which discards the
    # per-size pixel snapping above and visibly softens the 16px frame.
    # The LARGEST frame must be the base image: Pillow drops any requested size
    # bigger than the image it is called on, so basing this on the 16px render
    # silently yields a single-frame icon.
    ico_frames = [render_mark(s) for s in (48, 32, 16)]
    ico_frames[0].save(
        STATIC / "favicon-tweezer.ico",
        format="ICO",
        sizes=[(16, 16), (32, 32), (48, 48)],
        append_images=ico_frames[1:],
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
