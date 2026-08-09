"""Generate the Tweezer Picks brand mark: favicon set + social card.

The mark is the real Honk "T", not an approximation. The header wordmark is
set in Honk, a COLRv1 *chromatic* font: it carries its own colour layers inside
the font binary and paints them regardless of the CSS ``color`` property, which
is why ``lot-poster.css`` setting ``color: var(--orange)`` on ``.brand`` never
renders and why reading the stylesheet gives a confidently wrong answer.

Source artwork lives in ``scripts/brand/*.svg``: the glyph and the wordmark
rendered to vector outlines with the colour layers resolved. Those files are
the input, so THIS SCRIPT NEEDS NO FONT, NO NETWORK AND NO EXTRA PACKAGES, and
regenerating is deterministic offline and in CI. Pillow is the only dependency,
and it cannot parse SVG, so the small path reader below flattens the outlines
and fills them directly.

Honk: Copyright 2023 The Honk Project Authors (https://github.com/EkType/Honk),
SIL Open Font License 1.1 (https://scripts.sil.org/OFL). Shipping rendered
artwork is fine under the OFL, the same as converting text to outlines. The
font binary is deliberately not vendored here.

Run from the repo root:
    python3 scripts/make_brand_assets.py

Writes into src/setlist_stash/static/. Regenerating is idempotent.
"""

from __future__ import annotations

import itertools
import pathlib
import re

from PIL import Image, ImageChops, ImageDraw, ImageFont

# --- Palette, read from the font's own CPAL table (palette 0) ----------------
# The glyph gradient is defined bottom-to-top in font space (y-up).
GRADIENT = [
    (0.00, (0xFF, 0x46, 0xAF)),  # bottom: hot pink
    (0.20, (0xFF, 0x3C, 0xAF)),
    (0.40, (0xFF, 0x75, 0x5F)),  # coral
    (0.60, (0xFF, 0xC7, 0x53)),  # amber
    (0.80, (0xFF, 0xFF, 0x78)),
    (1.00, (0xFF, 0xFF, 0xB2)),  # top: pale yellow
]
PAPER = (0xF9, 0xEC, 0xD5)
INK = (0x3A, 0x25, 0x20)   # --ink, the theme's warm near-black
GOLD = (0xE9, 0xA8, 0x32)  # --gold
CREAM_POP = (0xF0, 0xD9, 0xB8)  # --cream-pop
ORANGE = (0xD6, 0x69, 0x1F)     # --orange
BRICK = (0xB0, 0x3D, 0x1A)      # --brick
RULE = (0xC8, 0xAD, 0x85)
BLACK = (0x00, 0x00, 0x00)

ROOT = pathlib.Path(__file__).resolve().parent.parent
STATIC = ROOT / "src/setlist_stash/static"
BRAND = ROOT / "scripts/brand"

# --- Minimal SVG path reader -------------------------------------------------
# Handles exactly the command set the exported outlines use: M (absolute
# moveto), h / v / l / q (relative), Z. Deliberately not a general SVG parser.
_TOKEN = re.compile(r"[MmZzLlHhVvQq]|-?\d*\.?\d+(?:e-?\d+)?")
_QUAD_STEPS = 8   # flattening resolution; the outlines are all quadratics
_FLAT_EPS = 1.0   # font units: below this a quadratic is treated as a line
_DEDUPE_EPS = 0.25


def _push(pts: list, pt: tuple[float, float]) -> None:
    """Append unless it repeats the previous point."""
    if pts and abs(pts[-1][0] - pt[0]) < _DEDUPE_EPS \
            and abs(pts[-1][1] - pt[1]) < _DEDUPE_EPS:
        return
    pts.append(pt)


def _parse_path(d: str) -> list[list[tuple[float, float]]]:
    """Flatten a path's ``d`` into closed polygons, one per subpath."""
    toks = _TOKEN.findall(d)
    subpaths: list[list[tuple[float, float]]] = []
    cur: list[tuple[float, float]] = []
    x = y = 0.0
    i = 0
    cmd = ""
    while i < len(toks):
        t = toks[i]
        if t.isalpha():
            cmd = t
            i += 1
            if cmd in "Zz":
                if len(cur) > 2:
                    subpaths.append(cur)
                cur = []
                continue
        def n(k: int, _i: int = i) -> float:
            return float(toks[_i + k])

        if cmd == "M":
            if len(cur) > 2:
                subpaths.append(cur)
            x, y = n(0), n(1)
            cur = [(x, y)]
            i += 2
            cmd = "L"  # implicit repeats after M are linetos
        elif cmd == "m":
            if len(cur) > 2:
                subpaths.append(cur)
            x, y = x + n(0), y + n(1)
            cur = [(x, y)]
            i += 2
            cmd = "l"
        elif cmd in "Ll":
            x, y = (n(0), n(1)) if cmd == "L" else (x + n(0), y + n(1))
            _push(cur, (x, y))
            i += 2
        elif cmd in "Hh":
            x = n(0) if cmd == "H" else x + n(0)
            _push(cur, (x, y))
            i += 1
        elif cmd in "Vv":
            y = n(0) if cmd == "V" else y + n(0)
            _push(cur, (x, y))
            i += 1
        elif cmd in "Qq":
            if cmd == "Q":
                cx, cy, ex, ey = n(0), n(1), n(2), n(3)
            else:
                cx, cy, ex, ey = x + n(0), y + n(1), x + n(2), y + n(3)
            # The export emits a great many degenerate quadratics (control and
            # both ends coincident, or the control exactly on the chord).
            # Flattening those to _QUAD_STEPS identical points bloats the
            # shipped SVG by an order of magnitude for no visual gain.
            area = abs((cx - x) * (ey - y) - (cy - y) * (ex - x))
            if area < _FLAT_EPS:
                _push(cur, (ex, ey))
            else:
                for k in range(1, _QUAD_STEPS + 1):
                    u = k / _QUAD_STEPS
                    m = 1 - u
                    _push(cur, (m * m * x + 2 * m * u * cx + u * u * ex,
                                m * m * y + 2 * m * u * cy + u * u * ey))
            x, y = ex, ey
            i += 4
        else:  # unknown command: stop rather than silently mis-draw
            raise ValueError(f"unsupported path command {cmd!r}")
    if len(cur) > 2:
        subpaths.append(cur)
    return subpaths


def _matrix(spec: str) -> tuple[float, ...]:
    return tuple(float(v) for v in spec[spec.index("(") + 1: spec.index(")")].split(","))


def load_layers(name: str) -> tuple[list[tuple[str, list]], tuple[float, ...]]:
    """Read an outline SVG into (fill_kind, polygons) layers, in paint order.

    ``fill_kind`` is "black" or "gradient"; the exported gradient_0 is a
    black-to-black ramp, i.e. flat black.
    """
    svg = (BRAND / name).read_text()
    vb = tuple(
        float(v)
        for v in re.search(r'viewBox="([^"]+)"', svg).group(1).split()
    )
    layers = []
    for d, grad, tf in re.findall(
        r'<path d="([^"]+)"[^>]*fill="url\(#([^)]+)\)"[^>]*transform="([^"]+)"',
        svg,
    ):
        a, b, c, dd, e, f = _matrix(tf)
        polys = [
            [(a * px + c * py + e, b * px + dd * py + f) for px, py in sub]
            for sub in _parse_path(d)
        ]
        layers.append(("black" if grad == "gradient_0" else "gradient", polys))
    return layers, vb


def _bbox(layers: list[tuple[str, list]], kinds: set[str]) -> tuple[float, ...]:
    xs, ys = [], []
    for kind, polys in layers:
        if kind not in kinds:
            continue
        for p in polys:
            xs += [q[0] for q in p]
            ys += [q[1] for q in p]
    return min(xs), min(ys), max(xs), max(ys)


def _gradient_image(w: int, h: int, top_px: float, bot_px: float) -> Image.Image:
    """Vertical gradient ramping between two PIXEL rows.

    Bounds are pixels rather than a normalised square: building a square and
    resizing it to a wide box squashes the ramp, which silently flattened the
    wordmark to a single colour.
    """
    img = Image.new("RGB", (w, h))
    d = ImageDraw.Draw(img)
    for y in range(h):
        t = 1.0 - (y - top_px) / max(bot_px - top_px, 1e-6)  # y-down vs y-up
        t = min(max(t, 0.0), 1.0)
        for (p0, c0), (p1, c1) in itertools.pairwise(GRADIENT):
            if t <= p1:
                k = 0.0 if p1 == p0 else (t - p0) / (p1 - p0)
                c = tuple(round(c0[j] + (c1[j] - c0[j]) * k) for j in range(3))
                break
        else:
            c = GRADIENT[-1][1]
        d.line([(0, y), (w, y)], fill=c)
    return img


def glyph_bbox_user(layers: list[tuple[str, list]], probe: int = 900
                    ) -> tuple[float, float, float, float]:
    """Bounds of the visible LETTERFORM in user space.

    The letter is the gradient field minus the carve path, which has no closed
    form, so it is measured by rasterising a probe and mapping the result back.
    """
    x0, y0, x1, y1 = _bbox(layers, {"gradient"})
    s = probe / max(x1 - x0, y1 - y0)
    field = _fill((probe, probe), next(p for k, p in layers if k == "gradient"),
                  s, s, x0, y0)
    carve = _fill((probe, probe), [p for k, p in layers if k == "black"][-1],
                  s, s, x0, y0)
    bb = ImageChops.subtract(field, carve).getbbox()
    return (x0 + bb[0] / s, y0 + bb[1] / s, x0 + bb[2] / s, y0 + bb[3] / s)


def _fill(size: tuple[int, int], polys: list, sx: float, sy: float,
          ox: float, oy: float) -> Image.Image:
    """Mask of `polys` mapped into `size` by scale/offset."""
    m = Image.new("L", size, 0)
    d = ImageDraw.Draw(m)
    for p in polys:
        d.polygon([((q[0] - ox) * sx, (q[1] - oy) * sy) for q in p], fill=255)
    return m


def render_outline_art(name: str, w: int, h: int, pad: float = 0.0,
                       variant: str = "full", bg: tuple | None = None,
                       ) -> Image.Image:
    """Composite an outline SVG's layers into a `w` x `h` RGBA image.

    Variants exist because the extrusion and the letterform's narrow slots do
    not survive a 16px tile; see the candidate comparison in the commit message.
      full         extrusion, gradient field, carve  (the true rendering)
      no_extrusion gradient field and carve, no 3D drop
      glyph_only   just the letterform, filled with the gradient
    """
    layers, _ = load_layers(name)
    # Fit on the ink actually drawn: including the extrusion when it is painted
    # keeps the 3D drop inside the tile instead of letting it run off the edge.
    kinds = {"black", "gradient"} if variant == "full" else {"gradient"}
    x0, y0, x1, y1 = _bbox(layers, kinds)
    aw, ah = w * (1 - pad * 2), h * (1 - pad * 2)
    s = min(aw / (x1 - x0), ah / (y1 - y0))
    ox = x0 - (w - (x1 - x0) * s) / 2 / s
    oy = y0 - (h - (y1 - y0) * s) / 2 / s

    img = Image.new("RGBA", (w, h), (*bg, 255) if bg else (0, 0, 0, 0))
    gb = _bbox(layers, {"gradient"})
    grad = _gradient_image(w, h, (gb[1] - oy) * s, (gb[3] - oy) * s)

    if variant == "glyph_only":
        # The letterform is the gradient field MINUS the black path that carves
        # it. Painting the field alone just yields a filled rectangle.
        field = _fill((w, h), next(p for k, p in layers if k == "gradient"),
                      s, s, ox, oy)
        carve = _fill((w, h), [p for k, p in layers if k == "black"][-1],
                      s, s, ox, oy)
        img.paste(grad, (0, 0), ImageChops.subtract(field, carve))
        return img

    for idx, (kind, polys) in enumerate(layers):
        if variant == "no_extrusion" and idx == 0:
            continue  # layer 0 is the 3D extrusion
        img.paste(grad if kind == "gradient" else BLACK, (0, 0),
                  _fill((w, h), polys, s, s, ox, oy))
    return img


def _rounded(img: Image.Image, radius_frac: float = 0.18) -> Image.Image:
    m = Image.new("L", img.size, 0)
    ImageDraw.Draw(m).rounded_rectangle(
        [0, 0, img.width - 1, img.height - 1],
        radius=radius_frac * img.width, fill=255)
    out = Image.new("RGBA", img.size, (0, 0, 0, 0))
    out.paste(img, (0, 0), m)
    return out


SS = 8  # supersample: an outline fill cannot be pixel-snapped


def render_mark(size: int, variant: str = "glyph_only",
                tile: tuple = INK, pad: float = 0.15) -> Image.Image:
    """The icon mark: the Honk T on a tile.

    The art is rendered transparent, cropped to its own ink, then centred, so
    the letterform fills the tile regardless of variant. Fitting to the source
    artboard instead would leave the glyph swimming in the rectangle's margins.
    """
    px = size * SS
    art = render_outline_art("honk-T.svg", px, px, variant=variant)
    art = art.crop(art.getbbox())
    inner = round(px * (1 - pad * 2))
    s = min(inner / art.width, inner / art.height)
    art = art.resize((max(1, round(art.width * s)),
                      max(1, round(art.height * s))), Image.LANCZOS)

    canvas = Image.new("RGBA", (px, px), (*tile, 255))
    canvas.paste(art, ((px - art.width) // 2, (px - art.height) // 2), art)
    out = canvas.resize((size, size), Image.LANCZOS)
    return _rounded(out) if size >= 32 else out


# --- Social card -------------------------------------------------------------
OG_W, OG_H = 1200, 630
_RULE_K = 3  # the CSS rule is authored at 18px/6px for a browser topbar
# The tagline is the only text not drawn from outlines, so it needs a real
# font file. Candidates cover macOS and common Linux/CI images; Pillow's
# bitmap default is the last resort so a regeneration never hard-fails on a
# machine that has none of them.
_TAG_FONTS = [
    "/System/Library/Fonts/Supplemental/Arial Bold.ttf",
    "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
    "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf",
    "/usr/share/fonts/TTF/DejaVuSans-Bold.ttf",
]


def _tag_font(size: int) -> ImageFont.FreeTypeFont | ImageFont.ImageFont:
    for path in _TAG_FONTS:
        if pathlib.Path(path).exists():
            return ImageFont.truetype(path, size)
    return ImageFont.load_default()


def render_og(taglines: list[str]) -> Image.Image:
    """1200x630 link-unfurl card, using the real Honk wordmark.

    A social card is not a big favicon: it needs the wordmark and a line saying
    what the thing is, because most impressions are a thumbnail next to a URL.
    """
    img = Image.new("RGB", (OG_W, OG_H), PAPER)
    d = ImageDraw.Draw(img)

    # The site's .topbar::after stripe: gold, cream-pop, orange, brick, cycling
    # at 18px. Scaled 3x here because the card is a poster, not a 1:1 screenshot
    # of the header. Deliberately NOT the Honk gradient's amber and pink: those
    # belong to the glyph, and borrowing them invents a palette the site never
    # uses.
    band_h = 6 * _RULE_K
    seg = 18 * _RULE_K
    cycle = [GOLD, CREAM_POP, ORANGE, BRICK]
    for i, x in enumerate(range(0, OG_W, seg)):
        d.rectangle([x, 0, x + seg, band_h], fill=cycle[i % 4])
        d.rectangle([x, OG_H - band_h, x + seg, OG_H], fill=cycle[i % 4])
    d.rectangle([36, band_h + 22, OG_W - 36, OG_H - band_h - 22],
                outline=RULE, width=3)

    # Real Honk wordmark. Its own aspect ratio drives the height so it is never
    # squashed, and the width is what keeps it inside the keyline.
    layers, _ = load_layers("wordmark.svg")
    _, wy0, _, wy1 = _bbox(layers, {"black", "gradient"})
    wx0, _, wx1, _ = _bbox(layers, {"black", "gradient"})
    word_w = 726
    word_h = round(word_w * (wy1 - wy0) / (wx1 - wx0))
    word = render_outline_art("wordmark.svg", word_w * 2, word_h * 2,
                              variant="full").resize(
        (word_w, word_h), Image.LANCZOS)

    # The header sets .brand with `text-shadow: 2px 2px 0 --ink, 4px 4px 0
    # --gold` on top of the font's own extrusion. Reproduced here in the same
    # order (gold furthest) and scaled to this wordmark's size, because without
    # it the card reads flatter than the site it links to.
    sil = word.getchannel("A").point(lambda v: 255 if v > 40 else 0)
    ink_off, gold_off = round(word_h * 0.05), round(word_h * 0.10)

    tag_font = _tag_font(30)
    tag_h = 42 * len(taglines)
    block_h = word_h + gold_off + 30 + tag_h
    wx = 372
    wy = (OG_H - block_h) // 2

    mark = render_mark(236)
    img.paste(mark, (104, (OG_H - mark.height) // 2), mark)

    img.paste(GOLD, (wx + gold_off, wy + gold_off), sil)
    img.paste(INK, (wx + ink_off, wy + ink_off), sil)
    img.paste(word, (wx, wy), word)

    for i, line in enumerate(taglines):
        d.text((wx + 10, wy + word_h + gold_off + 30 + i * 42), line,
               font=tag_font, fill=INK)
    return img


def main() -> None:
    STATIC.mkdir(parents=True, exist_ok=True)

    # Scalable mark: the real outlines on the ink tile, no font reference.
    (STATIC / "logo-tweezer.svg").write_text(build_svg())

    render_mark(192).save(STATIC / "logo-tweezer-192.png")
    render_mark(512).save(STATIC / "logo-tweezer-512.png")
    render_mark(180).save(STATIC / "apple-touch-icon-tweezer.png")

    # Each frame rendered NATIVELY at its own size. Handing Pillow one large
    # image plus `sizes=` makes it downsample internally, which visibly softens
    # the 16px frame. The largest must be the base image: Pillow drops any
    # requested size bigger than the image it is called on.
    frames = [render_mark(s) for s in (48, 32, 16)]
    frames[0].save(STATIC / "favicon-tweezer.ico", format="ICO",
                   sizes=[(16, 16), (32, 32), (48, 48)],
                   append_images=frames[1:])

    render_og(
        ["Phish setlist prediction game.", "Pick 5 before the lights go down."]
    ).save(STATIC / "og-tweezer.png")

    for name in ["logo-tweezer.svg", "logo-tweezer-192.png",
                 "logo-tweezer-512.png", "apple-touch-icon-tweezer.png",
                 "favicon-tweezer.ico", "og-tweezer.png"]:
        print(f"{name:34} {(STATIC / name).stat().st_size:>8} bytes")


def build_svg() -> str:
    """Scalable favicon: the glyph outlines on the ink tile.

    Matches the raster variant (letterform only, no extrusion) so the SVG and
    the .ico read as the same mark.
    """
    layers, _ = load_layers("honk-T.svg")
    gx0, gy0, gx1, gy1 = glyph_bbox_user(layers)
    # Fit the LETTERFORM, not the source artboard. The gradient field is a
    # plain rectangle much larger than the glyph, so fitting to it leaves the
    # T swimming in margin.
    inner = 70.0
    s = inner / max(gx1 - gx0, gy1 - gy0)
    ox = 50 - (gx0 + gx1) / 2 * s
    oy = 50 - (gy0 + gy1) / 2 * s

    def emit(polys: list) -> str:
        out = []
        for p in polys:
            pts = " ".join(f"{q[0] * s + ox:.2f},{q[1] * s + oy:.2f}" for q in p)
            out.append(f'<polygon points="{pts}"/>')
        return "".join(out)

    grad_layer = next(p for k, p in layers if k == "gradient")
    carve = [p for k, p in layers if k == "black"][-1]
    # The ramp spans the gradient FIELD, exactly as the font defines it, so the
    # letter samples the same sub-range it does in the real rendering. Stops are
    # emitted top-to-bottom with ascending offsets, since GRADIENT is stored
    # bottom-to-top in font space.
    fy0, fy1 = _bbox(layers, {"gradient"})[1], _bbox(layers, {"gradient"})[3]
    ink = f"#{INK[0]:02X}{INK[1]:02X}{INK[2]:02X}"
    stops = "".join(
        f'\n      <stop offset="{(1 - p) * 100:g}%"'
        f' stop-color="#{c[0]:02X}{c[1]:02X}{c[2]:02X}"/>'
        for p, c in reversed(GRADIENT)
    )
    return (
        '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 100 100"'
        ' width="100" height="100" role="img" aria-label="Tweezer Picks">\n'
        "  <defs>\n"
        '    <linearGradient id="g" gradientUnits="userSpaceOnUse"'
        f' x1="0" y1="{fy0 * s + oy:.2f}" x2="0" y2="{fy1 * s + oy:.2f}">{stops}\n'
        "    </linearGradient>\n"
        '    <clipPath id="c">\n      ' + emit(grad_layer) + "\n    </clipPath>\n"
        "  </defs>\n"
        f'  <rect x="0" y="0" width="100" height="100" rx="18" fill="{ink}"/>\n'
        '  <g clip-path="url(#c)">\n'
        '    <rect x="-20" y="-20" width="140" height="140" fill="url(#g)"/>\n'
        f'    <g fill="{ink}">' + emit(carve) + "</g>\n"
        "  </g>\n</svg>\n"
    )


if __name__ == "__main__":
    main()
