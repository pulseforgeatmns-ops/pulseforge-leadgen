"""Build the Pulseforge LinkedIn personal banner with Pillow.

The output is 1584 x 396. A separate transparent text layer is checked
against LinkedIn's 568 x 264 bottom-left profile-photo safe zone.
"""

from pathlib import Path

from PIL import Image, ImageDraw, ImageFont


DARK = "#242424"
CREAM = "#faf7f2"
MUTED = "#c2bdb5"
QUIET = "#918b82"
EMBER = "#ff7a1a"

BW = 1584
BH = 396
SAFE_ZONE_WIDTH = 568
SAFE_ZONE_HEIGHT = 264
SAFE_ZONE_TOP = BH - SAFE_ZONE_HEIGHT
TEXT_LEFT = 620
TEXT_RIGHT = 1536

ROOT = Path(__file__).resolve().parent
FONT_DIR = ROOT / "fonts"
OUTPUT_DIR = ROOT / "outputs"

BOSKA_REGULAR = FONT_DIR / "Boska-Regular.ttf"
SWITZER_REGULAR = FONT_DIR / "Switzer-Regular.ttf"
SWITZER_MEDIUM = FONT_DIR / "Switzer-Medium.ttf"

WORDMARK_COPY = "pulseforge"
HERO_COPY = "Operator-run, not bot-run."
SUPPORT_COPY = (
    "I orchestrate a floor of AI agents so the volume scales and "
    "the judgment stays human."
)
SUPPORT_LINES = (
    "I orchestrate a floor of AI agents so the volume scales and",
    "the judgment stays human.",
)
URL_COPY = "gopulseforge.com"


def load_fonts():
    """Load the exact brand faces and fail clearly if an asset is missing."""
    font_paths = (BOSKA_REGULAR, SWITZER_REGULAR, SWITZER_MEDIUM)
    for font_path in font_paths:
        if not font_path.exists():
            raise FileNotFoundError(f"Required font not found: {font_path}")

    return {
        "wordmark": ImageFont.truetype(str(BOSKA_REGULAR), 28),
        "hero": ImageFont.truetype(str(BOSKA_REGULAR), 74),
        "support": ImageFont.truetype(str(SWITZER_REGULAR), 24),
        "url": ImageFont.truetype(str(SWITZER_MEDIUM), 20),
    }


def text_box(draw, xy, text, font):
    """Return the exact Pillow text bounds for a top-left anchor."""
    return draw.textbbox(xy, text, font=font, anchor="lt")


def assert_text_box(name, box):
    """Fail if text leaves the approved block or touches mobile crop edges."""
    left, top, right, bottom = box
    if left < TEXT_LEFT or right > TEXT_RIGHT:
        raise ValueError(
            f"{name} leaves horizontal text block: {box}, "
            f"allowed x={TEXT_LEFT}..{TEXT_RIGHT}"
        )
    if top < 32 or bottom > BH - 32:
        raise ValueError(
            f"{name} enters the 8 percent mobile crop margin: {box}"
        )


def draw_waveform(draw):
    """Draw quiet amber atmosphere only on the far-left side."""
    primary = [
        (0, 209),
        (72, 209),
        (112, 188),
        (152, 209),
        (338, 209),
        (354, 169),
        (370, 249),
        (386, 209),
        (498, 209),
    ]
    secondary = [
        (0, 67),
        (178, 67),
        (193, 48),
        (208, 86),
        (223, 67),
        (462, 67),
    ]
    draw.line(primary, fill=(255, 122, 26, 56), width=2, joint="curve")
    draw.line(secondary, fill=(255, 122, 26, 31), width=1, joint="curve")


def build_banner():
    """Compose the final banner and return it with its isolated text layer."""
    fonts = load_fonts()
    banner = Image.new("RGBA", (BW, BH), DARK)
    atmosphere = Image.new("RGBA", (BW, BH), (0, 0, 0, 0))
    draw_waveform(ImageDraw.Draw(atmosphere))
    banner = Image.alpha_composite(banner, atmosphere)

    text_layer = Image.new("RGBA", (BW, BH), (0, 0, 0, 0))
    text_draw = ImageDraw.Draw(text_layer)

    placements = (
        ("wordmark", (TEXT_LEFT, 38), WORDMARK_COPY, fonts["wordmark"], CREAM),
        ("hero", (TEXT_LEFT, 102), HERO_COPY, fonts["hero"], CREAM),
        (
            "support line 1",
            (TEXT_LEFT, 221),
            SUPPORT_LINES[0],
            fonts["support"],
            MUTED,
        ),
        (
            "support line 2",
            (TEXT_LEFT, 253),
            SUPPORT_LINES[1],
            fonts["support"],
            MUTED,
        ),
    )

    bounds = {}
    for name, xy, text, font, color in placements:
        box = text_box(text_draw, xy, text, font)
        assert_text_box(name, box)
        bounds[name] = box
        text_draw.text(xy, text, font=font, fill=color, anchor="lt")

    url_width = text_draw.textlength(URL_COPY, font=fonts["url"])
    url_xy = (round(TEXT_RIGHT - url_width), 328)
    url_box = text_box(text_draw, url_xy, URL_COPY, fonts["url"])
    assert_text_box("URL", url_box)
    bounds["URL"] = url_box
    text_draw.text(url_xy, URL_COPY, font=fonts["url"], fill=QUIET, anchor="lt")

    banner = Image.alpha_composite(banner, text_layer)
    banner_draw = ImageDraw.Draw(banner)
    banner_draw.rounded_rectangle(
        (TEXT_LEFT, 79, TEXT_LEFT + 72, 82),
        radius=2,
        fill=EMBER,
    )
    return banner, text_layer, bounds


def assert_safe_zone_clear(text_layer):
    """Require zero text pixels inside the bottom-left profile-photo box."""
    safe_zone_alpha = text_layer.getchannel("A").crop(
        (0, SAFE_ZONE_TOP, SAFE_ZONE_WIDTH, BH)
    )
    occupied = safe_zone_alpha.getbbox()
    if occupied is not None:
        raise ValueError(f"Text entered profile-photo safe zone: {occupied}")


def build_safe_zone_review(banner):
    """Create a review-only PNG with the no-text zone overlaid."""
    review = banner.copy()
    overlay = Image.new("RGBA", (BW, BH), (0, 0, 0, 0))
    draw = ImageDraw.Draw(overlay)
    draw.rectangle(
        (0, SAFE_ZONE_TOP, SAFE_ZONE_WIDTH - 1, BH - 1),
        fill=(255, 49, 88, 28),
        outline=(255, 49, 88, 220),
        width=2,
    )
    label_font = ImageFont.truetype(str(SWITZER_MEDIUM), 16)
    draw.text(
        (18, SAFE_ZONE_TOP + 16),
        "PROFILE PHOTO SAFE ZONE",
        font=label_font,
        fill=(255, 157, 175, 255),
        anchor="lt",
    )
    return Image.alpha_composite(review, overlay)


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    banner, text_layer, bounds = build_banner()
    assert_safe_zone_clear(text_layer)

    png_path = OUTPUT_DIR / "pulseforge-linkedin-banner.png"
    review_path = OUTPUT_DIR / "pulseforge-linkedin-banner-safe-zone-review.png"
    banner.convert("RGB").save(png_path, format="PNG", optimize=True)
    build_safe_zone_review(banner).convert("RGB").save(
        review_path, format="PNG", optimize=True
    )

    print(f"Built {png_path} ({BW} x {BH})")
    print(
        f"Safe zone clear: x=0..{SAFE_ZONE_WIDTH}, "
        f"y={SAFE_ZONE_TOP}..{BH}"
    )
    for name, box in bounds.items():
        print(f"{name}: {box}")
    print(f"Exact hero copy: {HERO_COPY}")
    print(f"Exact support copy: {SUPPORT_COPY}")
    print(f"Exact URL copy: {URL_COPY}")


if __name__ == "__main__":
    main()
