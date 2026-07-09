#!/usr/bin/env python3
"""Compose the 4 AWS console screenshots into one labeled evidence image."""
import os
from PIL import Image, ImageDraw, ImageFont

BASE = os.path.join(os.path.dirname(__file__), "..", "docs", "artifact", "screenshots")
OUT = os.path.join(BASE, "aws-evidence-combined.png")

CELLS = [
    ("02-s3-lakehouse.png",     "S3 lakehouse", "Medallion prefixes: bronze / silver / gold / landing"),
    ("03-glue-catalog.png",     "Glue Data Catalog", "convergence DB — 3 cataloged tables"),
    ("04-apprunner-service.png","App Runner", "Public HTTPS dashboard — no login required"),
    ("05-athena-catalog.png",   "Athena", "Presto query engine over the catalog + HLL"),
]

# palette (matches the one-pager dark theme)
BG      = (14, 21, 38)      # --ink
CARD    = (22, 31, 53)      # --surface
LINE    = (42, 55, 87)      # --line
TEXT    = (232, 237, 247)   # --text
MUTED   = (147, 160, 184)   # --muted
ACCENT  = (94, 224, 210)    # --accent
FAINT   = (107, 120, 150)

W = 2400
PAD = 48
GAP = 40
TITLE_H = 190
CAP_H = 96
FOOT_H = 96

CW = (W - 2 * PAD - GAP) // 2           # cell width
IMG_H = 760                              # image box height
ROW_H = CAP_H + IMG_H
H = TITLE_H + 2 * ROW_H + GAP + FOOT_H + PAD

def font(sz, bold=False):
    paths = [
        "/System/Library/Fonts/Supplemental/Arial Bold.ttf" if bold else "/System/Library/Fonts/Supplemental/Arial.ttf",
        "/System/Library/Fonts/Helvetica.ttc",
        "/Library/Fonts/Arial.ttf",
    ]
    for p in paths:
        try:
            return ImageFont.truetype(p, sz)
        except Exception:
            continue
    return ImageFont.load_default()

f_title = font(64, bold=True)
f_sub   = font(30)
f_cap   = font(38, bold=True)
f_capd  = font(27)
f_foot  = font(26)
f_mono  = font(26)

canvas = Image.new("RGB", (W, H), BG)
d = ImageDraw.Draw(canvas)

# header
d.text((PAD, 54), "Convergence Reach — running on AWS", font=f_title, fill=TEXT)
d.text((PAD, 132), "Live us-east-1 deployment · account 194611079924 · captured from the AWS console",
       font=f_sub, fill=MUTED)
# accent rule
d.rectangle([PAD, TITLE_H - 24, W - PAD, TITLE_H - 21], fill=ACCENT)

def fit(im, bw, bh):
    r = min(bw / im.width, bh / im.height)
    return im.resize((max(1, int(im.width * r)), max(1, int(im.height * r))), Image.LANCZOS)

for i, (fn, cap, capd) in enumerate(CELLS):
    col, row = i % 2, i // 2
    x = PAD + col * (CW + GAP)
    y = TITLE_H + row * (ROW_H + (GAP if row else 0))
    # card
    d.rounded_rectangle([x, y, x + CW, y + ROW_H], radius=18, fill=CARD, outline=LINE, width=2)
    # caption
    d.text((x + 32, y + 20), cap, font=f_cap, fill=ACCENT)
    d.text((x + 32, y + 62), capd, font=f_capd, fill=MUTED)
    # image, centered in the image box
    im = Image.open(os.path.join(BASE, fn)).convert("RGB")
    im = fit(im, CW - 40, IMG_H - 28)
    ix = x + (CW - im.width) // 2
    iy = y + CAP_H + (IMG_H - CAP_H // 4 - im.height) // 2
    if iy < y + CAP_H:
        iy = y + CAP_H
    # thin frame behind the screenshot
    d.rectangle([ix - 2, iy - 2, ix + im.width + 2, iy + im.height + 2], outline=LINE, width=2)
    canvas.paste(im, (ix, iy))

# footer
fy = H - FOOT_H - 8
d.rectangle([PAD, fy - 8, W - PAD, fy - 6], fill=LINE)
d.text((PAD, fy + 8),
       "EMR Serverless (Spark ETL) · Athena/HyperLogLog · MWAA · Bedrock · Terraform",
       font=f_foot, fill=MUTED)
d.text((PAD, fy + 46), "Live dashboard: miatibcmck.us-east-1.awsapprunner.com", font=f_mono, fill=ACCENT)

canvas.save(OUT)
print("wrote", OUT, canvas.size)
