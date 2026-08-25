# Brand

## Files

| File | Use |
|---|---|
| `plexus-wordmark.svg` | Wordmark, `currentColor` — inherits colour from context |
| `plexus-mark.svg` | Square mark, `currentColor` — favicon, avatar, app icon |
| `*-light.svg` | Explicit dark ink, for light backgrounds |
| `*-dark.svg` | Explicit light ink, for dark backgrounds |

The `currentColor` versions are correct almost everywhere. The explicit-colour
pairs exist because GitHub markdown cannot resolve `currentColor` — an SVG
using it renders invisible on one of the two themes.

## The idea

Two details carry the concept, and neither is decoration:

**The E has no stem.** Three bars that never connect — the protocol's central
claim, that agents reach each other without touching, written into a letterform.

**The X is two crossing paths**, and in the square mark those paths are broken
where they intersect. They cross; they do not join. That is the protocol in one
glyph, and it is why the mark is a *broken* X rather than an X.

## Rules

- **Never fill the gaps.** The break in the mark is the entire point.
- Single colour only. No gradients, glows or shadows.
- Test at 16px in solid black before approving any change; the gap must survive.
- Clear space around the lockup: at least the height of the mark.
- Accent `#0E7C86` on light, `#2FB3BE` on dark. Ink `#0F1E24` / `#E4EFF1`.
