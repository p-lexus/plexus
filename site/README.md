# The website

One file. `site/index.html` is entirely self-contained — the logo is inline SVG, the terminal
recordings are inline data URIs, the CSS and the tab script are in the page. There is no build
step, no bundler and no external request at runtime, so it renders identically from `file://`, a
CDN, or a bucket.

```bash
open site/index.html          # that's the whole preview process
```

## Keeping it honest

The two terminal recordings come from [`tools/record-demo.mjs`](../tools/record-demo.mjs), which
runs the examples for real and times each line as it arrives. When an example changes:

```bash
node tools/record-demo.mjs examples/with-plugins.mjs docs/demo.svg
node tools/record-demo.mjs examples/demo.mjs docs/demo-delegation.svg
```

then re-inline them (they are base64 `data:` URIs in `index.html`). The point of recording rather
than drawing is that the site cannot claim something the code no longer does.

## Deploying

Any static host. Three that need no configuration:

**Cloudflare Pages / Netlify** — point at this repository, set the publish directory to `site`,
build command empty.

**Vercel** — `vercel --cwd site` , or set the root directory to `site` in the dashboard.

**GitHub Pages** — Pages only serves from the repository root or `/docs`, neither of which is
this folder, so either copy the file into `/docs` on release or use one of the above. This is
the only awkward option, and only because of that constraint.

### The domain

`CNAME` holds the custom domain for hosts that read it. Point the domain's DNS at your host —
usually a `CNAME` record for `www` and either an `ALIAS`/`ANAME` at the apex or your host's
documented apex records.

## Design notes

The identity has one idea in it: **paths that cross without joining.** The `E` in the wordmark has
no stem; the `X` in the mark is broken where the strokes meet. Agents reach each other without
touching, and the mark says so before the copy does.

That is why the small broken-X appears before every section label rather than a number or a
bullet — it is the concept used as punctuation. Do not close those gaps.

Type is monospace for everything structural — headings, labels, buttons, code — and a system sans
for prose. A wire format should look like one, and it avoids the interchangeable geometric sans
that most protocol sites reach for.

Both themes are defined as tokens on `:root`, with the dark set repeated under
`prefers-color-scheme` and `[data-theme="dark"]` so an explicit choice wins in either direction.
Nothing declares a colour outside those blocks; a value that only exists inside a media query is
how a page ends up rendering one theme's text on the other theme's background.
