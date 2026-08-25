# The website

Deployed from its own repository: **[MoGhali/plexus-site](https://github.com/MoGhali/plexus-site)**,
where the page sits at the root — which is what makes GitHub Pages and a custom domain work
without the `/docs` contortion this repository would need. This folder is the source of truth;
copy `index.html`, `assets/` and `CNAME` across when the page changes.

The only assets are the console tour and its poster frame — the still screenshots came out once
the video went in, because showing the same six screens twice is just a longer page.

One file, plus screenshots. `site/index.html` is entirely self-contained — the logo is inline SVG, the terminal
recordings are inline data URIs, the CSS and the tab script are in the page. There is no build
step, no bundler and no external request at runtime, so it renders identically from `file://`, a
CDN, or a bucket.

```bash
open site/index.html          # that's the whole preview process
```

## Pages

| | |
|---|---|
| `index.html` | The argument: problem, use cases, console tour, comparison, install |
| `protocol.html` | The specification — topics, payloads, guarantees, conformance |
| `assets/plexus.css` | Shared tokens and components. **Both pages use it** |

The stylesheet is shared rather than copied into each page, which is why the header nav rules are
scoped as `header nav` — a bare `nav` selector also claimed the documentation contents rail and
laid it out as a flex row across the article.

## The console tour

`assets/console.mp4` is generated, not filmed:

```bash
npm run site:video            # node tools/panel-video.mjs — needs Chrome and ffmpeg
```

It drives the **shipped** panel — the same `dist/web/index.html` the plugin serves — against the
redacted fixtures in `test/fixtures`, captures six views at 2× density, cross-fades them, and
burns the wordmark into the corner. So the tour cannot show a screen that no longer exists, and
cannot leak a live deployment's topics, job ids or repository names.

Regenerate it whenever the panel's appearance changes, along with the screenshots.

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

**GitHub Pages** — enable it on `plexus-site`, serving from the root of `main`. Note that Pages
on a private repository needs a paid plan; Cloudflare Pages and Netlify both serve private repos
on their free tiers.

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
