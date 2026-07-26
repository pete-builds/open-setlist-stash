// Cloudflare Worker: inject OG / Twitter Card meta tags into every HTML
// response from tweezerpicks.com so social previews display the site image.
// Once the nix1 container is updated (it serves these tags natively),
// this Worker can be disabled.

const OG_IMAGE =
  "https://raw.githubusercontent.com/pete-builds/open-setlist-stash/main/src/setlist_stash/static/og-image.png";
const SITE_NAME = "Tweezer Picks";
const DESCRIPTION =
  "Phish setlist picks game — make your calls before the lights go down.";

export default {
  async fetch(request) {
    const response = await fetch(request);
    const ct = response.headers.get("content-type") || "";
    if (!ct.includes("text/html")) return response;
    const url = new URL(request.url);
    const canonicalUrl = `${url.origin}${url.pathname}`;
    return new HTMLRewriter()
      .on("head", {
        element(el) {
          el.append(
            `\n  <meta property="og:type" content="website">` +
            `\n  <meta property="og:site_name" content="${SITE_NAME}">` +
            `\n  <meta property="og:title" content="${SITE_NAME}">` +
            `\n  <meta property="og:description" content="${DESCRIPTION}">` +
            `\n  <meta property="og:image" content="${OG_IMAGE}">` +
            `\n  <meta property="og:image:width" content="1200">` +
            `\n  <meta property="og:image:height" content="630">` +
            `\n  <meta property="og:url" content="${canonicalUrl}">` +
            `\n  <meta name="twitter:card" content="summary_large_image">` +
            `\n  <meta name="twitter:title" content="${SITE_NAME}">` +
            `\n  <meta name="twitter:description" content="${DESCRIPTION}">` +
            `\n  <meta name="twitter:image" content="${OG_IMAGE}">`,
            { html: true }
          );
        },
      })
      .transform(response);
  },
};
