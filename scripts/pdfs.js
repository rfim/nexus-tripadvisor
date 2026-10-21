// Render the three downloads to PDF with Chromium (Playwright): node scripts/pdfs.js
// The deck is A4 landscape, one slide per page; the one-pager and companion note are one tall page each.
const path = require("path");
const { chromium } = require(process.env.PLAYWRIGHT || "playwright");
const src = path.join(__dirname, "..", "docs", "pdf-src"), out = path.join(__dirname, "..", "assets");
(async () => {
  const b = await chromium.launch();
  for (const [html, pdf, tall] of [["deck.html", "Nexus_Tripadvisor_VIM.pdf", false], ["one_page.html", "VIM_Tripadvisor_one_page.pdf", true],
                                   ["companion.html", "VIM_Tripadvisor_companion_note.pdf", true]]) {
    const p = await b.newPage();
    await p.goto("file://" + path.join(src, html), { waitUntil: "networkidle" });
    if (tall) {
      await p.emulateMedia({ media: "print" });
      const h = await p.evaluate(() => Math.ceil(document.documentElement.scrollHeight / 96 * 25.4) + 40);
      await p.pdf({ path: path.join(out, pdf), width: "238mm", height: `${h}mm`, printBackground: true, pageRanges: "1" });
    } else await p.pdf({ path: path.join(out, pdf), preferCSSPageSize: true, printBackground: true });
    console.log(pdf);
  }
  await b.close();
})();
