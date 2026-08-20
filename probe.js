const { chromium } = require('playwright');
const fs = require('fs');

(async () => {
  const browser = await chromium.launch({ headless: true, args: ['--no-sandbox'] });
  const ctx = await browser.newContext({
    userAgent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
    locale: 'en-US',
    viewport: { width: 1280, height: 800 },
  });
  const page = await ctx.newPage();
  await page.goto('https://justanime.to/', { waitUntil: 'domcontentloaded', timeout: 90000 });
  await page.waitForTimeout(5000);
  console.log('PAGE_URL:', page.url());
  console.log('PAGE_TITLE:', await page.title());
  const bodyLen = (await page.content()).length;
  console.log('BODY_LEN:', bodyLen);

  const API = 'https://core.justanime.to/api';
  async function probe(name, url) {
    try {
      const r = await ctx.request.get(url, { timeout: 30000 });
      const text = await r.text();
      fs.writeFileSync('probes/' + name, text);
      console.log(name, '->', r.status(), text.length);
      return JSON.parse(text);
    } catch (e) {
      console.log(name, 'ERROR', e.message);
      return null;
    }
  }

  const home = await probe('home.json', API + '/home');
  let id = '';
  if (home) {
    function walk(o) {
      if (Array.isArray(o)) for (const v of o) { const r = walk(v); if (r) return r; }
      else if (o && typeof o === 'object') {
        if (typeof o.id === 'number') return o.id;
        for (const v of Object.values(o)) { const r = walk(v); if (r) return r; }
      }
      return '';
    }
    id = walk(home);
    console.log('FIRST_ANIME_ID:', id);
  }
  if (id) {
    await probe('anime.json', API + '/anime/' + id);
    await probe('watch.json', API + '/watch/' + id + '/episode/1');
    await probe('avail.json', API + '/watch/' + id + '/episode/1/availability');
    await probe('anineko.json', API + '/watch/' + id + '/episode/1/anineko/neko');
    await probe('animegg.json', API + '/watch/' + id + '/episode/1/animegg');
    await probe('megaplay.json', API + '/watch/' + id + '/episode/1/megaplay');
  }
  await probe('genre.json', API + '/genre');
  await probe('search.json', API + '/search?query=one%20piece');

  await browser.close();
})().catch(e => { console.error('FATAL', e.message); process.exit(1); });
