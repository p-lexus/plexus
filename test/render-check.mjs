import fs from 'fs';
const src = fs.readFileSync(new URL('../dist/web/index.html', import.meta.url), 'utf8');
const js = src.match(/<script>([\s\S]*?)<\/script>/)[1];
// minimal DOM + browser stubs so the module body can run
const el = () => ({ textContent:'', innerHTML:'', className:'', value:'', hidden:false,
                    classList:{add(){},remove(){},contains(){return false}}, focus(){}, dataset:{} });
globalThis.document = { getElementById: el, querySelectorAll: () => [], querySelector: () => null,
                        addEventListener(){}, documentElement:{ getAttribute:()=>null, setAttribute(){} },
                        activeElement:{ tagName:'BODY' } };
globalThis.location = { pathname:'/', search:'', hash:'', origin:'http://127.0.0.1:8765' };
globalThis.history = { replaceState(){} };
globalThis.localStorage = { getItem:()=>null, setItem(){}, removeItem(){} };
globalThis.EventSource = class { constructor(){} addEventListener(){} close(){} };
globalThis.fetch = async () => ({ ok:true, status:200, json: async()=>({}) });
globalThis.matchMedia = () => ({ matches:false });
Object.defineProperty(globalThis, "navigator", { value:{ clipboard:{ writeText: async()=>{} } }, configurable:true });
globalThis.confirm = () => true;
globalThis.setInterval = () => 0; globalThis.clearInterval = () => {};
globalThis.setTimeout = () => 0; globalThis.clearTimeout = () => {};

const mod = new Function(js + `
  ;return { setState:(p,s,j)=>{ profile=p; status=s; jobs=j; },
            views:{ overview, jobsView, runView, services, sessionView, varsView } };`)();
const F = (n) => JSON.parse(fs.readFileSync(new URL(`fixtures/${n}.json`, import.meta.url),'utf8'));
mod.setState(F('profile'), F('status'), F('jobs'));
let bad = 0;

/* The stylesheet has to travel with the page.
   theme.css is the box console's own file and the panel LINKS it rather than
   inlining it, so a packaging step that copies index.html and forgets the
   stylesheet ships an unstyled panel that still passes every render test
   below. Assert the link and the shipped file together. */
const linked = /<link[^>]+href="theme\.css"/.test(src);
const shipped = fs.existsSync(new URL('../dist/web/theme.css', import.meta.url));
if (linked && shipped) console.log('\u2705 theme.css      linked and packaged');
else { console.log(`\u274c theme.css      ${linked ? '' : 'not linked by index.html '}${shipped ? '' : 'missing from dist/web'}`); bad++; }

for (const [name, fn] of Object.entries(mod.views)) {
  try { const html = fn(); console.log(`\u2705 ${name.padEnd(12)} ${String(html.length).padStart(6)} chars`); }
  catch (e) { console.log(`\u274c ${name.padEnd(12)} ${e.constructor.name}: ${e.message}`); bad++; }
}
process.exit(bad ? 1 : 0);
