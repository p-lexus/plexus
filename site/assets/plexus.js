// The theme switch, shared by both pages.
//
// The page follows the reader's system setting until this is touched. Once it
// is, the choice is stored and applies on every visit. There is deliberately no
// third "system" state: the same button takes the choice back, and a tri-state
// cycle is a mode to explain to people who only wanted the lights off.
(function () {
  var root = document.documentElement;
  var btn = document.getElementById('theme');
  if (!btn) return;

  // What is on screen right now, not what the page started as — so the first
  // click always flips what the reader is actually looking at.
  function showing() {
    var set = root.getAttribute('data-theme');
    if (set === 'dark' || set === 'light') return set;
    return window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches
      ? 'dark' : 'light';
  }

  function label() {
    var next = showing() === 'dark' ? 'light' : 'dark';
    btn.setAttribute('aria-label', 'Switch to the ' + next + ' theme');
    btn.title = next.charAt(0).toUpperCase() + next.slice(1);
  }

  btn.addEventListener('click', function () {
    var next = showing() === 'dark' ? 'light' : 'dark';
    root.setAttribute('data-theme', next);
    root.style.colorScheme = next;
    try { localStorage.setItem('plexus-theme', next); } catch (e) {}
    label();
  });

  label();
})();
