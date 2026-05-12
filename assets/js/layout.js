
(() => {
  const body = document.body;
  if (!body) return;

  const page = body.dataset.page || 'index';
  const shell = body.dataset.shell || 'public';
  const host = document.getElementById('siteNav');
  if (!host) return;

  const pages = {
    index: {
  brandHref: 'index.html',
  links: [
    { href: '#what', label: 'What it is', active: body.dataset.active === 'what' },
    { href: '#principles', label: 'Principles', active: body.dataset.active === 'principles' },
    { href: '#edge', label: 'The Edge', active: body.dataset.active === 'edge' },
    { href: '#coinflip', label: 'Coin Flip', active: body.dataset.active === 'coinflip' },
    { href: '#performance', label: 'Performance', active: body.dataset.active === 'performance' },
    { href: '#plb', label: 'PLB', active: body.dataset.active === 'plb' },
    { href: '#transparency', label: 'Transparency', active: body.dataset.active === 'transparency' },
    { href: 'https://buy.stripe.com/00w14obYvf9B3fO88Q8g002', label: 'Subscribe', cta: true },
    { href: 'login.html', label: 'Login', cta: true }
  ]
},
    app: {
      brandHref: 'index.html',
      links: [
        { href: 'research.html', label: 'Research', active: true },
        { href: 'model.html', label: 'Model' },
        { button: true, id: 'logoutBtn', label: 'Logout', cta: true }
      ]
    },
    model: {
      brandHref: 'index.html',
      links: [
        { href: 'research.html', label: 'Research' },
        { href: 'model.html', label: 'Model', active: true },
        { button: true, id: 'logoutBtn', label: 'Logout', cta: true }
      ]
    },
    login: {
      brandHref: 'index.html',
      links: [
        { href: 'index.html', label: 'Home' },
        { href: 'login.html', label: 'Login', cta: true, active: true }
      ]
    }
  };

  const config = pages[page] || pages.index;

  const linksHtml = config.links.map(item => {
    const classes = [item.button ? 'site-nav-button' : 'site-nav-link'];
    if (item.cta) classes.push('is-cta');
    if (item.active) classes.push('is-active');
    if (item.button) {
      return `<button type="button" class="${classes.join(' ')}"${item.id ? ` id="${item.id}"` : ''}>${item.label}</button>`;
    }
    return `<a class="${classes.join(' ')}" href="${item.href}">${item.label}</a>`;
  }).join('');

  host.innerHTML = `
    <div class="site-nav">
      <div class="container site-nav-inner">
        <a class="site-brand" href="${config.brandHref}" aria-label="1XW Trading">
          <img src="logo.png" alt="1XW Trading logo" />
          <span class="site-logo">1XW TRADING</span>
        </a>
        <div class="site-nav-links">${linksHtml}</div>
      </div>
    </div>
  `;
document.addEventListener('click', async (e) => {
  const btn = e.target.closest('#logoutBtn');
  if (!btn) return;

  e.preventDefault();

  const { createClient } = await import('https://esm.sh/@supabase/supabase-js@2');

  const supabase = createClient(
    'https://iiawkbucjgellrtrizhk.supabase.co',
    'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImlpYXdrYnVjamdlbGxydHJpemhrIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzM0ODAzNjUsImV4cCI6MjA4OTA1NjM2NX0.DK5_m4XQUNGqlCu6UoIMJRSpfKPbMHW8B56ikK1wqPM'
  );

  await supabase.auth.signOut();
  window.location.href = 'login.html';
});

})();
