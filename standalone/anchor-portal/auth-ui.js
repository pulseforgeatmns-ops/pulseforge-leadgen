'use strict';
const form = document.getElementById('login');
const logout = document.getElementById('logout');
if (location.pathname === '/logout') { form.hidden=true; logout.hidden=false; }
async function submit(path, body) {
  const status = document.getElementById('status');
  try {
    const token = await fetch('/auth/csrf').then(r=>r.json());
    const res = await fetch(path,{method:'POST',headers:{'Content-Type':'application/json','X-CSRF-Token':token.csrf},body:JSON.stringify(body)});
    if (!res.ok) { status.textContent='Unable to continue. Check your credentials or try again later.'; return; }
    location.assign(path === '/login' ? '/portal/anchor' : '/login');
  } catch (_) { status.textContent='Connection interrupted. Please try again.'; }
}
form.addEventListener('submit', e=>{e.preventDefault();submit('/login',Object.fromEntries(new FormData(form)));});
logout.addEventListener('click',()=>submit('/logout',{}));
