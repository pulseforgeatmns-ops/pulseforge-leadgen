'use strict';
const demoSwitch=document.getElementById('demoRole');
// Keep the public switch readable despite the header's white ghost-button text.
demoSwitch.style.color='#102e49';
demoSwitch.style.background='#ffffff';
async function refreshDemoRole(){
  const response=await fetch('/api/anchor-portal/me');if(!response.ok)throw new Error('session');
  const {user}=await response.json();
  demoSwitch.dataset.next=user.role==='manager'?'client':'operator';
  demoSwitch.textContent=user.role==='manager'?'Demo client':'Demo operator';
}
demoSwitch.disabled=true;
refreshDemoRole().then(()=>{demoSwitch.disabled=false;}).catch(()=>{demoSwitch.textContent='Reload demo';demoSwitch.disabled=false;});
demoSwitch.addEventListener('click',async()=>{
  demoSwitch.disabled=true;
  try{
    if(!demoSwitch.dataset.next){location.reload();return;}
    const token=await fetch('/auth/csrf').then(r=>{if(!r.ok)throw new Error('session');return r.json();});
    const response=await fetch('/auth/demo-role',{method:'POST',headers:{'Content-Type':'application/json','X-CSRF-Token':token.csrf},body:JSON.stringify({role:demoSwitch.dataset.next})});
    if(!response.ok)throw new Error('switch');
    location.assign('/portal/anchor');
  }catch(_){demoSwitch.textContent='Try switching again';demoSwitch.disabled=false;}
});
