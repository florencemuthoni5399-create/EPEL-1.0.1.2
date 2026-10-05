'use strict';

require('dotenv').config();
const http = require('http');
const fs = require('fs');
const path = require('path');
const WebSocket = require('ws');

const CONFIG = {
  appId: process.env.DERIV_APP_ID || '',
  apiToken: process.env.DERIV_API_TOKEN || '',
  accountType: (process.env.DERIV_ACCOUNT_TYPE || 'demo').toLowerCase(),
  asset: process.env.ASSET || 'R_75',
  horizons: parseList(process.env.HORIZONS_TICKS || '3,5,7,10,15'),
  lookback: positiveInt(process.env.LOOKBACK_TICKS, 20),
  moveTicks: positiveInt(process.env.MOVE_TICKS, 20),
  minMovePct: Number(process.env.MIN_MOVE_PCT) || 0.05,
  stake: Number(process.env.STAKE) || 1,
  epelEnabled: String(process.env.EPEL_ENABLED ?? 'true').toLowerCase() === 'true',
  epelLambda: Number(process.env.EPEL_LAMBDA) || 0.5,
  epelMinSamples: positiveInt(process.env.EPEL_MIN_SAMPLES, 50),
  wilsonZ: Number(process.env.WILSON_Z) || 1.959964,
  ledgerFile: resolvePath(process.env.LEDGER_FILE || './data/r75_tick_epel_ledger.csv'),
  healthFile: resolvePath(process.env.HEALTH_FILE || './data/health.json'),
  port: Number(process.env.PORT) || 8787,
  dashboardToken: process.env.DASHBOARD_TOKEN || '',
  enableTrading: String(process.env.ENABLE_TRADING || 'false').toLowerCase() === 'true',
};

function resolvePath(p) { return path.isAbsolute(p) ? p : path.join(__dirname, p); }
function positiveInt(v, fallback) { const n = Number(v); return Number.isInteger(n) && n > 0 ? n : fallback; }
function parseList(s) { return [...new Set(s.split(',').map(x => Number(x.trim())).filter(n => Number.isInteger(n) && n > 0))].sort((a,b)=>a-b); }

if (!CONFIG.appId || !CONFIG.apiToken) {
  console.error('[FATAL] DERIV_APP_ID and DERIV_API_TOKEN are required. Use the same credentials as the working SynthTrade bot.');
  process.exit(1);
}
if (CONFIG.accountType !== 'demo' && CONFIG.accountType !== 'real') {
  console.error('[FATAL] DERIV_ACCOUNT_TYPE must be demo or real.'); process.exit(1);
}
if (CONFIG.asset !== 'R_75') {
  console.error(`[FATAL] This research build is frozen to R_75. Got ${CONFIG.asset}.`); process.exit(1);
}
if (CONFIG.enableTrading) {
  console.error('[FATAL] ENABLE_TRADING=true is intentionally blocked in v2.2. This build is measurement-only.');
  process.exit(1);
}

fs.mkdirSync(path.dirname(CONFIG.ledgerFile), {recursive:true});
fs.mkdirSync(path.dirname(CONFIG.healthFile), {recursive:true});
if (!fs.existsSync(CONFIG.ledgerFile)) {
  fs.writeFileSync(CONFIG.ledgerFile,
    'signal_id,signal_time,direction,entry_price,lookback_ticks,move_ticks,horizon_ticks,entry_tick_index,expiry_tick_index,expiry_price,result,prob_samples_before,predicted_p,epel_qualifies,epel_threshold,flat_pnl\n');
}

const state = {
  status: 'starting', account: null, connectedAt: null, lastTickAt: null,
  currentPrice: null, tickCount: 0, signalCount: 0, resolvedCount: 0,
  wins: Object.fromEntries(CONFIG.horizons.map(h=>[h,0])),
  losses: Object.fromEntries(CONFIG.horizons.map(h=>[h,0])),
  error: null, wsEndpoint: null, lastApiMessage: null,
};

let ws = null;
let reconnectTimer = null;
let reconnectAttempt = 0;
let reqId = 0;
let tickIndex = 0;
const ticks = [];
const pending = [];
const outcomes = Object.fromEntries(CONFIG.horizons.map(h => [h, {wins:[], losses:[]} ]));

function nextReqId(){ return ++reqId; }
function log(...a){ console.log(new Date().toISOString(), ...a); }
function sleep(ms){ return new Promise(r=>setTimeout(r,ms)); }
async function jsonFetch(url, options, label){
  const r = await fetch(url, options);
  const text = await r.text();
  let j=null; try { j=JSON.parse(text); } catch {}
  if (!r.ok) throw new Error(`${label} HTTP ${r.status}: ${j?.errors?.[0]?.message || text.slice(0,300)}`);
  return j;
}

function wilsonLower(w,n,z=CONFIG.wilsonZ){
  if (!n) return 0;
  const phat=w/n, z2=z*z;
  return (phat + z2/(2*n) - z*Math.sqrt((phat*(1-phat)+z2/(4*n))/n))/(1+z2/n);
}
function epelThreshold(){
  // EL <= lambda * EP; loss=1, win gross profit assumed 0.78.
  const r=0.78;
  return 1/(1+CONFIG.epelLambda*r);
}
function estimate(direction,h){
  const o=outcomes[h];
  const w=direction==='RISE'?o.wins.length:o.losses.length; // placeholder; same-direction sample needs its own store below
  return null;
}

// Separate same-direction historical outcome stores, because EPEL must not mix RISE and FALL base rates.
const directionOutcomes = Object.fromEntries(CONFIG.horizons.map(h=>[h, {RISE:[], FALL:[]} ]));
function predictedProbability(direction,h){
  const arr=directionOutcomes[h][direction];
  if (arr.length < CONFIG.epelMinSamples) return {p:null,samples:arr.length,qualifies:false,threshold:epelThreshold()};
  const wins=arr.reduce((a,b)=>a+b,0);
  const p=(wins+1)/(arr.length+2); // Laplace/Beta(1,1), leakage-safe.
  const lower=wilsonLower(wins,arr.length);
  const threshold=epelThreshold();
  return {p,lower,samples:arr.length,qualifies:lower>=threshold,threshold};
}

async function connectAuthenticated(){
  if (ws && (ws.readyState===WebSocket.OPEN || ws.readyState===WebSocket.CONNECTING)) return;
  state.status='authenticating'; state.error=null;
  log('[CONNECT] Starting the SAME REST + account lookup + OTP WebSocket architecture used by SynthTrade Pro.');
  try {
    const accounts = await jsonFetch('https://api.derivws.com/trading/v1/options/accounts', {
      headers:{'Deriv-App-ID':CONFIG.appId,'Authorization':`Bearer ${CONFIG.apiToken}`}
    }, 'account lookup');
    const list=accounts.data || [];
    const match=list.find(a=>a.account_type===CONFIG.accountType && a.status==='active') || list.find(a=>a.account_type===CONFIG.accountType);
    if(!match) throw new Error(`No ${CONFIG.accountType} account found for this token/app ID`);
    state.account={accountId:match.account_id,accountType:match.account_type,currency:match.currency,balance:Number(match.balance)||0,status:match.status};
    log(`[ACCOUNT] ${match.account_id} type=${match.account_type} currency=${match.currency} balance=${state.account.balance}`);

    const otp=await jsonFetch(`https://api.derivws.com/trading/v1/options/accounts/${match.account_id}/otp`, {
      method:'POST', headers:{'Deriv-App-ID':CONFIG.appId,'Authorization':`Bearer ${CONFIG.apiToken}`}
    }, 'OTP request');
    const url=otp?.data?.url;
    if(!url) throw new Error('OTP response did not contain data.url');
    state.wsEndpoint = url.replace(/([?&]otp=)[^&]+/, '$1<redacted>');
    log(`[OTP] Authenticated WebSocket URL received: ${state.wsEndpoint}`);
    log('[OTP] Connecting immediately because the OTP is short-lived and single-use.');

    ws=new WebSocket(url);
    ws.on('open',()=>{
      reconnectAttempt=0; state.status='authenticated'; state.connectedAt=new Date().toISOString();
      log('[WS] AUTHENTICATED — Deriv WebSocket is open.');
      send({time:1,req_id:nextReqId()});
      send({balance:1,subscribe:1,req_id:nextReqId()});
      send({active_symbols:'brief',product_type:'basic',req_id:nextReqId()});
      send({ticks:CONFIG.asset,subscribe:1,req_id:nextReqId()});
      send({ticks_history:CONFIG.asset,count:200,end:'latest',style:'ticks',req_id:nextReqId()});
      log(`[STREAM] Subscribed to ${CONFIG.asset} ticks + 200-tick history.`);
      writeHealth();
    });
    ws.on('message',raw=>{ try{handleMessage(JSON.parse(raw.toString()));}catch(e){log('[WARN] message parse:',e.message);} });
    ws.on('error',e=>{state.error=e.message; log('[WS ERROR]',e.message); writeHealth();});
    ws.on('close',(code,reason)=>{
      state.status='disconnected'; log(`[WS] closed code=${code} reason=${reason?.toString()||''}`); writeHealth();
      scheduleReconnect();
    });
  } catch(e){
    state.status='error'; state.error=e.message; log('[CONNECT ERROR]',e.message); writeHealth(); scheduleReconnect();
  }
}
function send(o){ if(ws?.readyState===WebSocket.OPEN) ws.send(JSON.stringify(o)); }
function scheduleReconnect(){
  if(reconnectTimer) return;
  reconnectAttempt++;
  const ms=Math.min(30000,2000*Math.max(1,reconnectAttempt));
  log(`[RECONNECT] attempt=${reconnectAttempt} in ${ms}ms`);
  reconnectTimer=setTimeout(()=>{reconnectTimer=null; connectAuthenticated();},ms);
}

function handleMessage(d){
  state.lastApiMessage=d.msg_type||null;
  if(d.error){ state.error=`${d.error.code||'API_ERROR'}: ${d.error.message||'unknown'}`; log('[DERIV ERROR]',JSON.stringify(d.error)); writeHealth(); return; }
  if(d.msg_type==='time'){ log(`[API] time OK: ${d.time}`); return; }
  if(d.msg_type==='balance' && state.account){ state.account.balance=Number(d.balance?.balance)||state.account.balance; return; }
  if(d.msg_type==='active_symbols'){
    const found=(d.active_symbols||[]).find(x=>x.symbol===CONFIG.asset);
    log(`[SYMBOL] ${CONFIG.asset} ${found?'FOUND':'NOT FOUND'} in active_symbols`); return;
  }
  if(d.msg_type==='history'){
    const prices=d.history?.prices||[]; const times=d.history?.times||[];
    log(`[HISTORY] received ${prices.length} historical ${CONFIG.asset} ticks`);
    for(let i=0;i<prices.length;i++) addTick(Number(prices[i]), Number(times[i])*1000, true);
    return;
  }
  if(d.msg_type==='tick'){
    addTick(Number(d.tick.quote), Number(d.tick.epoch)*1000, false);
  }
}

function addTick(price,ts,isHistory){
  if(!Number.isFinite(price)) return;
  tickIndex++;
  const t={i:tickIndex,p:price,ts};
  ticks.push(t); if(ticks.length>5000) ticks.shift();
  state.tickCount++; state.currentPrice=price; state.lastTickAt=new Date(ts).toISOString();
  resolvePending(t);
  if(!isHistory && ticks.length>=CONFIG.lookback+1) evaluateSignal(t);
  writeHealth(false);
}

function evaluateSignal(t){
  const prev=ticks[ticks.length-1-CONFIG.lookback];
  if(!prev) return;
  const move=t.p-prev.p;
  const movePct=Math.abs(prev.p)>0 ? Math.abs(move/prev.p)*100 : 0;
  if(movePct < CONFIG.minMovePct) return;
  const direction=move>0?'FALL':'RISE'; // mean-reversion research seed
  const probs={};
  for(const h of CONFIG.horizons) probs[h]=predictedProbability(direction,h);
  const signalId=`${Date.now()}-${t.i}`;
  state.signalCount++;
  for(const h of CONFIG.horizons){
    const p=probs[h];
    pending.push({signalId,direction,entry:t,horizon:h,targetIndex:t.i+h,prob:p});
    appendRow([signalId,new Date(t.ts).toISOString(),direction,t.p,CONFIG.lookback,move,h,t.i,t.i+h,'',p.samples,p.p??'',p.qualifies,p.threshold,'']);
  }
  log(`[SIGNAL] ${signalId} ${direction} netMove=${move} (${movePct.toFixed(4)}%) over ${CONFIG.lookback} ticks | horizons=${CONFIG.horizons.join(',')} | EPEL threshold=${(epelThreshold()*100).toFixed(2)}%`);
}
function resolvePending(t){
  for(let i=pending.length-1;i>=0;i--){
    const x=pending[i]; if(t.i<x.targetIndex) continue;
    const win=x.direction==='RISE'?t.p>x.entry.p:t.p<x.entry.p;
    const flatPnl=win?0.78:-1;
    state.resolvedCount++; if(win) state.wins[x.horizon]++; else state.losses[x.horizon]++;
    directionOutcomes[x.horizon][x.direction].push(win?1:0);
    const q=predictedProbability(x.direction,x.horizon);
    // Append a resolution row rather than mutating the original row, preserving an audit trail.
    appendRow([x.signalId,new Date(x.entry.ts).toISOString(),x.direction,x.entry.p,CONFIG.lookback, x.entry.p-ticks.find(z=>z.i===x.entry.i-CONFIG.lookback)?.p||'', x.horizon,x.entry.i,t.i,t.p,win?'WIN':'LOSS',x.prob.samples,x.prob.p??'',x.prob.qualifies,x.prob.threshold,flatPnl]);
    pending.splice(i,1);
  }
}
function appendRow(values){ fs.appendFileSync(CONFIG.ledgerFile, values.map(csv).join(',')+'\n'); }
function csv(v){ if(v===null||v===undefined)return ''; const s=String(v); return /[",\n]/.test(s)?'"'+s.replace(/"/g,'""')+'"':s; }

function health(){
  return {version:'2.2.0',mode:'MEASURE_ONLY',asset:CONFIG.asset,status:state.status,account:state.account?{accountId:state.account.accountId,accountType:state.account.accountType,currency:state.account.currency,balance:state.account.balance}:null,wsEndpoint:state.wsEndpoint,connectedAt:state.connectedAt,lastTickAt:state.lastTickAt,currentPrice:state.currentPrice,tickCount:state.tickCount,signalCount:state.signalCount,resolvedCount:state.resolvedCount,wins:state.wins,losses:state.losses,pending:pending.length,error:state.error,epel:{enabled:CONFIG.epelEnabled,lambda:CONFIG.epelLambda,minSamples:CONFIG.epelMinSamples,threshold:epelThreshold()},horizons:CONFIG.horizons,updatedAt:new Date().toISOString()};
}
function writeHealth(verbose=true){ try{fs.writeFileSync(CONFIG.healthFile,JSON.stringify(health(),null,2));}catch(e){if(verbose)log('[HEALTH WRITE ERROR]',e.message);} }

const server=http.createServer((req,res)=>{
  if(req.url==='/health'){
    const body=JSON.stringify(health(),null,2); res.writeHead(200,{'content-type':'application/json','cache-control':'no-store'}); return res.end(body);
  }
  if(req.url==='/'){
    res.writeHead(200,{'content-type':'text/plain; charset=utf-8'}); return res.end(`SynthEPEL-R75 v2.2\nMode: MEASURE_ONLY\nAsset: ${CONFIG.asset}\nHorizons: ${CONFIG.horizons.join(', ')} ticks\nWS: ${state.status}\nTicks: ${state.tickCount}\n`);
  }
  res.writeHead(404); res.end('Not found');
});
server.listen(CONFIG.port,'0.0.0.0',()=>log(`[HTTP] listening on ${CONFIG.port}`));

process.on('SIGTERM',()=>shutdown('SIGTERM')); process.on('SIGINT',()=>shutdown('SIGINT'));
function shutdown(sig){ log(`[SHUTDOWN] ${sig}`); try{ws?.close();}catch{} try{server.close();}catch{} process.exit(0); }

log('SynthEPEL-R75 v2.2 starting.');
log(`Research: ${CONFIG.asset} | horizons=${CONFIG.horizons.join(',')} ticks | lookback=${CONFIG.lookback} | minMovePct=${CONFIG.minMovePct}`);
log('Trading is hard-disabled in this build.');
writeHealth();
connectAuthenticated();
