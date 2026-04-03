# -*- coding: utf-8 -*-
"""
Dashboard сервер — самодостаточный файл, HTML встроен внутрь.
НЕ нужна папка dashboard_static — всё работает из одного файла.

Запуск:
    pip install flask python-dotenv
    python dashboard_server.py
    Открыть: http://localhost:8080
"""

import os, sys, threading, asyncio, time, logging, json
from pathlib import Path
from flask import Flask, jsonify, request, Response
from dotenv import load_dotenv, set_key

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(BASE_DIR))

# Все киберспортивные sport_id (начинаются с 🎮 в SPORT_NAMES)
try:
    from polymarket_bet import SPORT_NAMES as _SN
    ESPORTS_IDS = frozenset(sid for sid, name in _SN.items() if "🎮" in name)
except Exception:
    ESPORTS_IDS = frozenset({21,39,41,46,47,48,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71})
ENV_FILE = BASE_DIR / ".env"
if not ENV_FILE.exists():
    ex = BASE_DIR / ".env.example"
    if ex.exists():
        import shutil; shutil.copy(str(ex), str(ENV_FILE))
    else:
        ENV_FILE.write_text("")

# ── HTML встроен прямо здесь ──────────────────────────────────────────────────
_HTML = """<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>POLYBOT — Value Bet Terminal</title>
<link href="https://fonts.googleapis.com/css2?family=Space+Mono:wght@400;700&family=Syne:wght@700;800&display=swap" rel="stylesheet">
<style>
:root {
  --bg:    #06090d;
  --bg2:   #0b0f16;
  --bg3:   #0f1520;
  --line:  #162030;
  --line2: #1c3048;
  --g:     #00e87a;
  --g2:    #00b85f;
  --gd:    rgba(0,232,122,.07);
  --gg:    rgba(0,232,122,.22);
  --r:     #ff3c5a;
  --rd:    rgba(255,60,90,.08);
  --y:     #ffc94d;
  --yd:    rgba(255,201,77,.08);
  --b:     #40aaff;
  --tx:    #b8cfe0;
  --tx2:   #4a6a84;
  --tx3:   #1e3a52;
  --mono:  'Space Mono', monospace;
  --sans:  'Syne', sans-serif;
}
*{box-sizing:border-box;margin:0;padding:0}
html{font-size:13px}
body{background:var(--bg);color:var(--tx);font-family:var(--mono);height:100vh;overflow:hidden;display:grid;grid-template-rows:52px 1fr;grid-template-columns:210px 1fr 300px}
::-webkit-scrollbar{width:4px;height:4px}
::-webkit-scrollbar-track{background:var(--bg2)}
::-webkit-scrollbar-thumb{background:var(--line2);border-radius:2px}

/* HEADER */
header{grid-column:1/-1;background:var(--bg2);border-bottom:1px solid var(--line);display:flex;align-items:center;padding:0 18px;gap:0;position:relative;z-index:50}
.logo{font-family:var(--sans);font-size:15px;font-weight:800;letter-spacing:5px;color:var(--g);text-shadow:0 0 24px var(--gg);padding-right:20px;flex-shrink:0}
.logo em{color:var(--tx2);font-style:normal}
.hstat{display:flex;flex-direction:column;padding:0 18px;border-left:1px solid var(--line)}
.hstat .hl{font-size:9px;color:var(--tx2);letter-spacing:1.5px;text-transform:uppercase;margin-bottom:3px}
.hstat .hv{font-size:13px;font-weight:700;font-variant-numeric:tabular-nums}
.hv.g{color:var(--g)}.hv.r{color:var(--r)}.hv.y{color:var(--y)}.hv.n{color:var(--tx)}
.hr{margin-left:auto;display:flex;align-items:center;gap:12px}
.dot{width:8px;height:8px;border-radius:50%;background:var(--r);transition:.3s}
.dot.on{background:var(--g);box-shadow:0 0 8px var(--g);animation:pulse 2s infinite}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.35}}
.bst{font-size:10px;letter-spacing:1.5px;text-transform:uppercase;color:var(--tx2)}
.upt{font-size:11px;color:var(--tx3);font-variant-numeric:tabular-nums;min-width:70px}
.btn{font-family:var(--mono);font-size:10px;font-weight:700;letter-spacing:2px;text-transform:uppercase;border:1px solid currentColor;color:#ccc;background:transparent;padding:6px 14px;cursor:pointer;border-radius:3px;transition:.15s}
.btn-g{color:var(--g)}.btn-g:hover{background:var(--gd);box-shadow:0 0 12px var(--gg)}
.btn-r{color:var(--r)}.btn-r:hover{background:var(--rd)}

/* SIDEBAR */
aside.left{background:var(--bg2);border-right:1px solid var(--line);overflow-y:auto;padding:8px 0}
.nsec{padding:12px 0 4px}
.nlb{font-size:9px;color:var(--tx3);letter-spacing:2px;text-transform:uppercase;padding:0 14px 6px;font-weight:700}
.ni{display:flex;align-items:center;gap:9px;padding:9px 14px;cursor:pointer;font-size:11px;color:var(--tx2);letter-spacing:.3px;transition:.1s;border-left:2px solid transparent}
.ni:hover{color:var(--tx);background:var(--gd);border-left-color:var(--line2)}
.ni.on{color:var(--g);background:var(--gd);border-left-color:var(--g)}
.ni .ic{font-size:13px;width:16px;text-align:center}
.nbadge{margin-left:auto;font-size:9px;padding:2px 6px;background:var(--gd);color:var(--g);border:1px solid rgba(0,232,122,.2);border-radius:2px}

/* MAIN */
main{overflow-y:auto;padding:16px;display:flex;flex-direction:column;gap:14px}
.page{display:none;flex-direction:column;gap:14px}
.page.on{display:flex}

/* PANELS */
.pnl{background:var(--bg2);border:1px solid var(--line);border-radius:3px;overflow:hidden}
.ph{display:flex;align-items:center;justify-content:space-between;padding:10px 14px;border-bottom:1px solid var(--line);background:var(--bg3)}
.pt{font-size:9px;letter-spacing:2px;text-transform:uppercase;color:var(--tx2);font-weight:700}
.pt span{color:var(--g);margin-right:5px}
.pb{padding:14px}

/* STATS GRID */
.sg{display:grid;grid-template-columns:repeat(4,1fr);gap:10px}
.sc{background:var(--bg2);border:1px solid var(--line);border-radius:3px;padding:14px;position:relative;overflow:hidden;cursor:default}
.sc::before{content:'';position:absolute;top:0;left:0;right:0;height:2px}
.sc.cg::before{background:var(--g)} .sc.cr::before{background:var(--r)} .sc.cy::before{background:var(--y)} .sc.cb::before{background:var(--b)}
.sl{font-size:9px;letter-spacing:1.5px;text-transform:uppercase;color:var(--tx2);margin-bottom:8px}
.sv{font-size:26px;font-weight:700;font-family:var(--sans);line-height:1}
.sv.g{color:var(--g)}.sv.r{color:var(--r)}.sv.y{color:var(--y)}.sv.b{color:var(--b)}.sv.n{color:var(--tx)}
.ss{font-size:10px;color:var(--tx2);margin-top:5px}

/* TABLE */
.tw{overflow-x:auto}
table{width:100%;border-collapse:collapse;font-size:11px}
th{text-align:left;font-size:9px;letter-spacing:1.5px;text-transform:uppercase;color:var(--tx3);padding:8px 10px;border-bottom:1px solid var(--line);white-space:nowrap;font-weight:700}
tr{border-bottom:1px solid var(--line);transition:background .1s}
tr:hover{background:rgba(255,255,255,.015)}
td{padding:9px 10px;vertical-align:middle;white-space:nowrap}
.ten{max-width:180px;overflow:hidden;text-overflow:ellipsis}
.ten .en{color:var(--tx);font-size:11px}
.ten .el{font-size:9px;color:var(--tx2);margin-top:1px}
.tou .on{color:var(--y)}
.tou .ot{font-size:9px;color:var(--tx2);margin-top:1px}
.edge{color:var(--g);font-weight:700}
.pp{color:var(--g);font-weight:700}.np{color:var(--r);font-weight:700}
.badge{display:inline-flex;align-items:center;font-size:9px;font-weight:700;letter-spacing:1px;text-transform:uppercase;padding:2px 7px;border-radius:2px;border:1px solid}
.bpl{color:var(--g);border-color:var(--g);background:var(--gd)}
.bpe{color:var(--y);border-color:var(--y);background:var(--yd)}
.bfa{color:var(--r);border-color:var(--r);background:var(--rd)}
.bse{color:var(--tx2);border-color:var(--line);background:transparent}
.bwn{color:var(--g);border-color:var(--g);background:var(--gd)}
.bls{color:var(--r);border-color:var(--r);background:var(--rd)}
.bvo{color:var(--tx2);border-color:var(--line);background:transparent}
.bsd{color:#ffb800;border-color:#ffb800;background:rgba(255,184,0,.1)}

/* CHART */
.dchart{display:flex;align-items:flex-end;gap:5px;height:72px;padding:0 2px}
.db{flex:1;display:flex;flex-direction:column;align-items:center;height:100%;gap:3px;cursor:default}
.dbar{width:100%;border-radius:2px 2px 0 0;min-height:2px;transition:.2s}
.dbar:hover{opacity:.7}
.dbar.pos{background:var(--g)} .dbar.neg{background:var(--r)}
.dlb{font-size:8px;color:var(--tx3)}

/* SETTINGS */
.sgrids{display:grid;grid-template-columns:1fr 1fr;gap:20px}
.sgroup{display:flex;flex-direction:column;gap:10px}
.sgtitle{font-size:9px;letter-spacing:2px;text-transform:uppercase;color:var(--tx3);padding-bottom:8px;border-bottom:1px solid var(--line);font-weight:700}
.srow{display:flex;align-items:center;justify-content:space-between;gap:12px}
.slbl{font-size:11px;color:var(--tx2);flex:1}
.slbl small{display:block;font-size:9px;color:var(--tx3);margin-top:2px}
.sinput{font-family:var(--mono);font-size:12px;color:var(--g);background:var(--bg);border:1px solid var(--line);border-radius:3px;padding:6px 10px;width:110px;text-align:right;outline:none;transition:.15s}
.sinput:focus{border-color:var(--g);box-shadow:0 0 8px var(--gg)}
.sinput.wd{width:170px;text-align:left}
.tog{position:relative;width:38px;height:21px;flex-shrink:0}
.tog input{display:none}
.tslider{position:absolute;inset:0;background:var(--bg3);border:1px solid var(--line);border-radius:11px;cursor:pointer;transition:.2s}
.tslider::after{content:'';position:absolute;left:3px;top:3px;width:13px;height:13px;border-radius:50%;background:var(--tx3);transition:.2s}
.tog input:checked+.tslider{background:var(--gd);border-color:var(--g)}
.tog input:checked+.tslider::after{transform:translateX(17px);background:var(--g)}
.savebtn{font-family:var(--mono);font-size:10px;font-weight:700;letter-spacing:2px;text-transform:uppercase;color:var(--g);background:var(--gd);border:1px solid var(--g);border-radius:3px;padding:10px 22px;cursor:pointer;transition:.15s}
.savebtn:hover{box-shadow:0 0 16px var(--gg)}
.savemsg{font-size:10px;color:var(--g);margin-left:10px;opacity:0;transition:.3s}
.savemsg.on{opacity:1}
.brlinput{font-family:var(--mono);font-size:18px;font-weight:700;color:var(--g);background:var(--bg);border:1px solid var(--line);border-radius:3px;padding:8px 12px;width:150px;outline:none}
.brlinput:focus{border-color:var(--g)}
.brlbtn{font-family:var(--mono);font-size:10px;font-weight:700;letter-spacing:1px;text-transform:uppercase;color:var(--g);background:transparent;border:1px solid var(--g);border-radius:3px;padding:8px 12px;cursor:pointer;transition:.15s}
.brlbtn:hover{background:var(--gd)}

/* LOG */
.logbox{background:#040710;border-top:none;padding:12px 14px;height:380px;overflow-y:auto;font-size:11px;line-height:1.65}
.ll{word-break:break-all}
.ll.li{color:var(--tx)} .ll.lw{color:var(--y)} .ll.le{color:var(--r)}
.ll.lb{color:var(--g);font-weight:700} .ll.ls{color:var(--tx3)}

/* SETTLE MODAL */
.moverlay{position:fixed;inset:0;background:rgba(0,0,0,.85);backdrop-filter:blur(4px);z-index:200;display:none;align-items:center;justify-content:center}
.moverlay.on{display:flex}
.modal{background:var(--bg2);border:1px solid var(--line2);border-radius:4px;padding:24px;width:400px;box-shadow:0 0 60px rgba(0,0,0,.9)}
.mtitle{font-family:var(--sans);font-size:13px;font-weight:700;color:var(--g);letter-spacing:2px;text-transform:uppercase;margin-bottom:16px}
.minfo{background:var(--bg3);border:1px solid var(--line);border-radius:3px;padding:10px 12px;margin-bottom:16px;font-size:11px;line-height:1.9}
.minfo .ml{color:var(--tx2)} .minfo .mv{color:var(--tx);font-weight:700}
.rbtnrow{display:grid;grid-template-columns:repeat(5,1fr);gap:7px;margin-bottom:14px}
.rbtn{font-family:var(--mono);font-size:10px;font-weight:700;letter-spacing:1.5px;text-transform:uppercase;padding:9px;border-radius:3px;cursor:pointer;border:1px solid;background:transparent;transition:.15s}
.rbtn.won{color:var(--g);border-color:var(--g)}.rbtn.won.on,.rbtn.won:hover{background:var(--gd)}
.rbtn.lost{color:var(--r);border-color:var(--r)}.rbtn.lost.on,.rbtn.lost:hover{background:var(--rd)}
.rbtn.void,.rbtn.push{color:var(--tx2);border-color:var(--line)}.rbtn.void.on,.rbtn.void:hover,.rbtn.push.on,.rbtn.push:hover{background:rgba(255,255,255,.04)}
.rbtn.sold{color:#ffb800;border-color:#ffb800}.rbtn.sold.on,.rbtn.sold:hover{background:rgba(255,184,0,.15)}
.sell-row{background:rgba(255,184,0,.05);border:1px solid rgba(255,184,0,.2);border-radius:4px;padding:10px;margin-bottom:10px}
.sell-mode-btn{font-family:var(--mono);font-size:9px;font-weight:700;padding:4px 10px;border:1px solid #555;background:transparent;color:#aaa;cursor:pointer;border-radius:2px;transition:.15s}
.sell-mode-btn.active{background:rgba(255,184,0,.2);color:#ffb800;border-color:#ffb800}
.prow{display:flex;align-items:center;gap:10px;margin-bottom:18px}
.prow label{font-size:10px;color:var(--tx2);text-transform:uppercase;letter-spacing:1px;width:60px}
.pinput{flex:1;font-family:var(--mono);font-size:13px;color:var(--g);background:var(--bg);border:1px solid var(--line);border-radius:3px;padding:7px 10px;outline:none}
.pinput:focus{border-color:var(--g)}
.mfooter{display:flex;gap:8px;justify-content:flex-end}
.mbtn-cancel{color:var(--tx2);border-color:var(--line)}
.mbtn-ok{color:var(--g);border-color:var(--g)}.mbtn-ok:hover{background:var(--gd)}

/* RIGHT PANEL */
aside.right{background:var(--bg2);border-left:1px solid var(--line);overflow-y:auto;display:flex;flex-direction:column}
.rps{padding:14px;border-bottom:1px solid var(--line)}
.rpt{font-size:9px;letter-spacing:2px;text-transform:uppercase;color:var(--tx3);margin-bottom:10px;font-weight:700}
.abcard{background:var(--bg3);border:1px solid var(--line);border-radius:3px;padding:9px 10px;margin-bottom:7px;border-left:3px solid var(--g)}
.abev{font-size:11px;color:var(--tx);margin-bottom:2px}
.about{font-size:10px;color:var(--y);margin-bottom:5px}
.abmeta{display:flex;gap:10px;font-size:9px}
.abmeta span{color:var(--tx2)}.abmeta .av{color:var(--g);font-weight:700}
.absb{margin-top:7px;font-family:var(--mono);font-size:9px;font-weight:700;letter-spacing:1px;text-transform:uppercase;color:var(--tx2);background:transparent;border:1px solid var(--line);border-radius:2px;padding:4px 8px;cursor:pointer;transition:.15s;width:100%}
.absb:hover{color:var(--g);border-color:var(--g)}
.sprow{display:flex;align-items:center;justify-content:space-between;padding:5px 0;font-size:11px;border-bottom:1px solid var(--line)}
.sprow:last-child{border:none}
.spnm{color:var(--tx2)} .spcnt{color:var(--tx);font-weight:700} .sppnl{font-size:10px;font-weight:700;min-width:56px;text-align:right}
.nodata{padding:20px;text-align:center;color:var(--tx3);font-size:11px;letter-spacing:.5px}

/* TOAST */
.toast{position:fixed;bottom:20px;right:20px;background:var(--bg2);border:1px solid var(--g);color:var(--g);font-size:11px;padding:10px 18px;border-radius:3px;box-shadow:0 0 20px var(--gg);z-index:500;opacity:0;transform:translateY(8px);transition:.25s;pointer-events:none}
.toast.on{opacity:1;transform:none}
.toast.err{border-color:var(--r);color:var(--r);box-shadow:none}

/* DEMO BANNER */
.demobanner{background:var(--yd);border:1px solid var(--y);color:var(--y);font-size:10px;padding:8px 14px;border-radius:3px;letter-spacing:.5px;display:flex;align-items:center;gap:8px}
.demobanner.hidden{display:none}
.btn-sm{padding:4px 10px;font-size:10px;letter-spacing:1px;cursor:pointer;border:1px solid var(--g1);background:transparent;color:var(--g1);font-family:var(--mono);transition:.2s}
.btn-sm:hover{opacity:.8}
.hsep{width:1px;background:var(--line);margin:0 4px;align-self:stretch}
</style>
</head>
<body>

<!-- HEADER -->
<header>
  <div class="logo">POLY<em>BOT</em></div>
  <div class="hstat"><div class="hl">Банкролл</div><div class="hv n" id="h-brl">—</div></div>
  <div class="hstat"><div class="hl">Итого P&L</div><div class="hv n" id="h-pnl">—</div></div>
  <div class="hstat"><div class="hl">ROI</div><div class="hv n" id="h-roi">—</div></div>
  <div class="hstat"><div class="hl">Win Rate</div><div class="hv n" id="h-wr">—</div></div>
  <div class="hstat"><div class="hl">Ставок</div><div class="hv n" id="h-tot">—</div></div>
  <div class="hstat"><div class="hl">Свободно</div><div class="hv" id="h-free" style="color:#2ecc71">—</div></div>
  <div class="hstat"><div class="hl">Fees</div><div class="hv" id="h-fees" style="color:#555">$0</div></div>
  <div class="hsep"></div>
  <div class="hstat" title="PM: свободный USDC (обновляется каждые 5 мин)" style="cursor:pointer" onclick="loadWallet(true)"><div class="hl">PM CASH</div><div class="hv" id="hpm-cash" style="color:#3498db">—</div></div>
  <div class="hstat" title="PM: стоимость позиций" style="cursor:pointer" onclick="loadWallet(true)"><div class="hl">PM ПОЗИЦИИ</div><div class="hv" id="hpm-portval" style="color:#9b59b6">—</div></div>
  <div class="hstat" title="PM: итого (cash + позиции)" style="cursor:pointer" onclick="loadWallet(true)"><div class="hl">PM ИТОГО</div><div class="hv" id="hpm-total" style="color:#e67e22">—</div></div>
  <div class="hstat" title="Settled позиции ожидающие выплаты" style="cursor:pointer" onclick="loadWallet(true)">
    <div class="hl">РЕДИМ</div>
    <div class="hv" id="hpm-redeem" style="color:#2ecc71">—</div>
  </div>
  <button id="hpm-redeem-btn" onclick="headerRedeem()" title="Выкупить все settled позиции"
    style="padding:3px 10px;background:#1a5c35;color:#2ecc71;border:1px solid #2ecc7155;
           font-family:monospace;font-size:10px;font-weight:700;cursor:pointer;border-radius:2px;
           letter-spacing:1px;flex-shrink:0;margin-left:4px">
    ↑ REDEEM
  </button>
  <div class="hr">
    <span class="upt" id="upt">00:00:00</span>
    <div class="dot" id="dot"></div>
    <span class="bst" id="bst">ОСТАНОВЛЕН</span>
    <button id="ar-toggle" onclick="toggleAutoRefresh()" title="Автообновление вкл/выкл"
      style="padding:4px 10px;background:transparent;color:#00e87a;border:1px solid #00e87a44;font-family:monospace;font-size:10px;cursor:pointer;border-radius:2px;letter-spacing:1px">⏸ АВТО</button>
    <button class="btn btn-g" id="btn-start" onclick="startBot()">▶ СТАРТ</button>
    <button class="btn btn-r" id="btn-stop"  onclick="stopBot()" style="display:none">■ СТОП</button>
    <button class="btn" id="btn-live-start" onclick="startLiveBot()"
      style="background:#e67e22;color:#000;font-weight:900;border:none">⚡ ЛАЙВ</button>
    <button class="btn btn-r" id="btn-live-stop" onclick="stopLiveBot()" style="display:none">■ СТОП ЛАЙВ</button>
  </div>
</header>

<!-- SIDEBAR -->
<aside class="left">
  <div class="nsec">
    <div class="nlb">Обзор</div>
    <div class="ni on" data-p="dash" onclick="nav('dash')"><span class="ic">▦</span>Дашборд</div>
    <div class="ni" data-p="bets" onclick="nav('bets')"><span class="ic">◈</span>История<span class="nbadge" id="nbact">0</span></div>
  </div>
  <div class="nsec">
    <div class="nlb">Управление</div>
    <div class="ni" data-p="cfg" onclick="nav('cfg')"><span class="ic">⚙</span>Настройки</div>
    <div class="ni" data-p="log" onclick="nav('log')"><span class="ic">≡</span>Лог бота</div>
    <div class="ni" data-p="signals" onclick="nav('signals')"><span class="ic">⚡</span>Сигналы BB</div>
    <div class="ni" data-p="feed" onclick="nav('feed')"><span class="ic">📡</span>Фид BB</div>
    <div class="ni" data-p="portfolio" onclick="nav('portfolio')"><span class="ic">◈</span>Портфель PM</div>
  </div>
  <div class="nsec">
    <div class="nlb">Хедж</div>
    <div class="ni" data-p="hedge" onclick="nav('hedge')"><span class="ic">⚖</span>Hedge Calc</div>
  </div>
  <div class="nsec">
    <div class="nlb">Арбитраж</div>
    <div class="ni" data-p="dutch" onclick="nav('dutch')"><span class="ic">&#9889;</span>Dutching</div>
    <div class="ni" data-p="mm" onclick="nav('mm')"><span class="ic">&#9878;</span>Market Making</div>
    <div class="ni" data-p="snipe" onclick="nav('snipe')"><span class="ic">&#127919;</span>Settlement</div>
    <div class="ni" data-p="backlog" onclick="nav('backlog')"><span class="ic">&#9776;</span>Backlog</div>
  </div>
</aside>

<!-- MAIN -->
<main>

  <!-- DASHBOARD -->
  <div class="page on" id="page-dash">

    <div class="demobanner" id="demobanner">
      ⚠ Демо-режим: сервер недоступен. Запусти <code style="background:rgba(0,0,0,.3);padding:1px 5px;border-radius:2px">python dashboard_server.py</code> для реального подключения.
    </div>

    <div class="sg">
      <div class="sc cg"><div class="sl">Размещено</div><div class="sv b" id="s-placed">—</div><div class="ss" id="s-pending">—</div></div>
      <div class="sc cr"><div class="sl">Выиграно / Проиграно</div><div class="sv n" id="s-wl">—</div><div class="ss" id="s-wr">—</div></div>
      <div class="sc cy"><div class="sl">Объём (USDC)</div><div class="sv y" id="s-vol">—</div><div class="ss">total staked</div></div>
      <div class="sc cb"><div class="sl">Avg Edge</div><div class="sv g" id="s-edge">—</div><div class="ss">от BetBurger</div></div>
    </div>

    <div style="display:grid;grid-template-columns:1fr 1fr;gap:12px">
      <div class="pnl">
        <div class="ph"><span class="pt"><span>▸</span>P&L по дням</span></div>
        <div class="pb"><div class="dchart" id="dchart"><div class="nodata">Нет данных</div></div></div>
      </div>
      <div class="pnl">
        <div class="ph"><span class="pt"><span>▸</span>Банкролл</span></div>
        <div class="pb">
          <div style="font-size:10px;color:var(--tx2);margin-bottom:7px">Текущий банкролл (USDC)</div>
          <div style="display:flex;align-items:center;gap:8px;margin-bottom:12px">
            <input type="number" class="brlinput" id="brl-inp" placeholder="500.00">
            <button class="brlbtn" onclick="saveBankroll()">SET</button>
          </div>
          <div style="font-size:10px;color:var(--tx3);line-height:2">
            Ставка = <span style="color:var(--g)" id="stk-pct">1.0%</span> от банкролла<br>
            Макс = <span style="color:var(--y)" id="max-pct">5.0%</span> от банкролла
          </div>
        </div>
      </div>
    </div>

    <!-- Resell stats block -->
    <div class="pnl" id="resell-block">
      <div class="ph" style="border-bottom-color:#3d2e00">
        <span class="pt" style="color:#ffb800"><span>▸</span>🔄 Авто-продажа (Resell)</span>
        <button class="btn" style="font-size:9px;padding:4px 10px;color:#ffb800;border-color:#ffb800" onclick="loadResellStats()">↻</button>
      </div>
      <div class="pb">
        <div style="display:grid;grid-template-columns:repeat(7,1fr);gap:8px">
          <div style="text-align:center">
            <div style="font-size:9px;color:var(--tx2);text-transform:uppercase;letter-spacing:1px;margin-bottom:4px">Продано</div>
            <div style="font-size:20px;font-weight:700;color:#ffb800;font-family:var(--sans)" id="rs-sold">0</div>
          </div>
          <div style="text-align:center">
            <div style="font-size:9px;color:var(--tx2);text-transform:uppercase;letter-spacing:1px;margin-bottom:4px">Прибыль</div>
            <div style="font-size:20px;font-weight:700;color:var(--g);font-family:var(--sans)" id="rs-profit">$0</div>
          </div>
          <div style="text-align:center">
            <div style="font-size:9px;color:var(--tx2);text-transform:uppercase;letter-spacing:1px;margin-bottom:4px">Оборот</div>
            <div style="font-size:20px;font-weight:700;color:var(--tx);font-family:var(--sans)" id="rs-volume">$0</div>
          </div>
          <div style="text-align:center">
            <div style="font-size:9px;color:var(--tx2);text-transform:uppercase;letter-spacing:1px;margin-bottom:4px">ROI</div>
            <div style="font-size:20px;font-weight:700;color:var(--g);font-family:var(--sans)" id="rs-roi">—</div>
          </div>
          <div style="text-align:center">
            <div style="font-size:9px;color:var(--tx2);text-transform:uppercase;letter-spacing:1px;margin-bottom:4px">Avg Markup</div>
            <div style="font-size:20px;font-weight:700;color:#ffb800;font-family:var(--sans)" id="rs-avg">0%</div>
          </div>
          <div style="text-align:center">
            <div style="font-size:9px;color:var(--tx2);text-transform:uppercase;letter-spacing:1px;margin-bottom:4px">На продаже</div>
            <div style="font-size:20px;font-weight:700;color:var(--y);font-family:var(--sans)" id="rs-pending">0</div>
          </div>
          <div style="text-align:center">
            <div style="font-size:9px;color:var(--tx2);text-transform:uppercase;letter-spacing:1px;margin-bottom:4px">Истекло</div>
            <div style="font-size:20px;font-weight:700;color:var(--tx2);font-family:var(--sans)" id="rs-expired">0</div>
          </div>
        </div>
        <div style="display:flex;gap:20px;margin-top:10px;font-size:10px;color:var(--tx2);justify-content:center">
          <span>📈 ПМ: <b id="rs-pm-sold" style="color:var(--tx)">0</b> продано, <b id="rs-pm-profit" style="color:var(--g)">$0</b></span>
          <span>⚡ Лайв: <b id="rs-lv-sold" style="color:#e67e22">0</b> продано, <b id="rs-lv-profit" style="color:var(--g)">$0</b></span>
        </div>
      </div>
    </div>

    <div class="pnl" id="line-move-block" style="display:none">
      <div class="ph" style="border-bottom-color:#1a3a2a">
        <span class="pt" style="color:#2ecc71"><span>▸</span>Line Movement (движение линии)</span>
        <button class="btn" style="font-size:9px;padding:4px 10px;color:#2ecc71;border-color:#2ecc71" onclick="loadLineStats()">↻</button>
      </div>
      <div class="pb">
        <div style="display:grid;grid-template-columns:repeat(3,1fr);gap:8px;margin-bottom:10px">
          <div style="text-align:center">
            <div style="font-size:9px;color:var(--tx2);text-transform:uppercase;letter-spacing:1px;margin-bottom:4px">Отслежено</div>
            <div style="font-size:20px;font-weight:700;color:#eee" id="lm-tracked">0</div>
          </div>
          <div style="text-align:center">
            <div style="font-size:9px;color:var(--tx2);text-transform:uppercase;letter-spacing:1px;margin-bottom:4px">В нашу пользу</div>
            <div style="font-size:20px;font-weight:700;color:var(--g)" id="lm-favorable">0%</div>
          </div>
          <div style="text-align:center">
            <div style="font-size:9px;color:var(--tx2);text-transform:uppercase;letter-spacing:1px;margin-bottom:4px">Avg движение</div>
            <div style="font-size:20px;font-weight:700" id="lm-avg">0%</div>
          </div>
        </div>
        <table class="bt" style="width:100%;font-size:11px">
          <thead><tr><th>Спорт</th><th>Ставок</th><th>Avg движение</th><th>В пользу %</th></tr></thead>
          <tbody id="lm-sports-tbody"></tbody>
        </table>
      </div>
    </div>

    <div class="pnl">
      <div class="ph">
        <span class="pt"><span>▸</span>Последние ставки</span>
        <button class="btn btn-g" onclick="loadBets()" style="font-size:9px;padding:4px 10px">↻</button>
      </div>
      <div class="tw">
        <table>
          <thead><tr>
            <th>#</th><th>Событие</th><th>Исход</th><th>Edge%</th><th>Коэф</th><th title="Ликвидность рынка">Ликв $</th><th title="Реально потрачено USDC">Ставка $</th><th title="Выигрыш при победе">Выигрыш $</th><th>Статус</th><th>P&L</th><th>Матч</th><th></th>
          </tr></thead>
          <tbody id="tb-dash"></tbody>
        </table>
      </div>
    </div>
  </div>

  <!-- BETS PAGE -->
  <div class="page" id="page-bets">
    <div class="pnl">
      <div class="ph">
        <span class="pt"><span>▸</span>История ставок</span>
        <div style="display:flex;gap:6px;align-items:center">
          <button onclick="autoSettle()" style="padding:4px 12px;background:#e67e22;color:#fff;border:none;font-family:monospace;font-size:10px;font-weight:700;cursor:pointer;border-radius:2px">⚡ АВТО-РАСЧЁТ</button>
          <button onclick="fixPnl()" style="padding:4px 12px;background:#8e44ad;color:#fff;border:none;font-family:monospace;font-size:10px;font-weight:700;cursor:pointer;border-radius:2px" title="Пересчитать P&L для всех settled ставок (исправить старые записи)">🔧 ПЕРЕСЧЁТ P&L</button>
          <button onclick="fixWrongWon()" style="padding:4px 12px;background:#c0392b;color:#fff;border:none;font-family:monospace;font-size:10px;font-weight:700;cursor:pointer;border-radius:2px" title="Найти WON-ставки где токен на самом деле проиграл и исправить через Gamma API">🔍 ИСПРАВИТЬ WON→LOST</button>
          <button onclick="fixCancelled()" style="padding:4px 12px;background:#2c3e50;color:#aaa;border:1px solid #555;font-family:monospace;font-size:10px;font-weight:700;cursor:pointer;border-radius:2px" title="Пометить cancelled/failed ставки как void (не влияют на P&L и winrate)">🚫 ЗАКРЫТЬ ОТМЕНЁННЫЕ</button>
          <button class="btn btn-g" onclick="loadBetsPage(1)" style="font-size:9px;padding:4px 10px">↻</button>
        </div>
      </div>

      <!-- Filters -->
      <div id="bets-filters" style="padding:10px 14px;border-bottom:1px solid var(--line);display:flex;gap:8px;flex-wrap:wrap;align-items:flex-end">
        <div>
          <div style="font-size:9px;color:var(--tx2);margin-bottom:3px;text-transform:uppercase">Результат</div>
          <select id="f-result" onchange="loadBetsPage(1)" style="font-family:var(--mono);font-size:10px;background:var(--bg2);color:var(--tx);border:1px solid var(--line);padding:3px 6px;outline:none">
            <option value="">Все</option>
            <option value="pending">Активные</option>
            <option value="won">Выигрыш ✅</option>
            <option value="lost">Проигрыш ❌</option>
            <option value="sold">Продано 💰</option>
            <option value="void">Void ⚪</option>
          </select>
        </div>
        <div>
          <div style="font-size:9px;color:var(--tx2);margin-bottom:3px;text-transform:uppercase">Спорт</div>
          <select id="f-sport" onchange="loadBetsPage(1)" style="font-family:var(--mono);font-size:10px;background:var(--bg2);color:var(--tx);border:1px solid var(--line);padding:3px 6px;outline:none">
            <option value="">Все виды</option>
          </select>
        </div>
        <div>
          <div style="font-size:9px;color:var(--tx2);margin-bottom:3px;text-transform:uppercase">Лига / Команда</div>
          <input id="f-league" type="text" placeholder="поиск..." oninput="clearTimeout(window._lt);window._lt=setTimeout(()=>loadBetsPage(1),400)" style="font-family:var(--mono);font-size:10px;background:var(--bg2);color:var(--tx);border:1px solid var(--line);padding:3px 6px;outline:none;width:120px">
        </div>
        <div>
          <div style="font-size:9px;color:var(--tx2);margin-bottom:3px;text-transform:uppercase">Дата от</div>
          <input id="f-date-from" type="date" onchange="loadBetsPage(1)" style="font-family:var(--mono);font-size:10px;background:var(--bg2);color:var(--tx);border:1px solid var(--line);padding:3px 6px;outline:none">
        </div>
        <div>
          <div style="font-size:9px;color:var(--tx2);margin-bottom:3px;text-transform:uppercase">Дата до</div>
          <input id="f-date-to" type="date" onchange="loadBetsPage(1)" style="font-family:var(--mono);font-size:10px;background:var(--bg2);color:var(--tx);border:1px solid var(--line);padding:3px 6px;outline:none">
        </div>
        <div>
          <div style="font-size:9px;color:var(--tx2);margin-bottom:3px;text-transform:uppercase">Коэф от / до</div>
          <div style="display:flex;gap:4px">
            <input id="f-odds-min" type="number" placeholder="1.0" step="0.1" oninput="clearTimeout(window._lt2);window._lt2=setTimeout(()=>loadBetsPage(1),600)" style="font-family:var(--mono);font-size:10px;background:var(--bg2);color:var(--tx);border:1px solid var(--line);padding:3px 4px;outline:none;width:52px">
            <input id="f-odds-max" type="number" placeholder="∞" step="0.1" oninput="clearTimeout(window._lt3);window._lt3=setTimeout(()=>loadBetsPage(1),600)" style="font-family:var(--mono);font-size:10px;background:var(--bg2);color:var(--tx);border:1px solid var(--line);padding:3px 4px;outline:none;width:52px">
          </div>
        </div>
        <div>
          <div style="font-size:9px;color:var(--tx2);margin-bottom:3px;text-transform:uppercase">Ликв от / до</div>
          <div style="display:flex;gap:4px">
            <input id="f-liq-min" type="number" placeholder="0" step="100" oninput="clearTimeout(window._lt8);window._lt8=setTimeout(()=>loadBetsPage(1),600)" style="font-family:var(--mono);font-size:10px;background:var(--bg2);color:var(--tx);border:1px solid var(--line);padding:3px 4px;outline:none;width:52px">
            <input id="f-liq-max" type="number" placeholder="∞" step="100" oninput="clearTimeout(window._lt9);window._lt9=setTimeout(()=>loadBetsPage(1),600)" style="font-family:var(--mono);font-size:10px;background:var(--bg2);color:var(--tx);border:1px solid var(--line);padding:3px 4px;outline:none;width:52px">
          </div>
        </div>
        <div>
          <div style="font-size:9px;color:var(--tx2);margin-bottom:3px;text-transform:uppercase">Edge% от / до</div>
          <div style="display:flex;gap:4px">
            <input id="f-edge-min" type="number" placeholder="0" step="0.5" oninput="clearTimeout(window._lt4);window._lt4=setTimeout(()=>loadBetsPage(1),600)" style="font-family:var(--mono);font-size:10px;background:var(--bg2);color:var(--tx);border:1px solid var(--line);padding:3px 4px;outline:none;width:52px">
            <input id="f-edge-max" type="number" placeholder="∞" step="0.5" oninput="clearTimeout(window._lt5);window._lt5=setTimeout(()=>loadBetsPage(1),600)" style="font-family:var(--mono);font-size:10px;background:var(--bg2);color:var(--tx);border:1px solid var(--line);padding:3px 4px;outline:none;width:52px">
          </div>
        </div>
        <div>
          <div style="font-size:9px;color:var(--tx2);margin-bottom:3px;text-transform:uppercase">Арб% от / до</div>
          <div style="display:flex;gap:4px">
            <input id="f-arb-min" type="number" placeholder="0" step="0.5" oninput="clearTimeout(window._lt6);window._lt6=setTimeout(()=>loadBetsPage(1),600)" style="font-family:var(--mono);font-size:10px;background:var(--bg2);color:var(--tx);border:1px solid var(--line);padding:3px 4px;outline:none;width:52px">
            <input id="f-arb-max" type="number" placeholder="∞" step="0.5" oninput="clearTimeout(window._lt7);window._lt7=setTimeout(()=>loadBetsPage(1),600)" style="font-family:var(--mono);font-size:10px;background:var(--bg2);color:var(--tx);border:1px solid var(--line);padding:3px 4px;outline:none;width:52px">
          </div>
        </div>
        <div>
          <div style="font-size:9px;color:var(--tx2);margin-bottom:3px;text-transform:uppercase">Режим</div>
          <select id="f-mode" onchange="loadBetsPage(1)" style="font-family:var(--mono);font-size:10px;background:var(--bg2);color:var(--tx);border:1px solid var(--line);padding:3px 6px;outline:none">
            <option value="">Все</option>
            <option value="prematch">📈 Прематч</option>
            <option value="live">\u26a1 Лайв</option>
            <option value="resell">🔄 Resell</option>
          </select>
        </div>
        <div style="margin-left:auto;display:flex;gap:6px;align-items:flex-end">
          <button onclick="resetFilters()" style="padding:3px 10px;background:var(--bg3);color:var(--tx2);border:1px solid var(--line);font-family:var(--mono);font-size:9px;cursor:pointer">\u2715 СБРОС</button>
        </div>
      </div>

      <div id="bets-stats-bar" style="padding:6px 14px;font-size:10px;color:var(--tx2);border-bottom:1px solid var(--line);display:flex;gap:16px;flex-wrap:wrap">
        <span>Найдено: <b id="bs-total" style="color:var(--tx)">—</b></span>
        <span>Ошибка ставки: <b id="bs-failed" style="color:#e74c3c">—</b></span>
        <span>Ожидает: <b id="bs-pending" style="color:#f39c12">—</b></span>
        <span>Рассчитано: <b id="bs-settled" style="color:#3498db">—</b></span>
        <span style="border-left:1px solid var(--line);padding-left:16px">Оборот (расч.): <b id="bs-settled-vol" style="color:#e74c3c">—</b></span>
        <span>В/П (winrate): <b id="bs-won" style="color:#00e87a">—</b></span>
        <span>ROI: <b id="bs-roi">—</b></span>
        <span>P&L: <b id="bs-pnl">—</b></span>
        <span>Avg Edge: <b id="bs-avg-edge" style="color:var(--tx)">—</b></span>
        <span>Avg Odds: <b id="bs-avg-odds" style="color:var(--tx)">—</b></span>
        <span id="bs-resell-wrap" style="display:none;border-left:1px solid var(--line);padding-left:16px">
          🔄 Продано: <b id="bs-resold" style="color:#ffb800">0</b> |
          На продаже: <b id="bs-onsale" style="color:var(--y)">0</b> |
          Истекло: <b id="bs-rs-expired" style="color:var(--tx2)">0</b> |
          Avg Markup: <b id="bs-rs-markup" style="color:#ffb800">—</b> |
          Resell P&L: <b id="bs-rs-pnl" style="color:var(--g)">—</b>
        </span>
      </div>

      <div class="tw">
        <table>
          <thead><tr>
            <th>#</th>
            <th>Дата</th>
            <th>Событие</th>
            <th>Исход</th>
            <th title="Edge от BetBurger">Edge%</th>
            <th title="Коэффициент">Коэф</th>
            <th title="Ликвидность рынка на момент ставки" style="cursor:help;border-bottom:1px dashed var(--tx2)">Ликв $</th>
            <th title="Реально потрачено USDC = shares × цена входа" style="cursor:help;border-bottom:1px dashed var(--tx2)">Ставка $</th>
            <th title="Получим при победе = shares × $1" style="cursor:help;border-bottom:1px dashed var(--tx2)">Выигрыш $</th>
            <th>Order ID</th>
            <th>Статус</th>
            <th>Режим</th>
            <th>Результат</th>
            <th>P&L $</th>
            <th title="Движение линии после ставки" style="cursor:help;border-bottom:1px dashed var(--tx2)">Линия</th>
            <th>Матч</th>
            <th>Расчёт</th>
          </tr></thead>
          <tbody id="tb-bets"></tbody>
        </table>
      </div>

      <!-- Pagination -->
      <div style="padding:10px 14px;display:flex;gap:8px;align-items:center;border-top:1px solid var(--line)">
        <button id="pg-prev" onclick="changeBetsPage(-1)" style="padding:4px 12px;background:var(--bg2);color:var(--tx);border:1px solid var(--line);font-family:var(--mono);font-size:10px;cursor:pointer;opacity:0.4">◀ ПРЕД</button>
        <span id="pg-info" style="font-size:10px;color:var(--tx2);flex:1;text-align:center">—</span>
        <select id="pg-size" onchange="loadBetsPage(1)" style="font-family:var(--mono);font-size:10px;background:var(--bg2);color:var(--tx);border:1px solid var(--line);padding:3px 6px;outline:none">
          <option value="50">50 / стр</option>
          <option value="100">100 / стр</option>
          <option value="200">200 / стр</option>
        </select>
        <button id="pg-next" onclick="changeBetsPage(1)" style="padding:4px 12px;background:var(--bg2);color:var(--tx);border:1px solid var(--line);font-family:var(--mono);font-size:10px;cursor:pointer;opacity:0.4">СЛЕД ▶</button>
      </div>
    </div>
  </div>

  <!-- LOG PAGE -->
  <div class="page" id="page-log">
    <div class="pnl">
      <div class="ph">
        <span class="pt"><span>▸</span>Лог бота</span>
        <div style="display:flex;gap:10px;align-items:center">
          <label style="font-size:9px;color:var(--tx2);display:flex;align-items:center;gap:5px;cursor:pointer">
            <input type="checkbox" id="log-auto" checked style="accent-color:var(--g)"> АВТОСКРОЛЛ
          </label>
          <button class="btn btn-g" onclick="loadLog()" style="font-size:9px;padding:4px 10px">↻</button>
        </div>
      </div>
      <div class="logbox" id="logbox"></div>
    </div>
  </div>

  <!-- PORTFOLIO PAGE -->
  <!-- ── SIGNALS PAGE ─────────────────────────────────────────────────── -->
  <div class="page" id="page-signals">
    <div class="ph" style="display:flex;align-items:center;justify-content:space-between;margin-bottom:16px">
      <span style="color:var(--g1);font-size:13px;letter-spacing:2px">⚡ СИГНАЛЫ BETBURGER — ПОСЛЕДНИЙ ТИК</span>
      <button onclick="loadSignals()" style="padding:7px 18px;background:#00e87a;color:#000;border:none;font-family:monospace;font-size:12px;font-weight:900;letter-spacing:1px;cursor:pointer;border-radius:2px;box-shadow:0 0 8px #00e87a55">↻ ОБНОВИТЬ</button>
    </div>

    <div id="signals-meta" style="display:flex;gap:12px;margin-bottom:16px;flex-wrap:wrap"></div>

    <!-- Edge диагностика -->
    <div style="margin-bottom:16px;background:#080808;border:1px solid #1a2a1a;border-radius:3px;padding:14px 16px">
      <div style="color:#00e87a;font-size:10px;letter-spacing:1px;margin-bottom:10px">🎯 ДИАГНОСТИКА EDGE (источник value%)</div>
      <div id="signals-edge-summary"><div style="color:#333;font-size:11px;font-family:monospace">— нажми ОБНОВИТЬ —</div></div>
    </div>

    <!-- Bet keys reference -->
    <div style="margin-bottom:16px">
      <div style="color:#555;font-size:10px;letter-spacing:1px;margin-bottom:6px">ВСЕ ПОЛЯ BET-ОБЪЕКТА</div>
      <div id="signals-bet-keys" style="font-family:monospace;font-size:11px;color:#3498db;line-height:1.8;word-break:break-all"></div>
    </div>
    <div style="margin-bottom:20px">
      <div style="color:#555;font-size:10px;letter-spacing:1px;margin-bottom:6px">ВСЕ ПОЛЯ ARB-ОБЪЕКТА</div>
      <div id="signals-arb-keys" style="font-family:monospace;font-size:11px;color:#9b59b6;line-height:1.8;word-break:break-all"></div>
    </div>

    <!-- Sample cards -->
    <div style="color:#555;font-size:10px;letter-spacing:1px;margin-bottom:10px">ПРИМЕРЫ СИГНАЛОВ (POLYMARKET)</div>
    <div id="signals-cards" style="display:flex;flex-direction:column;gap:16px">
      <div style="color:#555;text-align:center;padding:40px">Нажмите ОБНОВИТЬ для загрузки</div>
    </div>
  </div>

  <!-- ── FEED PAGE ─────────────────────────────────────────────────────── -->
  <div class="page" id="page-feed">
    <div class="ph" style="display:flex;align-items:center;justify-content:space-between;margin-bottom:16px">
      <span style="color:var(--g1);font-size:13px;letter-spacing:2px">📡 ФИД BETBURGER — ВСЕ СИГНАЛЫ</span>
      <div style="display:flex;gap:8px;align-items:center">
        <span id="feed-saved-at" style="font-size:10px;color:var(--tx3);font-family:var(--mono)"></span>
        <button onclick="loadFeed()" style="padding:7px 18px;background:#00e87a;color:#000;border:none;font-family:monospace;font-size:12px;font-weight:900;letter-spacing:1px;cursor:pointer;border-radius:2px;box-shadow:0 0 8px #00e87a55">↻ ОБНОВИТЬ</button>
      </div>
    </div>
    <!-- Tabs -->
    <div style="display:flex;gap:0;margin-bottom:16px;border-bottom:1px solid var(--line)">
      <button id="feed-tab-pre" onclick="feedTab('pre')"
        style="padding:8px 22px;background:#00e87a22;color:#00e87a;border:none;border-bottom:2px solid #00e87a;font-family:monospace;font-size:11px;font-weight:900;letter-spacing:1px;cursor:pointer">
        ⚡ ПРЕМАТЧ
      </button>
      <button id="feed-tab-live" onclick="feedTab('live')"
        style="padding:8px 22px;background:transparent;color:var(--tx3);border:none;border-bottom:2px solid transparent;font-family:monospace;font-size:11px;font-weight:900;letter-spacing:1px;cursor:pointer">
        🔴 ЛАЙВ
      </button>
    </div>
    <!-- Stats bar -->
    <div id="feed-stats" style="display:flex;gap:12px;margin-bottom:14px;flex-wrap:wrap"></div>
    <!-- List -->
    <div id="feed-list" style="display:flex;flex-direction:column;gap:6px">
      <div style="color:#555;text-align:center;padding:60px;font-family:monospace;font-size:12px">Нажмите ОБНОВИТЬ для загрузки</div>
    </div>
  </div>

  <!-- Backlog / Kanban -->
  <div class="page" id="page-backlog">
    <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:16px">
      <h2 style="margin:0;font-size:16px;color:#eee">Backlog</h2>
      <button class="savebtn" style="padding:4px 14px" onclick="blAddCard()">+ New</button>
    </div>
    <div id="bl-board" style="display:grid;grid-template-columns:repeat(4,1fr);gap:12px;align-items:start">
      <div class="pnl" style="min-height:300px">
        <div class="ph"><span class="pt" style="color:#3498db">Ideas</span></div>
        <div class="pb" id="bl-col-idea" style="min-height:200px;padding:6px"></div>
      </div>
      <div class="pnl" style="min-height:300px">
        <div class="ph"><span class="pt" style="color:#e67e22">Todo</span></div>
        <div class="pb" id="bl-col-todo" style="min-height:200px;padding:6px"></div>
      </div>
      <div class="pnl" style="min-height:300px">
        <div class="ph"><span class="pt" style="color:#9b59b6">In Progress</span></div>
        <div class="pb" id="bl-col-progress" style="min-height:200px;padding:6px"></div>
      </div>
      <div class="pnl" style="min-height:300px">
        <div class="ph"><span class="pt" style="color:#2ecc71">Done</span></div>
        <div class="pb" id="bl-col-done" style="min-height:200px;padding:6px"></div>
      </div>
    </div>
  </div>

  <!-- Settlement Sniper -->
  <div class="page" id="page-snipe">
    <div style="display:flex;align-items:center;gap:12px;margin-bottom:16px">
      <h2 style="margin:0;font-size:16px;color:#eee">Settlement Sniper</h2>
      <button class="savebtn" style="padding:3px 12px;font-size:10px" id="snipe-start"
        onclick="snipeToggle()">START</button>
      <button class="savebtn" style="padding:3px 12px;font-size:10px" id="snipe-mode"
        onclick="snipeModeToggle()">MANUAL</button>
      <span id="snipe-status" style="color:#555;font-size:11px">Stopped</span>
    </div>

    <!-- Stats -->
    <div style="display:grid;grid-template-columns:repeat(5,1fr);gap:10px;margin-bottom:16px">
      <div class="pnl"><div class="pb" style="text-align:center">
        <div style="font-size:18px;color:var(--g)" id="sn-active">0</div>
        <div style="font-size:9px;color:#666">Active</div>
      </div></div>
      <div class="pnl"><div class="pb" style="text-align:center">
        <div style="font-size:18px;color:#eee" id="sn-scanned">0</div>
        <div style="font-size:9px;color:#666">Scanned</div>
      </div></div>
      <div class="pnl"><div class="pb" style="text-align:center">
        <div style="font-size:18px;color:#3498db" id="sn-sniped">0</div>
        <div style="font-size:9px;color:#666">Sniped</div>
      </div></div>
      <div class="pnl"><div class="pb" style="text-align:center">
        <div style="font-size:18px;color:#e67e22" id="sn-settled">0</div>
        <div style="font-size:9px;color:#666">Settled</div>
      </div></div>
      <div class="pnl"><div class="pb" style="text-align:center">
        <div style="font-size:18px" id="sn-profit">$0</div>
        <div style="font-size:9px;color:#666">Profit</div>
      </div></div>
    </div>

    <!-- Active snipes -->
    <div class="pnl" style="margin-bottom:16px">
      <div class="ph"><span class="pt">Active Snipes</span></div>
      <div class="pb" style="padding:0;overflow-x:auto">
        <table class="bt" style="width:100%">
          <thead><tr>
            <th>Market</th><th>Side</th><th>Price</th><th>Shares</th><th>Cost</th>
            <th>Target Profit</th><th>Status</th>
          </tr></thead>
          <tbody id="sn-tbody"></tbody>
        </table>
        <div id="sn-empty" style="text-align:center;padding:20px;color:#555;font-size:12px">
          No active snipes. Start the bot to scan markets.
        </div>
      </div>
    </div>

    <!-- Candidates (manual mode) -->
    <div class="pnl" style="margin-bottom:16px" id="sn-candidates-panel">
      <div class="ph" style="display:flex;align-items:center;justify-content:space-between">
        <span class="pt" style="color:#e67e22">Candidates <span id="sn-cand-count" style="font-size:10px;color:#888">(0)</span></span>
        <button class="savebtn" style="padding:2px 10px;font-size:9px;background:#c0392b" onclick="snipeRejectAll()">SKIP ALL</button>
      </div>
      <!-- Filter bar -->
      <div style="padding:8px 12px;border-bottom:1px solid #1a1a1a;display:flex;flex-wrap:wrap;gap:6px;align-items:flex-end">
        <div>
          <div style="font-size:9px;color:#555;margin-bottom:2px">Search</div>
          <input id="snf-q" type="text" placeholder="market name…" oninput="renderCands()"
            class="sinput" style="width:130px;padding:3px 6px;font-size:10px">
        </div>
        <div>
          <div style="font-size:9px;color:#555;margin-bottom:2px">Tag / Category</div>
          <select id="snf-sport" onchange="renderCands()"
            class="sinput" style="padding:3px 4px;font-size:10px">
            <option value="">All</option>
            <option>nba</option><option>nhl</option><option>tennis</option>
            <option>soccer</option><option>mlb</option><option>mma</option><option>nfl</option>
            <option>golf</option><option>politics</option><option>crypto</option>
            <option>pop-culture</option>
          </select>
        </div>
        <div>
          <div style="font-size:9px;color:#555;margin-bottom:2px">Side</div>
          <select id="snf-side" onchange="renderCands()"
            class="sinput" style="padding:3px 4px;font-size:10px">
            <option value="">All</option>
            <option>YES</option><option>NO</option>
          </select>
        </div>
        <div>
          <div style="font-size:9px;color:#555;margin-bottom:2px">Min Price</div>
          <input id="snf-pmin" type="number" step="0.01" min="0" max="1" placeholder="0.95"
            oninput="renderCands()" class="sinput" style="width:60px;padding:3px 5px;font-size:10px">
        </div>
        <div>
          <div style="font-size:9px;color:#555;margin-bottom:2px">Max Price</div>
          <input id="snf-pmax" type="number" step="0.01" min="0" max="1" placeholder="0.999"
            oninput="renderCands()" class="sinput" style="width:60px;padding:3px 5px;font-size:10px">
        </div>
        <div>
          <div style="font-size:9px;color:#555;margin-bottom:2px">Min Profit %</div>
          <input id="snf-profmin" type="number" step="0.1" min="0" placeholder="0"
            oninput="renderCands()" class="sinput" style="width:60px;padding:3px 5px;font-size:10px">
        </div>
        <div>
          <div style="font-size:9px;color:#555;margin-bottom:2px">Ends within (h)</div>
          <input id="snf-endh" type="number" step="1" min="0" placeholder="72"
            oninput="renderCands()" class="sinput" style="width:60px;padding:3px 5px;font-size:10px">
        </div>
        <div>
          <div style="font-size:9px;color:#555;margin-bottom:2px">Sort by</div>
          <select id="snf-sort" onchange="renderCands()"
            class="sinput" style="padding:3px 4px;font-size:10px">
            <option value="profit_pct">Profit %</option>
            <option value="price">Price</option>
            <option value="ends">Ends soonest</option>
            <option value="age">Age</option>
          </select>
        </div>
        <button class="savebtn" onclick="snfReset()" style="padding:3px 10px;font-size:9px;background:#333;margin-bottom:0">✕ Reset</button>
      </div>
      <div style="padding:0;overflow-x:auto">
        <table class="bt" style="width:100%">
          <thead><tr>
            <th>Market</th><th>Side</th><th>Price</th><th>Profit %</th>
            <th>Exp. Profit</th><th>Tag</th><th>Ends In</th><th>Age</th><th></th>
          </tr></thead>
          <tbody id="sn-cand-tbody"></tbody>
        </table>
        <div id="sn-cand-empty" style="text-align:center;padding:15px;color:#555;font-size:11px">
          No candidates yet. Bot is scanning...
        </div>
      </div>
    </div>

    <!-- Settings -->
    <div class="pnl" style="margin-bottom:16px">
      <div class="ph"><span class="pt">Settings</span></div>
      <div class="pb">
        <div style="display:grid;grid-template-columns:repeat(4,1fr);gap:10px">
          <div><div style="color:#555;font-size:9px;margin-bottom:3px">Min Price</div>
            <input id="sn-minprice" type="number" step="0.01" min="0.90" max="0.999" class="sinput" style="width:100%" value="0.95"></div>
          <div><div style="color:#555;font-size:9px;margin-bottom:3px">Max Price</div>
            <input id="sn-maxprice" type="number" step="0.01" min="0.90" max="0.999" class="sinput" style="width:100%" value="0.995"></div>
          <div><div style="color:#555;font-size:9px;margin-bottom:3px">Order Size ($)</div>
            <input id="sn-size" type="number" step="1" min="1" class="sinput" style="width:100%" value="5"></div>
          <div><div style="color:#555;font-size:9px;margin-bottom:3px">Max Positions</div>
            <input id="sn-maxpos" type="number" step="1" min="1" class="sinput" style="width:100%" value="10"></div>
        </div>
        <div style="display:grid;grid-template-columns:repeat(3,1fr);gap:10px;margin-top:10px">
          <div><div style="color:#555;font-size:9px;margin-bottom:3px">Scan Interval (sec)</div>
            <input id="sn-interval" type="number" step="1" min="10" class="sinput" style="width:100%" value="30"></div>
          <div><div style="color:#555;font-size:9px;margin-bottom:3px">Max Days to End</div>
            <input id="sn-maxdays" type="number" step="0.5" min="0.5" max="30" class="sinput" style="width:100%" value="3"></div>
          <div><div style="color:#555;font-size:9px;margin-bottom:3px">Tags (comma-sep; <span style="color:#e67e22">all</span> = everything)</div>
            <input id="sn-tags" type="text" class="sinput" style="width:100%" value="nba,nhl,tennis,soccer,mlb,mma,nfl,golf,politics,crypto,pop-culture"></div>
        </div>
        <button class="savebtn" style="margin-top:10px" onclick="saveSnipeCfg()">SAVE</button>
      </div>
    </div>

    <!-- Log -->
    <div class="pnl">
      <div class="ph"><span class="pt">Log</span></div>
      <div class="pb" id="sn-log" style="font-family:monospace;font-size:10px;color:#888;max-height:200px;overflow-y:auto;white-space:pre-wrap">
      </div>
    </div>
  </div>

  <div class="page" id="page-portfolio">
    <div class="ph" style="display:flex;align-items:center;justify-content:space-between;margin-bottom:16px">
      <span style="color:var(--g1);font-size:13px;letter-spacing:2px">◈ ПОРТФЕЛЬ POLYMARKET</span>
      <div style="display:flex;gap:8px">
        <button onclick="loadPortfolio()" style="padding:7px 18px;background:#00e87a;color:#000000;border:none;font-family:monospace;font-size:12px;font-weight:900;letter-spacing:1px;cursor:pointer;border-radius:2px;box-shadow:0 0 8px #00e87a55">↻ ОБНОВИТЬ</button>
        <button onclick="purgePending()" style="padding:7px 18px;background:#e74c3c;color:#ffffff;border:none;font-family:monospace;font-size:12px;font-weight:900;letter-spacing:1px;cursor:pointer;border-radius:2px">🗑 БИТЫЕ PENDING</button>
      </div>
    </div>

    <!-- Summary cards -->
    <div style="display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:20px">
      <div class="card" style="position:relative">
        <div class="cl" style="color:#888;font-size:10px;letter-spacing:1px">СВОБОДНЫЙ USDC
          <span id="pm-cash-edit-btn" onclick="pmCashEditToggle()" title="Редактировать" style="cursor:pointer;margin-left:6px;color:#00e87a;font-size:11px;opacity:0.7">✎</span>
        </div>
        <div class="cv" id="pm-cash">—</div>
        <div id="pm-cash-edit-wrap" style="display:none;margin-top:6px;display:none">
          <input id="pm-cash-inp" type="number" step="0.01" min="0" placeholder="0.00"
            style="width:90px;background:#0a0a0a;border:1px solid #00e87a55;color:#00e87a;font-family:monospace;font-size:13px;padding:3px 6px;border-radius:2px"
            onkeydown="if(event.key==='Enter')pmCashSave()">
          <button onclick="pmCashSave()" style="margin-left:4px;padding:3px 8px;background:#00e87a;color:#000;border:none;font-family:monospace;font-size:11px;font-weight:900;cursor:pointer;border-radius:2px">✓</button>
          <button onclick="pmCashEditToggle()" style="margin-left:2px;padding:3px 6px;background:#333;color:#aaa;border:none;font-family:monospace;font-size:11px;cursor:pointer;border-radius:2px">✕</button>
        </div>
      </div>
      <div class="card">
        <div class="cl" style="color:#888;font-size:10px;letter-spacing:1px">СТОИМОСТЬ ПОЗИЦИЙ</div>
        <div class="cv" id="pm-portval">—</div>
      </div>
      <div class="card">
        <div class="cl" style="color:#888;font-size:10px;letter-spacing:1px">ОРДЕРА (ЗАМОРОЖЕНО)</div>
        <div class="cv" id="pm-orders">—</div>
      </div>
      <div class="card">
        <div class="cl" style="color:#888;font-size:10px;letter-spacing:1px">ИТОГО</div>
        <div class="cv" id="pm-total" style="color:var(--g1)">—</div>
      </div>
    </div>

    <!-- Positions table -->
    <div class="tbl-wrap">
      <table class="tbl">
        <thead><tr>
          <th>СОБЫТИЕ</th>
          <th>ИСХОД</th>
          <th style="text-align:right">ТОКЕНЫ</th>
          <th style="text-align:right">ЦЕНА ВХОДА</th>
          <th style="text-align:right">ТЕКУЩАЯ</th>
          <th style="text-align:right">СТОИМОСТЬ</th>
          <th style="text-align:right">P&L</th>
          <th style="text-align:right">P&L %</th>
        </tr></thead>
        <tbody id="pm-positions-body">
          <tr><td colspan="8" style="text-align:center;color:#555;padding:24px">Нажмите ОБНОВИТЬ для загрузки</td></tr>
        </tbody>
      </table>
    </div>

    <!-- Purge result -->
    <div id="purge-result" style="display:none;margin-top:16px;padding:12px;border:1px solid var(--g1);color:var(--g1);font-size:12px"></div>
  </div>

<!-- ═══════════════════════════════════════════════════════ -->
<!-- PAGE: HEDGE CALCULATOR                                    -->
<!-- ═══════════════════════════════════════════════════════ -->
<div class="page" id="page-hedge" style="padding:20px 24px">

  <!-- HEDGE CALCULATOR -->
  <div class="pnl" style="margin-bottom:14px">
    <div class="ph"><span class="pt"><span>&#9660;</span>HEDGE CALCULATOR</span></div>
    <div class="pb">
      <div style="font-size:12px;color:#999;margin-bottom:12px">
        <b>Hedge Calculator</b><br>Match + tournament positions + delta-neutral
      </div>

      <!-- Budget -->
      <div style="margin-bottom:14px">
        <div style="color:#555;font-size:9px;margin-bottom:3px;text-transform:uppercase">Budget</div>
        <div style="display:flex;align-items:center;gap:4px">
          <span style="color:#888">$</span>
          <input id="h-budget" type="number" value="10000" step="100" min="1" class="sinput" style="width:100px">
        </div>
      </div>

      <div style="display:grid;grid-template-columns:1fr 1fr;gap:16px;margin-bottom:16px">
        <!-- Position A — Match -->
        <div style="border:1px solid #2a5a3a;padding:12px;border-radius:6px">
          <div style="color:#4CAF50;font-size:11px;font-weight:700;margin-bottom:10px;text-transform:uppercase">Position A — Match</div>
          <div style="display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-bottom:8px">
            <div>
              <div style="color:#555;font-size:9px;margin-bottom:3px;text-transform:uppercase">Player / Team 1</div>
              <input id="h-a-player1" type="text" placeholder="Alcaraz" class="sinput" style="width:100%">
            </div>
            <div>
              <div style="color:#555;font-size:9px;margin-bottom:3px;text-transform:uppercase">Player / Team 2</div>
              <input id="h-a-player2" type="text" placeholder="Medvedev" class="sinput" style="width:100%">
            </div>
          </div>
          <div style="display:grid;grid-template-columns:1fr 1fr;gap:8px">
            <div>
              <div style="color:#555;font-size:9px;margin-bottom:3px;text-transform:uppercase">Side (betting on)</div>
              <input id="h-a-side" type="text" placeholder="Alcaraz" class="sinput" style="width:100%">
            </div>
            <div>
              <div style="color:#555;font-size:9px;margin-bottom:3px;text-transform:uppercase">Buy @</div>
              <div style="display:flex;align-items:center;gap:4px">
                <input id="h-a-price" type="number" step="1" min="1" max="99" value="55" class="sinput" style="width:60px">
                <span style="color:#888;font-size:11px">¢</span>
              </div>
            </div>
          </div>
          <div style="margin-top:8px">
            <div style="color:#555;font-size:9px;margin-bottom:3px;text-transform:uppercase">Token ID (optional)</div>
            <input id="h-a-token" type="text" placeholder="0x..." class="sinput" style="width:100%;font-size:10px">
          </div>
        </div>

        <!-- Position B — Tournament -->
        <div style="border:1px solid #5a3a2a;padding:12px;border-radius:6px">
          <div style="color:#FF9800;font-size:11px;font-weight:700;margin-bottom:10px;text-transform:uppercase">Position B — Tournament</div>
          <div style="margin-bottom:8px">
            <div style="color:#555;font-size:9px;margin-bottom:3px;text-transform:uppercase">Tournament / Market</div>
            <input id="h-b-tournament" type="text" placeholder="Indian Wells Winner" class="sinput" style="width:100%">
          </div>
          <div style="margin-bottom:8px">
            <div style="color:#555;font-size:9px;margin-bottom:3px;text-transform:uppercase">Player / Team</div>
            <input id="h-b-player" type="text" placeholder="Sinner" class="sinput" style="width:100%">
          </div>
          <div style="display:grid;grid-template-columns:1fr 1fr;gap:8px">
            <div>
              <div style="color:#555;font-size:9px;margin-bottom:3px;text-transform:uppercase">Side</div>
              <select id="h-b-side" class="sinput" style="width:100%">
                <option value="YES">YES</option>
                <option value="NO">NO</option>
              </select>
            </div>
            <div>
              <div style="color:#555;font-size:9px;margin-bottom:3px;text-transform:uppercase">Buy @</div>
              <div style="display:flex;align-items:center;gap:4px">
                <input id="h-b-price" type="number" step="1" min="1" max="99" value="55" class="sinput" style="width:60px">
                <span style="color:#888;font-size:11px">¢</span>
              </div>
            </div>
          </div>
          <div style="margin-top:8px">
            <div style="color:#555;font-size:9px;margin-bottom:3px;text-transform:uppercase">Token ID (optional)</div>
            <input id="h-b-token" type="text" placeholder="0x..." class="sinput" style="width:100%;font-size:10px">
          </div>
        </div>
      </div>

      <!-- Scenarios -->
      <div style="border:1px solid #333;padding:12px;border-radius:6px;margin-bottom:16px">
        <div style="color:#7C4DFF;font-size:11px;font-weight:700;margin-bottom:10px;text-transform:uppercase">Scenarios</div>
        <div id="h-scenarios">
          <!-- Scenario 1 -->
          <div class="h-scenario" style="margin-bottom:10px">
            <div style="color:#555;font-size:9px;margin-bottom:3px;text-transform:uppercase">Scenario 1</div>
            <input class="h-sc-name sinput" type="text" value="Alcaraz wins match" style="width:100%;margin-bottom:6px">
            <div style="display:grid;grid-template-columns:1fr 1fr;gap:8px">
              <div>
                <div style="color:#555;font-size:9px;margin-bottom:3px;text-transform:uppercase">Match position exits at</div>
                <div style="display:flex;align-items:center;gap:4px">
                  <input class="h-sc-exit-a sinput" type="number" step="1" min="0" max="100" value="100" style="width:60px">
                  <span style="color:#888;font-size:11px">¢</span>
                </div>
              </div>
              <div>
                <div style="color:#555;font-size:9px;margin-bottom:3px;text-transform:uppercase">Tournament position exits at</div>
                <div style="display:flex;align-items:center;gap:4px">
                  <input class="h-sc-exit-b sinput" type="number" step="1" min="0" max="100" value="45" style="width:60px">
                  <span style="color:#888;font-size:11px">¢</span>
                </div>
              </div>
            </div>
          </div>
          <!-- Scenario 2 -->
          <div class="h-scenario" style="margin-bottom:10px">
            <div style="color:#555;font-size:9px;margin-bottom:3px;text-transform:uppercase">Scenario 2</div>
            <input class="h-sc-name sinput" type="text" value="Medvedev wins match" style="width:100%;margin-bottom:6px">
            <div style="display:grid;grid-template-columns:1fr 1fr;gap:8px">
              <div>
                <div style="color:#555;font-size:9px;margin-bottom:3px;text-transform:uppercase">Match position exits at</div>
                <div style="display:flex;align-items:center;gap:4px">
                  <input class="h-sc-exit-a sinput" type="number" step="1" min="0" max="100" value="0" style="width:60px">
                  <span style="color:#888;font-size:11px">¢</span>
                </div>
              </div>
              <div>
                <div style="color:#555;font-size:9px;margin-bottom:3px;text-transform:uppercase">Tournament position exits at</div>
                <div style="display:flex;align-items:center;gap:4px">
                  <input class="h-sc-exit-b sinput" type="number" step="1" min="0" max="100" value="79" style="width:60px">
                  <span style="color:#888;font-size:11px">¢</span>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>

      <!-- Calculator Buttons -->
      <div style="display:flex;gap:8px;margin-bottom:10px">
        <button class="btn" onclick="hedgeCalc()">Calculate</button>
        <button class="btn" onclick="hedgeSave()">Save</button>
        <button class="btn" style="color:#F44336" onclick="hedgeClear()">Clear</button>
      </div>
      <!-- Scan & Analyze Buttons -->
      <div style="display:flex;gap:6px;flex-wrap:wrap;margin-bottom:16px">
        <button class="btn" style="background:#333" onclick="hedgeScan()">Scan All</button>
        <button class="btn" style="background:#1a3a2a;color:#4CAF50" onclick="hedgeScan('tennis')">🎾 Tennis</button>
        <button class="btn" style="background:#1a2a3a;color:#2196F3" onclick="hedgeScan('nba')">🏀 NBA</button>
        <button class="btn" style="background:#1a2a3a;color:#64B5F6" onclick="hedgeScan('nhl')">🏒 NHL</button>
        <button class="btn" style="background:#1a3a2a;color:#66BB6A" onclick="hedgeScan('soccer')">⚽ Soccer</button>
        <button class="btn" style="background:#2a1a1a;color:#EF9A9A" onclick="hedgeScan('mlb')">⚾ MLB</button>
        <button class="btn" style="background:#2a2a1a;color:#FDD835" onclick="hedgeScan('ncaa')">🏀 NCAA</button>
        <button class="btn" style="background:#7C4DFF" onclick="hedgeAnalyze()">Auto-Analyze</button>
      </div>

      <!-- Saved Calculations -->
      <div style="margin-bottom:16px">
        <div style="color:#555;font-size:9px;margin-bottom:6px;text-transform:uppercase">Saved Calculations</div>
        <div id="h-saved" style="font-size:12px;color:#888">—</div>
      </div>

    </div>
  </div>

  <!-- Position Sizing (результат расчёта) -->
  <div id="h-result" style="display:none">
    <div class="pnl" style="margin-bottom:14px">
      <div class="ph"><span class="pt">POSITION SIZING</span></div>
      <div class="pb">
        <div style="display:grid;grid-template-columns:1fr 1fr;gap:16px">
          <div style="border:1px solid #2a5a3a;padding:12px;border-radius:6px">
            <div id="h-res-a-label" style="color:#4CAF50;font-size:10px;margin-bottom:4px"></div>
            <div id="h-res-a-shares" style="font-size:20px;font-weight:700"></div>
            <div id="h-res-a-cost" style="color:#888;font-size:11px"></div>
          </div>
          <div style="border:1px solid #5a3a2a;padding:12px;border-radius:6px">
            <div id="h-res-b-label" style="color:#FF9800;font-size:10px;margin-bottom:4px"></div>
            <div id="h-res-b-shares" style="font-size:20px;font-weight:700"></div>
            <div id="h-res-b-cost" style="color:#888;font-size:11px"></div>
          </div>
        </div>
      </div>
    </div>

    <!-- Scenario P&L -->
    <div class="pnl" style="margin-bottom:14px">
      <div class="ph"><span class="pt">SCENARIO P&L</span></div>
      <div class="pb">
        <div id="h-res-scenarios" style="display:grid;grid-template-columns:repeat(auto-fit,minmax(200px,1fr));gap:12px"></div>
        <div id="h-res-note" style="margin-top:8px;padding:6px 10px;border-radius:4px;font-size:11px"></div>
      </div>
    </div>
  </div>

  <!-- Auto-Analyzed Opportunities -->
  <div class="pnl" style="margin-bottom:14px">
    <div class="ph"><span class="pt">OPPORTUNITIES (AUTO-ANALYZED)</span></div>
    <div class="pb">
      <div id="h-opportunities" style="font-size:12px;color:#888">Click "Auto-Analyze (Bayesian)" to scan for delta-neutral opportunities</div>
    </div>
  </div>

  <!-- Active Hedge Positions -->
  <div class="pnl" style="margin-bottom:14px">
    <div class="ph"><span class="pt">ACTIVE HEDGES</span></div>
    <div class="pb">
      <div id="h-positions" style="font-size:12px;color:#888">—</div>
    </div>
  </div>

  <!-- Discovered Pairs -->
  <div class="pnl" style="margin-bottom:14px">
    <div class="ph"><span class="pt">DISCOVERED PAIRS</span></div>
    <div class="pb">
      <div id="h-pairs" style="font-size:12px;color:#888">—</div>
    </div>
  </div>

</div>

<!-- ═══════════════════════════════════════════════════════ -->
<!-- PAGE: MARKET MAKING                                     -->
<!-- ═══════════════════════════════════════════════════════ -->
<div class="page" id="page-mm" style="padding:20px 24px">

  <div style="display:flex;align-items:center;gap:16px;margin-bottom:16px">
    <h2 style="margin:0;font-size:16px;color:#eee">&#9878; Market Making</h2>
    <button class="savebtn" style="padding:4px 14px;font-size:11px" onclick="startMM()">СТАРТ</button>
    <button class="savebtn" style="padding:4px 14px;font-size:11px;background:#c0392b" onclick="stopMM()">СТОП</button>
    <span id="mm-status" style="font-size:11px;color:#666">--</span>
  </div>

  <!-- Stats -->
  <div style="display:grid;grid-template-columns:repeat(8,1fr);gap:8px;margin-bottom:16px">
    <div class="pnl"><div class="pb" style="text-align:center">
      <div style="font-size:16px;color:var(--g)" id="mm-s-markets">0</div>
      <div style="font-size:9px;color:#666">Маркетов</div>
    </div></div>
    <div class="pnl"><div class="pb" style="text-align:center">
      <div style="font-size:16px;color:#eee" id="mm-s-fills">0</div>
      <div style="font-size:9px;color:#666">Fills</div>
    </div></div>
    <div class="pnl"><div class="pb" style="text-align:center">
      <div style="font-size:16px;color:#e74c3c" id="mm-s-spent">$0</div>
      <div style="font-size:9px;color:#666">Потрачено</div>
    </div></div>
    <div class="pnl"><div class="pb" style="text-align:center">
      <div style="font-size:16px;color:#2ecc71" id="mm-s-received">$0</div>
      <div style="font-size:9px;color:#666">Получено</div>
    </div></div>
    <div class="pnl"><div class="pb" style="text-align:center">
      <div style="font-size:16px" id="mm-s-pnl">$0</div>
      <div style="font-size:9px;color:#666">Total P&L</div>
    </div></div>
    <div class="pnl"><div class="pb" style="text-align:center">
      <div style="font-size:16px;color:#3498db" id="mm-s-paired">$0</div>
      <div style="font-size:9px;color:#666" title="Paired YES+NO shares = гарантированный $1 payout за пару">Paired Value</div>
    </div></div>
    <div class="pnl"><div class="pb" style="text-align:center">
      <div style="font-size:16px;color:#9b59b6" id="mm-s-spread">0%</div>
      <div style="font-size:9px;color:#666" title="Средняя маржа между bid и ask fills">Avg Margin</div>
    </div></div>
    <div class="pnl"><div class="pb" style="text-align:center">
      <div style="font-size:16px;color:#e67e22" id="mm-s-exposure">$0</div>
      <div style="font-size:9px;color:#666" title="Непарные shares = направленный риск">Exposure</div>
    </div></div>
    <div class="pnl"><div class="pb" style="text-align:center;padding:8px">
      <button class="savebtn" style="background:#c0392b;padding:4px 12px;font-size:10px"
        onclick="if(confirm('Очистить всю MM статистику?')) mmClearStats()">ОЧИСТИТЬ</button>
      <div style="font-size:9px;color:#666;margin-top:4px">Reset Stats</div>
    </div></div>
  </div>

  <!-- Active Markets -->
  <div class="pnl" style="margin-bottom:16px">
    <div class="ph"><span class="pt">&#9878; Active Markets</span></div>
    <div class="pb" style="padding:0;overflow-x:auto">
      <table class="bt" style="width:100%">
        <thead><tr>
          <th>Event / Market</th><th>Mid</th><th>Bids</th><th>Asks</th>
          <th>YES</th><th>NO</th><th>Cost</th><th>Fills</th><th>Margin</th><th>If YES</th><th>If NO</th><th></th>
        </tr></thead>
        <tbody id="mm-markets-tbody"></tbody>
      </table>
      <div id="mm-no-markets" style="text-align:center;padding:20px;color:#555;font-size:12px">
        No active markets. Use search below to add.
      </div>
    </div>
  </div>

  <!-- Add Market -->
  <div class="pnl" style="margin-bottom:16px">
    <div class="ph"><span class="pt">+ Add Market</span></div>
    <div class="pb">
      <div style="display:flex;gap:8px;margin-bottom:10px">
        <input id="mm-search-q" type="text" class="sinput" style="flex:1" placeholder="Search event name, or paste Polymarket URL / condition_id">
        <select id="mm-search-sport" class="sinput" style="width:140px">
          <option value="">All sports</option>
          <option value="soccer">Soccer</option>
          <option value="epl">EPL</option>
          <option value="la-liga">La Liga</option>
          <option value="serie-a">Serie A</option>
          <option value="champions-league">Champions League</option>
          <option value="nba">NBA</option>
          <option value="nhl">NHL</option>
          <option value="tennis">Tennis</option>
          <option value="mma">MMA / UFC</option>
          <option value="mlb">MLB</option>
          <option value="nfl">NFL</option>
          <option value="cricket">Cricket</option>
          <option value="ncaa">NCAAB / March Madness</option>
          <option value="esports">Esports (all)</option>
          <option value="cs2">CS2</option>
          <option value="league-of-legends">League of Legends</option>
          <option value="dota">Dota 2</option>
          <option value="valorant">Valorant</option>
        </select>
        <button class="savebtn" style="padding:4px 14px" onclick="mmSearch()">SEARCH</button>
      </div>
      <div id="mm-search-results" style="max-height:200px;overflow-y:auto"></div>
    </div>
  </div>

  <!-- Settings -->
  <div class="pnl" style="margin-bottom:16px">
    <div class="ph"><span class="pt">Settings</span></div>
    <div class="pb">
      <div style="display:grid;grid-template-columns:repeat(5,1fr);gap:10px">
        <div><div style="color:#555;font-size:9px;margin-bottom:3px">Levels (per side)</div>
          <input id="mm-levels" type="number" min="1" max="10" class="sinput" style="width:100%"></div>
        <div><div style="color:#555;font-size:9px;margin-bottom:3px">Step (ticks)</div>
          <input id="mm-step" type="number" min="1" max="10" class="sinput" style="width:100%"></div>
        <div><div style="color:#555;font-size:9px;margin-bottom:3px">Order Size ($)</div>
          <input id="mm-size" type="number" min="5" step="5" class="sinput" style="width:100%"></div>
        <div><div style="color:#555;font-size:9px;margin-bottom:3px">Poll (sec)</div>
          <input id="mm-poll" type="number" min="5" step="5" class="sinput" style="width:100%"></div>
        <div><div style="color:#555;font-size:9px;margin-bottom:3px">Max Markets</div>
          <input id="mm-maxmkt" type="number" min="1" max="20" class="sinput" style="width:100%"></div>
      </div>
      <div style="display:grid;grid-template-columns:repeat(2,1fr);gap:10px;margin-top:10px">
        <div><div style="color:#555;font-size:9px;margin-bottom:3px">Skew Step (shares)</div>
          <input id="mm-skewstep" type="number" min="10" step="10" class="sinput" style="width:100%"
            title="Every N shares of imbalance = +1 tick shift. Lower = more aggressive balancing."></div>
        <div><div style="color:#555;font-size:9px;margin-bottom:3px">Skew Max (ticks)</div>
          <input id="mm-skewmax" type="number" min="0" max="10" class="sinput" style="width:100%"
            title="Maximum ticks of skew offset. 0 = no balancing."></div>
        <div><div style="color:#555;font-size:9px;margin-bottom:3px">Max Position (shares)</div>
          <input id="mm-maxpos" type="number" min="0" step="50" class="sinput" style="width:100%"
            title="Max net position imbalance. Beyond this, stop accepting on heavy side."></div>
        <div><div style="color:#555;font-size:9px;margin-bottom:3px">Spread Panic (ticks)</div>
          <input id="mm-panic" type="number" min="0" max="20" class="sinput" style="width:100%"
            title="If market spread > N ticks, cancel all orders (circuit breaker)."></div>
        <div><div style="color:#555;font-size:9px;margin-bottom:3px">Anchor</div>
          <select id="mm-anchor" class="sinput" style="width:100%"
            title="Где начинать лесенку: mid=от середины спреда, spread=от текущего best bid/ask, spread1=на 1 тик лучше best bid/ask">
            <option value="mid">Mid (агрессивно)</option>
            <option value="spread">Spread Match</option>
            <option value="spread1">Spread +1 тик</option>
          </select></div>
        <div style="display:flex;align-items:center;gap:6px;padding-top:10px">
          <input id="mm-sell" type="checkbox" style="width:15px;height:15px;cursor:pointer;accent-color:var(--g)">
          <label for="mm-sell" style="color:#888;font-size:11px;cursor:pointer"
            title="Продавать имеющиеся shares вместо покупки новых (освобождает капитал)">SELL existing shares</label>
        </div>
      </div>
      <button class="savebtn" style="margin-top:10px" onclick="saveMMCfg()">SAVE</button>
      <span class="savemsg" id="mm-savemsg"></span>
    </div>
  </div>

  <!-- Log -->
  <div class="pnl">
    <div class="ph"><span class="pt">Log</span>
      <span style="float:right;font-size:10px;color:#555;cursor:pointer" onclick="loadMMLog()">&#8635;</span>
    </div>
    <div class="pb" style="padding:0">
      <pre id="mm-log" style="margin:0;padding:8px 10px;font-size:10px;line-height:1.5;color:#aaa;background:#0a0a0a;max-height:250px;overflow-y:auto;white-space:pre-wrap;word-break:break-all">Start the bot to see logs</pre>
    </div>
  </div>

</div>

<!-- ═══════════════════════════════════════════════════════ -->
<!-- PAGE: DUTCHING                                          -->
<!-- ═══════════════════════════════════════════════════════ -->
<div class="page" id="page-dutch" style="padding:20px 24px">

  <div style="display:flex;align-items:center;gap:16px;margin-bottom:16px">
    <h2 style="margin:0;font-size:16px;color:#eee">&#9889; Dutching (Internal Arb)</h2>
    <button class="savebtn" style="padding:4px 14px;font-size:11px" onclick="startDutch()">СТАРТ</button>
    <button class="savebtn" style="padding:4px 14px;font-size:11px;background:#c0392b" onclick="stopDutch()">СТОП</button>
    <span id="dutch-status" style="font-size:11px;color:#666">—</span>
  </div>

  <!-- Stats -->
  <div style="display:grid;grid-template-columns:repeat(5,1fr);gap:10px;margin-bottom:16px">
    <div class="pnl"><div class="pb" style="text-align:center">
      <div style="font-size:18px;color:var(--g)" id="ds-pairs">0</div>
      <div style="font-size:9px;color:#666">Пар размещено</div>
    </div></div>
    <div class="pnl"><div class="pb" style="text-align:center">
      <div style="font-size:18px;color:var(--g)" id="ds-settled">0</div>
      <div style="font-size:9px;color:#666">Заполнено</div>
    </div></div>
    <div class="pnl"><div class="pb" style="text-align:center">
      <div style="font-size:18px;color:var(--g)" id="ds-active">0</div>
      <div style="font-size:9px;color:#666">Активных</div>
    </div></div>
    <div class="pnl"><div class="pb" style="text-align:center">
      <div style="font-size:18px" id="ds-profit">$0.00</div>
      <div style="font-size:9px;color:#666">Прибыль</div>
    </div></div>
    <div class="pnl"><div class="pb" style="text-align:center">
      <div style="font-size:18px;color:#aaa" id="ds-volume">$0</div>
      <div style="font-size:9px;color:#666">Объём</div>
    </div></div>
  </div>

  <!-- Pairs table -->
  <div class="pnl" style="margin-bottom:16px">
    <div class="ph"><span class="pt">Пары</span></div>
    <div class="pb" style="padding:0;overflow-x:auto">
      <table class="bt" style="width:100%">
        <thead><tr>
          <th>Событие</th><th>Стороны</th><th>Cost</th><th>Статусы</th><th>P&L</th><th>Дата</th>
        </tr></thead>
        <tbody id="dutch-tbody"></tbody>
      </table>
    </div>
  </div>

  <!-- Settings -->
  <div class="pnl">
    <div class="ph"><span class="pt">Настройки Dutching</span></div>
    <div class="pb">
      <div style="display:grid;grid-template-columns:repeat(4,1fr);gap:10px">
        <div><div style="color:#555;font-size:9px;margin-bottom:3px">Min Spread %</div>
          <input id="d-spread" type="number" step="0.1" min="0" max="20" class="sinput" style="width:100%"></div>
        <div><div style="color:#555;font-size:9px;margin-bottom:3px">Min Ликвидность $</div>
          <input id="d-liq" type="number" step="1" min="0" class="sinput" style="width:100%"></div>
        <div><div style="color:#555;font-size:9px;margin-bottom:3px">Ставка $ (на пару)</div>
          <input id="d-stake" type="number" step="0.5" min="1" class="sinput" style="width:100%"></div>
        <div><div style="color:#555;font-size:9px;margin-bottom:3px">Макс. ставка $</div>
          <input id="d-maxstake" type="number" step="1" min="1" class="sinput" style="width:100%"></div>
        <div><div style="color:#555;font-size:9px;margin-bottom:3px">Интервал (сек)</div>
          <input id="d-poll" type="number" step="5" min="10" class="sinput" style="width:100%"></div>
        <div><div style="color:#555;font-size:9px;margin-bottom:3px">TTL ордера (сек)</div>
          <input id="d-ttl" type="number" step="10" min="30" class="sinput" style="width:100%"></div>
        <div style="grid-column:span 2"><div style="color:#555;font-size:9px;margin-bottom:3px">Спорты (через запятую)</div>
          <input id="d-sports" type="text" class="sinput" style="width:100%" placeholder="tennis,nba,nhl,soccer,mlb,mma,nfl"></div>
      </div>
      <button class="savebtn" style="margin-top:12px" onclick="saveDutchCfg()">СОХРАНИТЬ</button>
      <span class="savemsg" id="d-savemsg"></span>
    </div>
  </div>

  <!-- Log -->
  <div class="pnl" style="margin-top:16px">
    <div class="ph"><span class="pt">Лог сканирования</span>
      <span style="float:right;font-size:10px;color:#555;cursor:pointer" onclick="loadDutchLog()">&#8635; обновить</span>
    </div>
    <div class="pb" style="padding:0">
      <pre id="dutch-log" style="margin:0;padding:8px 10px;font-size:10px;line-height:1.5;color:#aaa;background:#0a0a0a;max-height:300px;overflow-y:auto;white-space:pre-wrap;word-break:break-all">Лог пуст — запустите бот</pre>
    </div>
  </div>

</div>

<!-- ═══════════════════════════════════════════════════════ -->
<!-- PAGE: НАСТРОЙКИ                                         -->
<!-- ═══════════════════════════════════════════════════════ -->
<div class="page" id="page-cfg" style="padding:20px 24px">

  <div class="pnl" style="margin-bottom:14px">
    <div class="ph"><span class="pt"><span>&#9654;</span>ПРЕМАТЧ БОТ</span></div>
    <div class="pb">
      <div style="display:grid;grid-template-columns:repeat(4,1fr);gap:10px">
        <div><div style="color:#555;font-size:9px;margin-bottom:3px">Мин. Edge %</div>
          <input id="c-roi" type="number" step="0.1" min="0" max="50" class="sinput" style="width:100%;text-align:left"></div>
        <div><div style="color:#555;font-size:9px;margin-bottom:3px">Мин. ликвидность $</div>
          <input id="c-liq" type="number" step="1" min="0" class="sinput" style="width:100%;text-align:left"></div>
        <div><div style="color:#555;font-size:9px;margin-bottom:3px">Ставка % от банкролла</div>
          <input id="c-stk" type="number" step="0.1" min="0" max="100" class="sinput" style="width:100%;text-align:left"></div>
        <div><div style="color:#555;font-size:9px;margin-bottom:3px">Макс. ставка %</div>
          <input id="c-maxstk" type="number" step="0.1" min="0" max="100" class="sinput" style="width:100%;text-align:left"></div>
        <div><div style="color:#555;font-size:9px;margin-bottom:3px">Мин. сумма ставки $</div>
          <input id="c-minstk" type="number" step="0.5" min="0" class="sinput" style="width:100%;text-align:left"></div>
        <div><div style="color:#555;font-size:9px;margin-bottom:3px">Макс. коэффициент (0=нет)</div>
          <input id="c-maxodds" type="number" step="0.1" min="0" class="sinput" style="width:100%;text-align:left"></div>
        <div><div style="color:#555;font-size:9px;margin-bottom:3px">Макс. Edge % (0=нет)</div>
          <input id="c-maxedge" type="number" step="0.5" min="0" class="sinput" style="width:100%;text-align:left" placeholder="напр. 15"></div>
        <div><div style="color:#555;font-size:9px;margin-bottom:3px">Интервал опроса (сек)</div>
          <input id="c-poll" type="number" step="1" min="1" class="sinput" style="width:100%;text-align:left"></div>
        <div style="display:flex;align-items:center;gap:8px;padding-top:14px">
          <input id="c-kelly" type="checkbox" style="width:15px;height:15px;cursor:pointer;accent-color:var(--g)">
          <label for="c-kelly" style="color:#888;font-size:11px;cursor:pointer">Half-Kelly</label>
        </div>
        <div style="display:flex;align-items:center;gap:8px;padding-top:6px">
          <input id="c-fulllimit" type="checkbox" style="width:15px;height:15px;cursor:pointer;accent-color:var(--g)">
          <label for="c-fulllimit" style="color:#888;font-size:11px;cursor:pointer" title="Ставить лимитку на всю сумму, не урезая по доступной ликвидности. Ордер будет ждать заполнения по TTL.">Full Limit (вся сумма)</label>
        </div>
      </div>
    </div>
  </div>

  <div class="pnl" style="margin-bottom:14px;border-color:#2a1a00">
    <div class="ph" style="border-color:#2a1a00"><span class="pt" style="color:#e67e22"><span>&#9654;</span>ЛАЙВ БОТ</span></div>
    <div class="pb">
      <div style="display:grid;grid-template-columns:repeat(5,1fr);gap:10px">
        <div><div style="color:#555;font-size:9px;margin-bottom:3px">Мин. Edge %</div>
          <input id="lv-roi" type="number" step="0.1" min="0" max="50" class="sinput" style="width:100%;text-align:left;border-color:#2a1a00"></div>
        <div><div style="color:#555;font-size:9px;margin-bottom:3px">Мин. ликвидность $</div>
          <input id="lv-liq" type="number" step="1" min="0" class="sinput" style="width:100%;text-align:left;border-color:#2a1a00"></div>
        <div><div style="color:#555;font-size:9px;margin-bottom:3px">Ставка %</div>
          <input id="lv-stk" type="number" step="0.1" min="0" max="100" class="sinput" style="width:100%;text-align:left;border-color:#2a1a00"></div>
        <div><div style="color:#555;font-size:9px;margin-bottom:3px">Макс. ставка %</div>
          <input id="lv-maxstk" type="number" step="0.1" min="0" max="100" class="sinput" style="width:100%;text-align:left;border-color:#2a1a00"></div>
        <div><div style="color:#555;font-size:9px;margin-bottom:3px">Мин. сумма ставки $</div>
          <input id="lv-minstk" type="number" step="0.5" min="0" class="sinput" style="width:100%;text-align:left;border-color:#2a1a00"></div>
        <div><div style="color:#555;font-size:9px;margin-bottom:3px">TTL ордера (сек)</div>
          <input id="lv-ttl" type="number" step="5" min="10" max="300" class="sinput" style="width:100%;text-align:left;border-color:#2a1a00;color:#e67e22"></div>
        <div><div style="color:#555;font-size:9px;margin-bottom:3px">Макс. коэф (0=нет)</div>
          <input id="lv-maxodds" type="number" step="0.1" min="0" class="sinput" style="width:100%;text-align:left;border-color:#2a1a00"></div>
        <div><div style="color:#555;font-size:9px;margin-bottom:3px">Макс. Edge % (0=нет)</div>
          <input id="lv-maxedge" type="number" step="0.5" min="0" class="sinput" style="width:100%;text-align:left;border-color:#2a1a00" placeholder="напр. 15"></div>
        <div><div style="color:#555;font-size:9px;margin-bottom:3px">Опрос (сек)</div>
          <input id="lv-poll" type="number" step="1" min="1" class="sinput" style="width:100%;text-align:left;border-color:#2a1a00"></div>
        <div><div style="color:#555;font-size:9px;margin-bottom:3px">Filter ID лайв</div>
          <input id="lv-fid" type="number" step="1" min="0" class="sinput" style="width:100%;text-align:left;border-color:#2a1a00;color:#e67e22"></div>
        <div style="display:flex;align-items:center;gap:8px;padding-top:14px">
          <input id="lv-kelly" type="checkbox" style="width:15px;height:15px;cursor:pointer;accent-color:#e67e22">
          <label for="lv-kelly" style="color:#888;font-size:11px;cursor:pointer">Half-Kelly</label>
        </div>
      </div>
    </div>
  </div>

  <div class="pnl" style="margin-bottom:14px;border-color:#3d2e00">
    <div class="ph" style="border-color:#3d2e00"><span class="pt" style="color:#ffb800"><span>&#9654;</span>АВТО-ПРОДАЖА (RESELL)</span></div>
    <div class="pb">
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:16px">
        <div style="background:var(--bg3);border:1px solid #2a2000;border-radius:4px;padding:12px">
          <div style="display:flex;align-items:center;gap:8px;margin-bottom:10px">
            <input id="c-vb-resell" type="checkbox" style="width:15px;height:15px;cursor:pointer;accent-color:#ffb800">
            <label for="c-vb-resell" style="color:var(--tx);font-size:11px;cursor:pointer;font-weight:700">Прематч resell</label>
          </div>
          <div style="display:grid;grid-template-columns:1fr 1fr;gap:8px">
            <div><div style="color:#555;font-size:9px;margin-bottom:3px">Наценка %</div>
              <input id="c-vb-markup" type="number" step="0.5" min="0.5" max="20" value="2" class="sinput" style="width:100%;text-align:left;border-color:#3d2e00;color:#ffb800"></div>
            <div><div style="color:#555;font-size:9px;margin-bottom:3px">Fallback</div>
              <select id="c-vb-fallback" class="sinput" style="width:100%;text-align:left;border-color:#3d2e00;padding:6px 8px">
                <option value="keep">keep (оставить)</option>
                <option value="market_sell">market_sell</option>
              </select></div>
          </div>
        </div>
        <div style="background:var(--bg3);border:1px solid #2a2000;border-radius:4px;padding:12px">
          <div style="display:flex;align-items:center;gap:8px;margin-bottom:10px">
            <input id="c-lv-resell" type="checkbox" style="width:15px;height:15px;cursor:pointer;accent-color:#ffb800">
            <label for="c-lv-resell" style="color:#e67e22;font-size:11px;cursor:pointer;font-weight:700">Лайв resell</label>
          </div>
          <div style="display:grid;grid-template-columns:1fr 1fr;gap:8px">
            <div><div style="color:#555;font-size:9px;margin-bottom:3px">Наценка %</div>
              <input id="c-lv-markup" type="number" step="0.5" min="0.5" max="20" value="3" class="sinput" style="width:100%;text-align:left;border-color:#3d2e00;color:#ffb800"></div>
            <div><div style="color:#555;font-size:9px;margin-bottom:3px">Fallback</div>
              <select id="c-lv-fallback" class="sinput" style="width:100%;text-align:left;border-color:#3d2e00;padding:6px 8px">
                <option value="keep">keep (оставить)</option>
                <option value="market_sell">market_sell</option>
              </select></div>
          </div>
        </div>
      </div>
      <div style="margin-top:10px;max-width:220px">
        <div style="color:#555;font-size:9px;margin-bottom:3px">TTL SELL-ордера (сек)</div>
        <input id="c-pm-ttl" type="number" step="60" min="60" max="86400" value="3600" class="sinput" style="width:100%;text-align:left;border-color:#3d2e00">
      </div>
    </div>
  </div>

  <div class="pnl" style="margin-bottom:14px;border-color:#1a1a2e">
    <div class="ph" style="border-color:#1a1a2e"><span class="pt" style="color:#e74c3c"><span>&#9654;</span>ФИЛЬТРЫ СПОРТОВ</span></div>
    <div class="pb">
      <div style="margin-bottom:10px">
        <div style="color:#555;font-size:9px;margin-bottom:3px">Исключить виды спорта (sport_id через запятую)</div>
        <input id="c-excl-sports" type="text" class="sinput" style="width:100%;text-align:left;border-color:#1a1a2e"
          placeholder="47,48,21 (47=CS2, 48=LoL, 21=E-Sports...)">
      </div>
      <div style="margin-bottom:10px">
        <div style="color:#555;font-size:9px;margin-bottom:3px">Исключить лиги (подстроки через запятую)</div>
        <input id="c-excl-leagues" type="text" class="sinput" style="width:100%;text-align:left;border-color:#1a1a2e"
          placeholder="Counter-Strike,ESL Challenger,Valorant">
      </div>
      <div style="margin-bottom:10px;max-width:220px">
        <div style="color:#555;font-size:9px;margin-bottom:3px">Макс. карта в киберспорте (0=без лимита)</div>
        <input id="c-maxmap" type="number" step="1" min="0" max="10" value="3" class="sinput" style="width:100%;text-align:left;border-color:#1a1a2e"
          title="Карта 4+ блокируется (защита от cancel-арба). 0 = без лимита, 3 = разрешены карты 1-3">
      </div>
      <div style="color:#444;font-size:9px;line-height:1.5">
        <b>Sport ID:</b>
        1=Baseball, 2=Basketball, 7=Soccer, 8=Tennis, 6=Hockey, 10=Am.Football, 18=Martial Arts, 45=MMA, 24=Cricket<br>
        <b>E-Sports:</b>
        21=Other, 39=E-Soccer, 41=E-Basketball, 46=Dota2, 47=CS2, 48=LoL, 51=Valorant, 52=Overwatch, 55=R6, 57=CoD
      </div>
    </div>
  </div>

  <div class="pnl" style="margin-bottom:14px">
    <div class="ph"><span class="pt"><span>&#9654;</span>АККАУНТЫ</span></div>
    <div class="pb">
      <div style="display:grid;grid-template-columns:repeat(3,1fr);gap:10px">
        <div><div style="color:#555;font-size:9px;margin-bottom:3px">BetBurger Email</div>
          <input id="c-email" type="email" class="sinput" style="width:100%;text-align:left"></div>
        <div><div style="color:#555;font-size:9px;margin-bottom:3px">Polymarket Funder (адрес)</div>
          <input id="c-funder" type="text" class="sinput" style="width:100%;text-align:left"></div>
        <div><div style="color:#555;font-size:9px;margin-bottom:3px">Filter ID прематч (BetBurger)</div>
          <input id="c-fid" type="number" step="1" min="0" class="sinput" style="width:100%;text-align:left"></div>
      </div>
    </div>
  </div>

  <div style="display:flex;align-items:center;gap:12px;margin-top:6px">
    <button onclick="saveCfg()" class="savebtn">СОХРАНИТЬ</button>
    <span id="savemsg" class="savemsg">Сохранено</span>
  </div>
</div>

</main>

<!-- RIGHT PANEL -->
<aside class="right">
  <div class="rps">
    <div class="rpt">▦ По видам спорта</div>
    <div id="sport-list"><div class="nodata">Загрузка...</div></div>
  </div>
  <div class="rps">
    <div class="rpt">⚡ Активные ставки</div>
    <div id="act-list"><div class="nodata">Нет активных</div></div>
  </div>
</aside>

<!-- SETTLE MODAL -->
<div class="moverlay" id="moverlay">
  <div class="modal">
    <div class="mtitle">Расчёт ставки</div>
    <div class="minfo" id="minfo"></div>
    <div class="rbtnrow">
      <button class="rbtn won"  onclick="selRes('won')">WON</button>
      <button class="rbtn lost" onclick="selRes('lost')">LOST</button>
      <button class="rbtn void" onclick="selRes('void')">VOID</button>
      <button class="rbtn push" onclick="selRes('push')">PUSH</button>
      <button class="rbtn sold" onclick="selRes('sold')">SOLD</button>
    </div>
    <div class="sell-row" id="sell-row" style="display:none">
      <div style="display:flex;gap:8px;align-items:center;margin-bottom:8px">
        <label style="font-size:10px;color:#aaa">Режим:</label>
        <button class="sell-mode-btn active" id="sm-price" onclick="setSellMode('price')">По цене</button>
        <button class="sell-mode-btn" id="sm-proceeds" onclick="setSellMode('proceeds')">По сумме</button>
      </div>
      <div class="prow" style="margin:0">
        <label id="sell-label">Цена продажи</label>
        <input type="number" class="pinput" id="sell-inp" step="0.001" placeholder="загрузка..." oninput="calcSoldPnl()">
      </div>
      <div id="sell-info" style="font-size:10px;color:#888;margin-top:4px"></div>
    </div>
    <div class="prow">
      <label>P&L $</label>
      <input type="number" class="pinput" id="pnl-inp" step="0.01" placeholder="автозаполнение">
    </div>
    <div class="mfooter">
      <button class="btn mbtn-cancel" onclick="closeSettle()">ОТМЕНА</button>
      <button class="btn mbtn-ok" onclick="confirmSettle()">ПОДТВЕРДИТЬ</button>
    </div>
  </div>
</div>

<div class="toast" id="toast"></div>

<script>
// ═══════════════════════════════════════════════════════
//  CONFIG — change API_BASE to your server address
// BUILD:20260311-v7
// ═══════════════════════════════════════════════════════
const API_BASE = '';   // same origin when served by Flask
let DEMO = false;       // set true if server unavailable

// ═══════════════════════════════════════════════════════
//  STATE
// ═══════════════════════════════════════════════════════
let curPage = 'dash', settleId = null, settleRes = null, settleBet = null;
let botRunning = false, botUptime = 0;
let liveRunning = false, liveUptime = 0;
let cachedCfg = {};

// ═══════════════════════════════════════════════════════
//  DEMO DATA
// ═══════════════════════════════════════════════════════
const DEMO_STATS = {
  ok:true,
  stats:{total:47,placed:39,won:18,lost:14,void:1,total_volume:312.50,total_profit:28.40,roi_actual_pct:9.09,avg_edge:4.21},
  bankroll:500,
  bot_running:false, bot_uptime:0,
  daily:[
    {day:'2026-02-23',placed:3,profit:-4.2},{day:'2026-02-24',placed:4,profit:8.1},
    {day:'2026-02-25',placed:2,profit:2.3},{day:'2026-02-26',placed:5,profit:-6.8},
    {day:'2026-02-27',placed:3,profit:11.2},{day:'2026-02-28',placed:4,profit:3.5},
    {day:'2026-03-01',placed:2,profit:7.9},{day:'2026-03-02',placed:6,profit:-2.1},
    {day:'2026-03-03',placed:3,profit:4.4},{day:'2026-03-04',placed:4,profit:9.7},
    {day:'2026-03-05',placed:2,profit:-1.3},{day:'2026-03-06',placed:5,profit:6.2},
    {day:'2026-03-07',placed:4,profit:-3.8},{day:'2026-03-08',placed:2,profit:2.5},
  ],
  sports:[
    {sport_id:2,sport_name:'🏀 Basketball',cnt:18,profit:14.2},
    {sport_id:6,sport_name:'🏒 Hockey',cnt:11,profit:8.7},
    {sport_id:7,sport_name:'⚽ Soccer',cnt:6,profit:-3.1},
    {sport_id:47,sport_name:'🎮 CS2',cnt:4,profit:8.6},
  ]
};
const DEMO_BETS = [
  {id:47,created_at:'2026-03-08T14:22:11',home:'Mavericks',away:'Raptors',league:'USA. NBA',outcome_name:'Under',market_type_name:'Total Under',market_param:229.5,bb_odds:2.0408,bb_price:0.49,value_pct:4.77,depth_at_price:1360.92,total_liquidity:9444,stake:5.0,status:'placed',order_id:'0x8eaae...5830',outcome_result:'pending',profit_actual:0,started_at_fmt:'08.03 22:00',error_msg:''},
  {id:46,created_at:'2026-03-08T11:05:44',home:'Rangers',away:'Flyers',league:'USA. NHL',outcome_name:'Team1 Win',market_type_name:'Team1 Win',market_param:0,bb_odds:1.724,bb_price:0.58,value_pct:1.84,depth_at_price:451,total_liquidity:2100,stake:5.0,status:'placed',order_id:'0x7fbbe...1234',outcome_result:'pending',profit_actual:0,started_at_fmt:'09.03 02:00',error_msg:''},
  {id:45,created_at:'2026-03-07T18:30:00',home:'Penn State',away:'Rutgers',league:'USA. NCAA',outcome_name:'Over',market_type_name:'Total Over',market_param:150.5,bb_odds:2.127,bb_price:0.47,value_pct:2.03,depth_at_price:1136,total_liquidity:3800,stake:5.0,status:'settled',order_id:'0x5aaac...9876',outcome_result:'won',profit_actual:5.64,started_at_fmt:'08.03 01:00',error_msg:''},
  {id:44,created_at:'2026-03-06T20:00:00',home:'Draper',away:'Cerundolo',league:'ATP Indian Wells',outcome_name:'Team1 Win',market_type_name:'Team1 Win',market_param:0,bb_odds:1.5625,bb_price:0.64,value_pct:2.15,depth_at_price:557,total_liquidity:2400,stake:5.0,status:'settled',order_id:'0x3bbbd...5555',outcome_result:'lost',profit_actual:-5.0,started_at_fmt:'08.03 20:00',error_msg:''},
];
const DEMO_CFG = {VB_MIN_ROI:'0.04',MIN_LIQUIDITY:'50',VB_STAKE_PCT:'0.01',VB_MAX_STAKE_PCT:'0.05',VB_MIN_STAKE:'2',VB_USE_KELLY:'false',POLL_INTERVAL:'5',BETBURGER_FILTER_ID_VALUEBET:'665262',BETBURGER_EMAIL:'demo@example.com',POLYMARKET_FUNDER:'0x3123...b44D'};
const DEMO_LOG = [
  '14:22:11  INFO     ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━',
  '14:22:11  INFO     🎯  Mavericks vs Raptors',
  '14:22:11  INFO         USA. NBA  |  08.03 22:00',
  '14:22:11  INFO         ИСХОД:   Under  (Total Under  линия=229.5)',
  '14:22:11  INFO         EDGE:    +4.77%',
  '14:22:11  INFO         КОЭФ:    2.0408   implied 49.0%',
  '14:22:11  INFO         ЛИК:     $9444 (рынок)  $1360 (стакан)',
  '14:22:11  INFO         СТАВКА:  $5.00 @ price=0.4900',
  '14:22:12  INFO     ✅  ПОСТАВЛЕНО  $5.00 @ 0.4900 | order=0x8eaaece3d3d994df',
  '14:22:12  INFO     ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━',
  '14:22:17  INFO     [tick 2] Получено 28 Polymarket бетов',
  '14:22:17  DEBUG    skip [in-memory dup] Mavericks vs Raptors | Under',
  '14:22:17  DEBUG    skip [low edge] Magic vs Bucks  edge=1.50% < 4.00%',
  '14:22:17  DEBUG    skip [low liq] Real Madrid vs Elche  $17 < $50',
  '14:25:00  INFO     [tick 37] Получено 31 Polymarket бетов',
];

// ═══════════════════════════════════════════════════════
//  API
// ═══════════════════════════════════════════════════════
async function api(url, opts={}) {
  try {
    const r = await fetch(API_BASE+url, opts);
    if (!r.ok) throw new Error('HTTP '+r.status);
    return await r.json();
  } catch(e) {
    return {ok:false, error:e.message, _net_err:true};
  }
}

// ═══════════════════════════════════════════════════════
//  NAV
// ═══════════════════════════════════════════════════════
function nav(p) {
  document.querySelectorAll('.page').forEach(x=>x.classList.remove('on'));
  document.querySelectorAll('.ni').forEach(x=>x.classList.remove('on'));
  document.getElementById('page-'+p).classList.add('on');
  document.querySelector(`[data-p="${p}"]`).classList.add('on');
  curPage = p;
  // Скрыть правую панель на страницах без неё (cfg, log, signals, feed, portfolio)
  const fullPages = ['cfg','log','signals','feed','portfolio','hedge','dutch','mm','snipe','backlog'];
  const rp = document.querySelector('aside.right');
  if(rp) {
    if(fullPages.includes(p)) { rp.style.display='none'; document.body.style.gridTemplateColumns='210px 1fr'; }
    else { rp.style.display=''; document.body.style.gridTemplateColumns='210px 1fr 300px'; }
  }
  if(p==='log') loadLog();
  if(p==='cfg') loadCfg();
  if(p==='bets') { loadSportFilter(); loadBetsPage(1); }
  if(p==='signals') loadSignals();
  if(p==='feed') loadFeed();
  if(p==='hedge') loadHedge();
  if(p==='dutch') loadDutch();
  if(p==='mm') loadMM();
  if(p==='snipe') loadSnipe();
  if(p==='backlog') loadBacklog();
}

// ═══════════════════════════════════════════════════════
//  HELPERS
// ═══════════════════════════════════════════════════════
const f=(n,d=2)=>(n===null||n===undefined||isNaN(n))?'\u2014':parseFloat(n).toFixed(d);
function setHV(id,txt,cls) {
  const el=document.getElementById(id); if(!el)return;
  el.textContent=txt; el.className='hv '+(cls||'n');
}
function sbadge(s) {
  const m={placed:'bpl',pending:'bpe',failed:'bfa',settled:'bse'};
  return `<span class="badge ${m[s]||'bse'}">${s||'\u2014'}</span>`;
}
function rbadge(r) {
  if(!r||r==='pending') return '<span style="color:var(--tx3);font-size:10px">—</span>';
  const m={won:'bwn',lost:'bls',void:'bvo',push:'bvo',sold:'bsd'};
  return `<span class="badge ${m[r]||'bvo'}">${r}</span>`;
}
function resellBadge(rs) {
  if(!rs) return '';
  const m={pending_sell:'bpe',selling:'bsd',sold:'bwn',expired:'bvo',cancelled:'bfa'};
  const labels={pending_sell:'RESELL⏳',selling:'SELLING',sold:'RESOLD✓',expired:'EXPIRED',cancelled:'CANCEL'};
  return `<span class="badge ${m[rs]||'bvo'}" style="font-size:8px;margin-left:3px">${labels[rs]||rs}</span>`;
}
function fmt2(n) {
  const v=parseFloat(n)||0;
  return `<span class="${v>=0?'pp':'np'}">${v>=0?'+':''}$${f(Math.abs(v))}</span>`;
}
function toast(msg,err=false) {
  const el=document.getElementById('toast');
  el.textContent=msg; el.className='toast on'+(err?' err':'');
  clearTimeout(toast._t); toast._t=setTimeout(()=>el.classList.remove('on'),3000);
}

// ═══════════════════════════════════════════════════════
//  STATS
// ═══════════════════════════════════════════════════════
async function loadStats() {
  let d = await api('/api/stats');
  if(d._net_err) { d=DEMO_STATS; DEMO=true; }
  else { DEMO=false; }
  document.getElementById('demobanner').classList.toggle('hidden',!DEMO);

  const s=d.stats||{}, brl=d.bankroll||0;
  const pnl=s.total_profit||0, roi=s.roi_actual_pct||0;
  const settled=(s.won||0)+(s.lost||0)+(s.sold||0), wr=settled>0?(s.won||0)/settled*100:0;

  document.getElementById('h-brl').textContent='$'+f(brl);
  if(d.free_usdc != null) document.getElementById('h-free').textContent='$'+f(d.free_usdc);
  setHV('h-pnl',(pnl>=0?'+':'')+'$'+f(Math.abs(pnl)),pnl>=0?'g':'r');
  setHV('h-roi',(roi>=0?'+':'')+f(roi)+'%',roi>=0?'g':'r');
  const fees = d.total_fees||0;
  const feesEl = document.getElementById('h-fees');
  if(feesEl) { feesEl.textContent = fees > 0 ? '-$'+fees.toFixed(2) : '$0'; feesEl.style.color = fees > 0 ? 'var(--r)' : '#555'; }
  setHV('h-wr',f(wr)+'%',wr>=55?'g':wr>=45?'n':'r');
  document.getElementById('h-tot').textContent=s.total||0;

  document.getElementById('s-placed').textContent=s.placed||0;
  document.getElementById('s-pending').textContent=(s.total-(s.placed||0))+' pending';
  document.getElementById('s-wl').innerHTML=`<span style="color:var(--g)">${s.won||0}</span><span style="color:var(--tx2)"> / </span><span style="color:var(--r)">${s.lost||0}</span>${(s.sold||0)>0?`<span style="color:var(--tx2)"> / </span><span style="color:#ffb800">${s.sold} sold</span>`:''}`;
  document.getElementById('s-wr').textContent=f(wr)+'% win rate';
  document.getElementById('s-vol').textContent='$'+f(s.total_volume||0);
  // Show total cost vs payout in bets page if elements exist
  const costEl = document.getElementById('s-cost');
  const payEl  = document.getElementById('s-pay');
  if (costEl) costEl.textContent='$'+f(s.total_cost||s.total_volume||0);
  if (payEl)  payEl.textContent='$'+f(s.total_payout_target||0);
  document.getElementById('s-edge').textContent='+'+f(s.avg_edge||0)+'%';

  document.getElementById('brl-inp').value=brl.toFixed(2);

  // Bankroll preview from config
  const sp=parseFloat(cachedCfg.VB_STAKE_PCT||0.01)*100;
  const mp=parseFloat(cachedCfg.VB_MAX_STAKE_PCT||0.05)*100;
  document.getElementById('stk-pct').textContent=f(sp)+'%';
  document.getElementById('max-pct').textContent=f(mp)+'%';

  renderDailyChart(d.daily||[]);
  renderSports(d.sports||[]);
  updateBotUI(d.bot_running,d.bot_uptime);
  await loadActiveBets(d);
  loadResellStats();
  loadLineStats();
}

async function loadResellStats() {
  if(DEMO) return;
  const d = await api('/api/stats/resell');
  if(!d.ok) return;
  const blk = document.getElementById('resell-block');
  if(!blk) return;
  document.getElementById('rs-sold').textContent    = d.total_resold||0;
  const prof = d.total_markup_profit||0;
  document.getElementById('rs-profit').textContent   = (prof>=0?'+':'')+'$'+Math.abs(prof).toFixed(2);
  document.getElementById('rs-profit').style.color   = prof>=0?'var(--g)':'var(--r)';
  const vol = d.resell_volume||0;
  document.getElementById('rs-volume').textContent   = '$'+vol.toFixed(2);
  const roi = vol > 0 ? (prof / vol * 100) : 0;
  document.getElementById('rs-roi').textContent      = (roi>=0?'+':'')+roi.toFixed(1)+'%';
  document.getElementById('rs-roi').style.color      = roi>=0?'var(--g)':'var(--r)';
  document.getElementById('rs-avg').textContent      = (d.avg_markup_pct||0).toFixed(1)+'%';
  document.getElementById('rs-pending').textContent  = d.pending_resells||0;
  document.getElementById('rs-expired').textContent  = d.expired||0;
  document.getElementById('rs-pm-sold').textContent  = d.pm_resold||0;
  document.getElementById('rs-pm-profit').textContent= '$'+(d.pm_profit||0).toFixed(2);
  document.getElementById('rs-lv-sold').textContent  = d.lv_resold||0;
  document.getElementById('rs-lv-profit').textContent= '$'+(d.lv_profit||0).toFixed(2);
}

function renderDailyChart(daily) {
  const el=document.getElementById('dchart');
  if(!daily.length){el.innerHTML='<div class="nodata">Нет данных</div>';return;}
  const sorted=[...daily].sort((a,b)=>a.day>b.day?1:-1).slice(-14);
  const maxA=Math.max(...sorted.map(d=>Math.abs(d.profit||0)),1);
  el.innerHTML=sorted.map(d=>{
    const p=d.profit||0, h=Math.max(2,Math.abs(p)/maxA*64);
    const lbl=d.day?d.day.slice(5):'';
    const tip=`${d.day}\\nСтавок:${d.placed||0}  P&L:${p>=0?'+':''}$${f(Math.abs(p))}`;
    return `<div class="db" title="${tip}">
      <div class="dbar ${p>=0?'pos':'neg'}" style="height:${h}px"></div>
      <div class="dlb">${lbl}</div>
    </div>`;
  }).join('');
}

function renderSports(sports) {
  const el=document.getElementById('sport-list');
  if(!sports.length){el.innerHTML='<div class="nodata">Нет данных</div>';return;}
  el.innerHTML=sports.slice(0,10).map(s=>{
    const p=s.profit||0;
    return `<div class="sprow">
      <span class="spnm">${s.sport_name}</span>
      <span class="spcnt">${s.cnt}</span>
      <span class="sppnl ${p>=0?'pp':'np'}">${p>=0?'+':''}$${f(Math.abs(p))}</span>
    </div>`;
  }).join('');
}

async function loadActiveBets(statsData) {
  let bets;
  if(DEMO) { bets=DEMO_BETS.filter(b=>b.outcome_result==='pending'); }
  else {
    const d=await api('/api/bets?status=active&limit=20');
    bets=d.ok?d.bets:[];
  }
  document.getElementById('nbact').textContent=bets.length;
  const el=document.getElementById('act-list');
  if(!bets.length){el.innerHTML='<div class="nodata">Нет активных</div>';return;}
  window._activeBets = bets;
  el.innerHTML=bets.map((b,i)=>{
    const isLive   = b.bet_mode === 'live';
    const stripCol = isLive ? '#e67e22' : 'var(--g)';  // оранжевая = лайв, зелёная = прематч
    const arbPct   = b.arb_pct || 0;
    return `
    <div class="abcard" style="border-left-color:${stripCol}">
      <div style="display:flex;justify-content:space-between;align-items:flex-start">
        <div class="abev" style="flex:1">${b.home} vs ${b.away}</div>
        ${isLive
          ? '<span style="font-size:9px;background:#e67e2222;color:#e67e22;border:1px solid #e67e2244;padding:1px 5px;border-radius:2px;font-family:monospace;white-space:nowrap">⚡ LIVE</span>'
          : '<span style="font-size:9px;color:#3498db;opacity:.6;font-family:monospace;white-space:nowrap">📈 ПМ</span>'}
      </div>
      <div class="about">${b.outcome_name}${b.market_param?' ('+b.market_param+')':''}</div>
      <div class="abmeta">
        <span style="color:#00e87a">EDGE: +${f(b.value_pct)}%</span>
        ${arbPct > 0 ? `<span style="color:#e67e22;font-size:10px" title="Доходность вилки BetBurger">Арб ${f(arbPct)}%</span>` : ''}
        <span title="${(b.shares||b.stake||0).toFixed(2)} shares × $${f(b.bb_price,3)} = потрачено">
          💰$<span style="color:#fff;font-weight:700">${f(b.cost_usdc != null ? b.cost_usdc : (b.shares||b.stake||0)*(b.bb_price||0))}</span>
        </span>
        <span title="Выигрыш при победе: ${b.shares||b.stake||0} shares × $1" style="color:#3498db">
          →$<span style="font-weight:700">${f(b.payout_target != null ? b.payout_target : (b.shares||b.stake||0))}</span>
        </span>
        ${(b.depth_at_price||b.total_liquidity) > 0 ? `<span title="Ликвидность по цене входа" style="color:#888">💧$${f(b.depth_at_price||b.total_liquidity)}</span>` : ''}
        <span style="color:#888">@${f(b.bb_odds,3)}</span>
      </div>
      <div style="font-size:9px;color:var(--tx2);margin-top:4px">📅 ${b.started_at_fmt}</div>
      <button class="absb" onclick="openSettle(window._activeBets[${i}].id,window._activeBets[${i}])">РАСЧИТАТЬ</button>
    </div>`;
  }).join('');
}

// ═══════════════════════════════════════════════════════
//  BETS TABLE
// ═══════════════════════════════════════════════════════
async function loadBets() {
  let bets;
  if(DEMO) bets=DEMO_BETS;
  else { const d=await api('/api/bets?limit=25'); bets=d.ok?d.bets:[]; }
  renderTable(bets,'tb-dash',false);
}

async function autoSettle() {
  const btn = event.target;
  btn.disabled = true;
  btn.textContent = '⏳ Проверяю...';
  try {
    const d = await api('/api/settle/auto', {
      method:'POST',
      headers:{'Content-Type':'application/json'},
      body:'{}'
    });
    if (d.ok) {
      alert(`✅ ${d.message || 'Готово'}`);
      loadBetsPage();
      loadStats();
    } else {
      alert('Ошибка: ' + (d.error || 'неизвестная'));
    }
  } finally {
    btn.disabled = false;
    btn.textContent = '⚡ АВТО-РАСЧЁТ';
  }
}

function resetFilters() {
  ['f-result','f-sport','f-league','f-date-from','f-date-to',
   'f-odds-min','f-odds-max','f-liq-min','f-liq-max','f-edge-min','f-edge-max','f-arb-min','f-arb-max','f-mode'].forEach(id => {
    const el = document.getElementById(id);
    if (el) el.value = '';
  });
  loadBetsPage(1);
}

function updateBetsStatsBar(bets, total, agg) {
  // Если есть агрегированные данные от сервера — используем их (по всей выборке)
  // Иначе считаем по текущей странице
  let totalCost, totalPnl, wonCnt, lostCnt, settledCnt, wr, roi;

  if (agg) {
    totalCost   = agg.total_cost  || 0;
    totalPnl    = agg.total_pnl   || 0;
    wonCnt      = agg.won         || 0;
    lostCnt     = agg.lost        || 0;
    const soldCnt = agg.sold || 0;
    settledCnt  = wonCnt + lostCnt + soldCnt;
    wr  = agg.winrate != null ? agg.winrate.toFixed(1) : '\u2014';
    roi = agg.roi     != null ? agg.roi.toFixed(1)     : '\u2014';
  } else {
    const settled = bets.filter(b => ['won','lost','sold'].includes(b.outcome_result));
    const won     = bets.filter(b => b.outcome_result === 'won');
    const lost    = bets.filter(b => b.outcome_result === 'lost');
    const sold    = bets.filter(b => b.outcome_result === 'sold');
    totalCost  = bets.reduce((s,b) => s + (b.cost_usdc || 0), 0);
    totalPnl   = settled.reduce((s,b) => s + (b.profit_actual||0), 0);
    wonCnt     = won.length; lostCnt = lost.length; settledCnt = settled.length;
    wr  = settledCnt > 0 ? (wonCnt / settledCnt * 100).toFixed(1) : '\u2014';
    // ROI от оборота расчитанных ставок (won+lost+sold)
    const settledCostJS = settled.reduce((s,b) => s + (b.cost_usdc || 0), 0);
    roi = settledCostJS > 0 ? (totalPnl / settledCostJS * 100).toFixed(1) : '\u2014';
  }

  const set = (id, val) => { const el=document.getElementById(id); if(el) el.textContent=val; };
  set('bs-total',       total || bets.length);
  set('bs-failed',      agg ? (agg.failed_cnt || 0) : bets.filter(b=>b.status==='failed').length);
  set('bs-pending',     agg ? (agg.pending_cnt || 0) : bets.filter(b=>b.outcome_result==='pending').length);
  set('bs-settled',     settledCnt);
  set('bs-settled-vol', '$' + f(agg ? (agg.settled_volume || 0) : totalCost));
  set('bs-won',         wonCnt + ' / ' + lostCnt + (settledCnt > 0 ? ' (' + (wonCnt/settledCnt*100).toFixed(0) + '%)' : ''));
  set('bs-wr',          wr === '\u2014' ? '\u2014' : wr + '%');
  set('bs-roi',         roi === '\u2014' ? '\u2014' : roi + '%');
  const pnlStr2 = totalPnl === 0 ? '$0.00' : (totalPnl > 0 ? '+$' : '-$') + f(Math.abs(totalPnl));
  set('bs-pnl',         pnlStr2);
  if (agg) {
    set('bs-avg-edge', agg.avg_edge != null ? '+' + f(agg.avg_edge) + '%' : '\u2014');
    set('bs-avg-odds', agg.avg_odds != null ? f(agg.avg_odds, 3) : '\u2014');
  }

  const roiEl = document.getElementById('bs-roi');
  if (roiEl) roiEl.style.color = (parseFloat(roi)||0) >= 0 ? '#00e87a' : '#e74c3c';
  const pnlEl = document.getElementById('bs-pnl');
  if (pnlEl) pnlEl.style.color = totalPnl >= 0 ? '#00e87a' : '#e74c3c';

  // Resell-specific stats
  const rsWrap = document.getElementById('bs-resell-wrap');
  const isResell = document.getElementById('f-mode')?.value === 'resell';
  if (rsWrap) rsWrap.style.display = isResell ? '' : 'none';
  if (isResell && agg && agg.resell) {
    const rs = agg.resell;
    set('bs-resold',     rs.resold || 0);
    set('bs-onsale',     rs.on_sale || 0);
    set('bs-rs-expired', rs.expired || 0);
    set('bs-rs-markup',  rs.avg_markup_pct != null ? rs.avg_markup_pct.toFixed(1)+'%' : '\u2014');
    const rsPnl = rs.resell_profit || 0;
    set('bs-rs-pnl',     (rsPnl >= 0 ? '+$' : '-$') + f(Math.abs(rsPnl)));
    const rsPnlEl = document.getElementById('bs-rs-pnl');
    if (rsPnlEl) rsPnlEl.style.color = rsPnl >= 0 ? '#00e87a' : '#e74c3c';
  }
}

async function loadSportFilter() {
  if (DEMO) return;
  const sel = document.getElementById('f-sport');
  if (!sel) return;
  // Prevent duplicate loading
  if (sel.dataset.loaded === '1') return;
  sel.dataset.loaded = '1';

  const d = await api('/api/stats');
  if (!d.ok) return;

  // Clear all except first "Все виды" option
  while (sel.options.length > 1) sel.remove(1);

  (d.sports || []).forEach(s => {
    const opt = document.createElement('option');
    opt.value = s.sport_id;
    opt.textContent = (s.sport_name||'') + ' (' + (s.cnt||0) + ')';
    sel.appendChild(opt);
  });
}

// ── Infinite scroll state ──────────────────────────────────────────────
let _betsPage       = 1;
let _betsTotalPages = 1;
let _betsLoading    = false;
let _betsAllLoaded  = false;
let _betsCurrentAgg = null;

async function fixWrongWon() {
  if (!confirm('Проверить все WON-ставки через Gamma API и исправить те, где токен на самом деле проиграл? Это займёт время (~0.3 сек на каждые 10 ставок).')) return;
  const btn = event.target;
  btn.disabled = true;
  btn.textContent = '⏳ Проверяю...';
  try {
    const d = await api('/api/bets/fix-wrong-won', {
      method:'POST',
      headers:{'Content-Type':'application/json'},
      body:'{}'
    });
    if (d.ok) {
      alert(`✅ ${d.message}\n\nПроверено: ${d.checked}\nИсправлено WON→LOST: ${d.fixed}`);
      loadBetsPage(1);
      loadStats();
    } else {
      alert('Ошибка: ' + (d.error || 'неизвестная'));
    }
  } finally {
    btn.disabled = false;
    btn.textContent = '🔍 ИСПРАВИТЬ WON→LOST';
  }
}

async function fixPnl() {
  const btn = event.target;
  btn.disabled = true;
  btn.textContent = '⏳ Пересчёт...';
  try {
    const d = await api('/api/bets/fix-pnl', {
      method:'POST',
      headers:{'Content-Type':'application/json'},
      body:'{}'
    });
    if (d.ok) {
      alert(`🔧 Исправлено ${d.fixed} из ${d.total} ставок`);
      loadBetsPage(1);
      loadStats();
    } else {
      alert('Ошибка: ' + (d.error || 'неизвестная'));
    }
  } finally {
    btn.disabled = false;
    btn.textContent = '🔧 ПЕРЕСЧЁТ P&L';
  }
}

async function fixCancelled() {
  const btn = event.target;
  btn.disabled = true;
  btn.textContent = '⏳ Закрываю...';
  try {
    const d = await api('/api/bets/fix-cancelled', {
      method:'POST',
      headers:{'Content-Type':'application/json'},
      body:'{}'
    });
    if (d.ok) {
      alert(`🚫 Закрыто ${d.voided} отменённых ставок как void\nОни больше не влияют на winrate и статистику`);
      loadBetsPage(1);
      loadStats();
    } else {
      alert('Ошибка: ' + (d.error || 'неизвестная'));
    }
  } finally {
    btn.disabled = false;
    btn.textContent = '🚫 ЗАКРЫТЬ ОТМЕНЁННЫЕ';
  }
}

async function loadBetsPage(page, append) {
  if (page) _betsPage = page;
  if (!append) {
    // Сброс при новой загрузке / фильтре
    _betsAllLoaded = false;
    document.getElementById('tb-bets').innerHTML = '';
  }
  if (_betsLoading || _betsAllLoaded) return;
  _betsLoading = true;

  const pgSize = parseInt(document.getElementById('pg-size')?.value || '50');
  const offset = (_betsPage - 1) * pgSize;

  const fResult   = document.getElementById('f-result')?.value  || '';
  const fSport    = document.getElementById('f-sport')?.value   || '';
  const fLeague   = document.getElementById('f-league')?.value  || '';
  const fDateFrom = document.getElementById('f-date-from')?.value || '';
  const fDateTo   = document.getElementById('f-date-to')?.value   || '';
  const fOddsMin  = document.getElementById('f-odds-min')?.value  || '';
  const fOddsMax  = document.getElementById('f-odds-max')?.value  || '';
  const fLiqMin   = document.getElementById('f-liq-min')?.value   || '';
  const fLiqMax   = document.getElementById('f-liq-max')?.value   || '';
  const fEdgeMin  = document.getElementById('f-edge-min')?.value  || '';
  const fEdgeMax  = document.getElementById('f-edge-max')?.value  || '';
  const fArbMin   = document.getElementById('f-arb-min')?.value   || '';
  const fArbMax   = document.getElementById('f-arb-max')?.value   || '';
  const fMode     = document.getElementById('f-mode')?.value      || '';

  let bets = [], totalCount = 0;
  if (DEMO) {
    bets = DEMO_BETS; totalCount = bets.length;
  } else {
    const params = new URLSearchParams({limit: pgSize, offset});
    if (fResult)   params.set('result', fResult);
    if (fSport)    params.set('sport', fSport);
    if (fLeague)   params.set('league', fLeague);
    if (fDateFrom) params.set('date_from', fDateFrom);
    if (fDateTo)   params.set('date_to', fDateTo);
    if (fOddsMin)  params.set('odds_min', fOddsMin);
    if (fOddsMax)  params.set('odds_max', fOddsMax);
    if (fLiqMin)   params.set('liq_min',  fLiqMin);
    if (fLiqMax)   params.set('liq_max',  fLiqMax);
    if (fEdgeMin)  params.set('edge_min', fEdgeMin);
    if (fEdgeMax)  params.set('edge_max', fEdgeMax);
    if (fArbMin)   params.set('arb_min',  fArbMin);
    if (fArbMax)   params.set('arb_max',  fArbMax);
    if (fMode)     params.set('mode', fMode);
    const d = await api('/api/bets?' + params);
    if (d.ok) {
      bets = d.bets; totalCount = d.total || d.count || 0;
      _betsCurrentAgg = d.agg || null;
      if (!d.has_more) _betsAllLoaded = true;
    }
  }

  _betsTotalPages = Math.max(1, Math.ceil(totalCount / pgSize));
  renderTableAppend(bets, 'tb-bets', true, append);

  // Pagination info
  const pgInfo = document.getElementById('pg-info');
  if (pgInfo) {
    const loaded = (offset + bets.length);
    pgInfo.textContent = _betsAllLoaded
      ? `Все ${totalCount} ставок загружены`
      : `${loaded} из ${totalCount} ставок — прокрутите вниз для загрузки ещё`;
  }
  const prevBtn = document.getElementById('pg-prev');
  const nextBtn = document.getElementById('pg-next');
  if (prevBtn) prevBtn.style.opacity = _betsPage <= 1 ? '0.3' : '1';
  if (nextBtn) nextBtn.style.opacity = _betsAllLoaded ? '0.3' : '1';

  updateBetsStatsBar(bets, totalCount, _betsCurrentAgg);
  _betsLoading = false;

  // Загрузить спарклайны для видимых ставок
  const sparkIds = bets.map(b => b.id).filter(Boolean);
  if(sparkIds.length) loadSparklines(sparkIds);
}

function changeBetsPage(delta) {
  if (delta > 0) {
    if (_betsAllLoaded) return;
    _betsPage++;
    loadBetsPage(null, true);  // append mode
  } else {
    // "Пред" = reload all from page 1
    loadBetsPage(1, false);
  }
}

// Infinite scroll
(function() {
  function onScroll() {
    const page = document.getElementById('page-bets');
    if (!page || page.style.display === 'none') return;
    const tw = page.querySelector('.tw');
    if (!tw) return;
    const rect = tw.getBoundingClientRect();
    if (rect.bottom < window.innerHeight + 300 && !_betsLoading && !_betsAllLoaded) {
      _betsPage++;
      loadBetsPage(null, true);
    }
  }
  window.addEventListener('scroll', onScroll, {passive: true});
  // Также слушаем скролл внутри .tw (таблица с overflow)
  document.addEventListener('DOMContentLoaded', () => {
    const tw = document.querySelector('#page-bets .tw');
    if (tw) tw.addEventListener('scroll', onScroll, {passive: true});
  });
})();

function renderTable(bets, tbId, full) {
  renderTableAppend(bets, tbId, full, false);
}

function renderTableAppend(bets, tbId, full, append) {
  const tb = document.getElementById(tbId);
  if (!bets || !bets.length) {
    if (!append) tb.innerHTML = `<tr><td colspan="${full?15:10}" class="nodata">Нет ставок</td></tr>`;
    return;
  }
  if (!append) window._tableBets = bets;
  else window._tableBets = (window._tableBets || []).concat(bets);
  const baseIdx = append ? (window._tableBets.length - bets.length) : 0;
  const rows = bets.map((b, _bi_rel) => {
    const _bi = baseIdx + _bi_rel;
    const pnl    = b.profit_actual || 0;
    const pnlStr = b.outcome_result !== 'pending' ? fmt2(pnl) : '\u2014';
    const pnlCol = pnl > 0 ? '#00e87a' : (pnl < 0 ? '#e74c3c' : 'var(--tx2)');
    const ord    = b.order_id
      ? `<span title="${b.order_id}" style="cursor:help;color:var(--tx2)">${b.order_id.slice(0,10)}…</span>`
      : '\u2014';
    const setBtn = full && b.outcome_result === 'pending' && b.status === 'placed'
      ? `<button class="absb" onclick="openSettle(${b.id},window._tableBets&&window._tableBets[${_bi}]||{id:${b.id}})" style="width:auto;padding:3px 8px;font-size:9px">РАСЧЁТ</button>`
      : ' ';
    const date = b.created_at ? b.created_at.slice(0,16).replace('T',' ') : '\u2014';

    // cost_usdc = shares × stake_price = реально потрачено USDC
    const shares  = b.shares || b.stake || 0;
    const entryP  = b.stake_price || b.bb_price || 0;
    const cost    = b.cost_usdc != null ? b.cost_usdc : Math.round(shares * entryP * 100) / 100;
    const payout  = b.payout_target != null ? b.payout_target : shares;
    const tooltip = `${shares.toFixed(2)} shares × $${f(entryP,3)} = $${f(cost)} потрачено`;
    // depth_at_price = ликвидность по нашей цене входа (market_depth из BB) — главное
    // total_liquidity = общий объём рынка — показываем в тултипе
    const depth   = b.depth_at_price || 0;
    const totalLiq = b.total_liquidity || 0;
    const liqShow = depth > 0 ? depth : totalLiq;  // depth приоритетнее
    const liqTip  = depth > 0 && totalLiq > 0
      ? `Стакан по цене: $${f(depth)} | Рынок всего: $${f(totalLiq,0)}`
      : depth > 0 ? `Стакан по цене: $${f(depth)}`
      : totalLiq > 0 ? `Рынок всего: $${f(totalLiq,0)}` : '';

    return `<tr>
      <td style="color:var(--tx3)">${b.id}</td>
      ${full ? `<td style="font-size:10px;color:var(--tx2)">${date}</td>` : ''}
      <td class="ten">
        <div class="en">${b.home||'?'} vs ${b.away||'?'}</div>
        <div class="el">${b.league||''}</div>
      </td>
      <td class="tou">
        <div class="on">${b.outcome_name||''}</div>
        <div class="ot">${b.market_type_name||''}${b.market_param ? ' ' + b.market_param : ''}</div>
      </td>
      <td class="edge">
        <div title="Велью% = middle_value BetBurger">+${f(b.value_pct)}%</div>
        ${(b.arb_pct||0) > 0 ? `<div style="font-size:9px;color:#e67e22;font-family:monospace" title="Доходность вилки arbs[].percent">Арб ${f(b.arb_pct)}%</div>` : ''}
      </td>
      <td style="color:var(--tx)">${f(b.bb_odds, 3)}</td>
      <td style="color:var(--tx2);font-size:10px" title="${liqTip}">
        ${liqShow > 0 ? '$'+f(liqShow) : '\u2014'}
      </td>
      <td style="font-weight:700;color:#e8c400" title="${tooltip}">
        $${f(cost)}
      </td>
      <td style="color:var(--acc)" title="Выигрыш при победе: ${shares.toFixed(2)} shares × $1 = $${f(payout)}">
        $${f(payout)}
      </td>
      ${full ? `<td style="font-size:10px">${ord}</td>` : ''}
      <td>${sbadge(b.status)}</td>
      ${full ? `<td style="font-size:9px">${b.bet_mode==='live'
        ? '<span style="color:#e67e22;font-weight:700">\u26a1 live</span>'
        : '<span style="color:#3498db;opacity:.7">📈 ПМ</span>'
      }</td>` : ''}
      ${full ? `<td>${rbadge(b.outcome_result)}${b.resell_status ? ' '+resellBadge(b.resell_status) : ''}</td>` : ''}
      <td style="font-weight:700">${pnlStr}</td>
      <td><span id="spark-${b.id}" style="display:inline-block;width:70px;height:18px"></span></td>
      <td style="font-size:9px;color:var(--tx2)">${b.started_at_fmt||'\u2014'}</td>
      ${full ? `<td>${setBtn}</td>` : ''}
    </tr>`;
  }).join('');

  if (append) {
    tb.insertAdjacentHTML('beforeend', rows);
  } else {
    tb.innerHTML = rows;
  }
}

// ═══════════════════════════════════════════════════════
//  LINE MOVEMENT
// ═══════════════════════════════════════════════════════
const SPORT_NAMES = {1:'Baseball',2:'Basketball',6:'Hockey',7:'Football',8:'Tennis',13:'Table Tennis',24:'Cricket',43:'Rugby',45:'MMA/UFC',46:'CS2',47:'CS2',48:'Dota 2',49:'LoL',50:'Valorant',51:'Valorant',57:'CoD',61:'ML:BB',63:'ML:BB',65:'StarCraft'};

async function loadLineStats() {
  const d = await api('/api/stats/line_movement');
  if(!d.ok) return;
  const blk = document.getElementById('line-move-block');
  if(d.total_tracked > 0) blk.style.display = '';
  else { blk.style.display = 'none'; return; }

  document.getElementById('lm-tracked').textContent = d.total_tracked;
  document.getElementById('lm-favorable').textContent = d.favorable_pct + '%';
  document.getElementById('lm-favorable').style.color = d.favorable_pct >= 50 ? 'var(--g)' : 'var(--r)';
  const avg = d.avg_move_pct || 0;
  const avgEl = document.getElementById('lm-avg');
  avgEl.textContent = (avg >= 0 ? '+' : '') + avg.toFixed(1) + '%';
  avgEl.style.color = avg >= 0 ? 'var(--g)' : 'var(--r)';

  const tb = document.getElementById('lm-sports-tbody');
  const sports = d.by_sport || {};
  tb.innerHTML = Object.entries(sports).sort((a,b) => b[1].count - a[1].count).map(([sid, s]) => {
    const name = SPORT_NAMES[sid] || 'Sport #'+sid;
    const avgM = s.avg_move || 0;
    const favP = s.favorable_pct || 0;
    return '<tr><td>'+name+'</td><td>'+s.count+'</td>'+
      '<td style="color:'+(avgM>=0?'var(--g)':'var(--r)')+'">'+
        (avgM>=0?'+':'')+avgM.toFixed(1)+'%</td>'+
      '<td style="color:'+(favP>=50?'var(--g)':'var(--r)')+'">'+favP+'%</td></tr>';
  }).join('');
}

// ═══════════════════════════════════════════════════════
//  LINE MOVEMENT SPARKLINES
// ═══════════════════════════════════════════════════════
function sparkSvg(entry, points) {
  if(!points||points.length<2) return '';
  const w=70, h=18, pad=1;
  const prices = points.map(p=>p.p);
  const mn = Math.min(entry, ...prices) - 0.005;
  const mx = Math.max(entry, ...prices) + 0.005;
  const rng = mx - mn || 0.01;
  const scaleY = v => pad + (mx - v) / rng * (h - 2*pad);
  const scaleX = (i) => pad + i / (points.length-1) * (w - 2*pad);
  const entryY = scaleY(entry).toFixed(1);
  const last = prices[prices.length-1];
  const favorable = last > entry;  // цена выросла = рынок подтвердил
  const col = favorable ? '#2ecc71' : '#e74c3c';
  const pts = points.map((p,i) => scaleX(i).toFixed(1)+','+scaleY(p.p).toFixed(1)).join(' ');
  const move = ((last - entry) / entry * 100).toFixed(1);
  return `<svg width="${w}" height="${h}" style="vertical-align:middle" title="Move: ${move}%">` +
    `<line x1="0" y1="${entryY}" x2="${w}" y2="${entryY}" stroke="#555" stroke-dasharray="2,2" stroke-width="0.5"/>` +
    `<polyline points="${pts}" fill="none" stroke="${col}" stroke-width="1.2"/>` +
    `<text x="${w-2}" y="${h-2}" text-anchor="end" font-size="7" fill="${col}">${move>0?'+':''}${move}%</text>` +
    `</svg>`;
}

async function loadSparklines(betIds) {
  if(!betIds||!betIds.length) return;
  for(const id of betIds.slice(0, 30)) {
    const el = document.getElementById('spark-'+id);
    if(!el) continue;
    try {
      const d = await api('/api/bets/'+id+'/line');
      if(d.ok && d.snapshots && d.snapshots.length >= 2) {
        el.innerHTML = sparkSvg(d.entry_price, d.snapshots);
      } else {
        el.innerHTML = '<span style="color:#333;font-size:8px">-</span>';
      }
    } catch(e) {
      el.innerHTML = '';
    }
  }
}

// ═══════════════════════════════════════════════════════
//  CONFIG
// ═══════════════════════════════════════════════════════
async function loadCfg() {
  let c;
  if(DEMO) c=DEMO_CFG;
  else { const d=await api('/api/config'); if(!d.ok) c=DEMO_CFG; else c=d.config||{}; }
  cachedCfg=c;
  // Прематч
  document.getElementById('c-roi').value    = parseFloat(c.VB_MIN_ROI||.04)*100;
  document.getElementById('c-liq').value    = c.MIN_LIQUIDITY||50;
  document.getElementById('c-stk').value    = parseFloat(c.VB_STAKE_PCT||.01)*100;
  document.getElementById('c-maxstk').value = parseFloat(c.VB_MAX_STAKE_PCT||.05)*100;
  document.getElementById('c-minstk').value  = c.VB_MIN_STAKE||2;
  document.getElementById('c-maxodds').value = parseFloat(c.VB_MAX_ODDS)||0;
  document.getElementById('c-maxedge').value = parseFloat(c.VB_MAX_EDGE)||0;
  document.getElementById('c-kelly').checked= c.VB_USE_KELLY==='true'||c.VB_USE_KELLY===true;
  document.getElementById('c-fulllimit').checked= c.VB_FULL_LIMIT==='true'||c.VB_FULL_LIMIT===true;
  document.getElementById('c-poll').value   = c.POLL_INTERVAL||5;
  document.getElementById('c-fid').value    = c.BETBURGER_FILTER_ID_VALUEBET||'';
  document.getElementById('c-email').value  = c.BETBURGER_EMAIL||'';
  document.getElementById('c-funder').value = c.POLYMARKET_FUNDER||'';
  // Лайв
  document.getElementById('lv-roi').value    = parseFloat(c.LV_MIN_ROI||.04)*100;
  document.getElementById('lv-liq').value    = c.LV_MIN_LIQUIDITY||30;
  document.getElementById('lv-stk').value    = parseFloat(c.LV_STAKE_PCT||.01)*100;
  document.getElementById('lv-maxstk').value = parseFloat(c.LV_MAX_STAKE_PCT||.05)*100;
  document.getElementById('lv-minstk').value = c.LV_MIN_STAKE||2;
  document.getElementById('lv-ttl').value    = c.LV_ORDER_TTL_SECS||30;
  document.getElementById('lv-maxodds').value= parseFloat(c.LV_MAX_ODDS)||0;
  document.getElementById('lv-maxedge').value= parseFloat(c.LV_MAX_EDGE)||0;
  document.getElementById('lv-kelly').checked= c.LV_USE_KELLY==='true'||c.LV_USE_KELLY===true;
  document.getElementById('lv-poll').value   = c.LV_POLL_INTERVAL||5;
  document.getElementById('lv-fid').value    = c.BETBURGER_FILTER_ID_LIVE||'';
  // Resell
  document.getElementById('c-vb-resell').checked = c.VB_RESELL_ENABLED==='true'||c.VB_RESELL_ENABLED===true;
  document.getElementById('c-vb-markup').value   = c.VB_RESELL_MARKUP||2;
  document.getElementById('c-vb-fallback').value = c.VB_RESELL_FALLBACK||'keep';
  document.getElementById('c-pm-ttl').value      = c.PM_ORDER_TTL_SECS||3600;
  document.getElementById('c-lv-resell').checked = c.LV_RESELL_ENABLED==='true'||c.LV_RESELL_ENABLED===true;
  document.getElementById('c-lv-markup').value   = c.LV_RESELL_MARKUP||3;
  document.getElementById('c-lv-fallback').value = c.LV_RESELL_FALLBACK||'keep';
  // Фильтры спортов
  document.getElementById('c-excl-sports').value = c.EXCLUDED_SPORTS||'';
  document.getElementById('c-excl-leagues').value = c.EXCLUDED_LEAGUES||'';
  document.getElementById('c-maxmap').value       = c.ESPORT_MAX_MAP||3;
}

async function saveCfg() {
  const body={
    // Прематч
    VB_MIN_ROI:        (parseFloat(document.getElementById('c-roi').value)/100).toFixed(4),
    MIN_LIQUIDITY:     document.getElementById('c-liq').value,
    VB_STAKE_PCT:      (parseFloat(document.getElementById('c-stk').value)/100).toFixed(4),
    VB_MAX_STAKE_PCT:  (parseFloat(document.getElementById('c-maxstk').value)/100).toFixed(4),
    VB_MIN_STAKE:      document.getElementById('c-minstk').value,
    VB_USE_KELLY:      document.getElementById('c-kelly').checked?'true':'false',
    VB_FULL_LIMIT:     document.getElementById('c-fulllimit').checked?'true':'false',
    VB_MAX_ODDS:       String(parseFloat(document.getElementById('c-maxodds').value)||0),
    VB_MAX_EDGE:       String(parseFloat(document.getElementById('c-maxedge').value)||0),
    POLL_INTERVAL:     document.getElementById('c-poll').value,
    BETBURGER_FILTER_ID_VALUEBET: document.getElementById('c-fid').value,
    BETBURGER_EMAIL:   document.getElementById('c-email').value,
    POLYMARKET_FUNDER: document.getElementById('c-funder').value,
    // Лайв
    LV_MIN_ROI:        (parseFloat(document.getElementById('lv-roi').value)/100).toFixed(4),
    LV_MIN_LIQUIDITY:  document.getElementById('lv-liq').value,
    LV_STAKE_PCT:      (parseFloat(document.getElementById('lv-stk').value)/100).toFixed(4),
    LV_MAX_STAKE_PCT:  (parseFloat(document.getElementById('lv-maxstk').value)/100).toFixed(4),
    LV_MIN_STAKE:      document.getElementById('lv-minstk').value,
    LV_ORDER_TTL_SECS: document.getElementById('lv-ttl').value,
    LV_USE_KELLY:      document.getElementById('lv-kelly').checked?'true':'false',
    LV_MAX_ODDS:       String(parseFloat(document.getElementById('lv-maxodds').value)||0),
    LV_MAX_EDGE:       String(parseFloat(document.getElementById('lv-maxedge').value)||0),
    LV_POLL_INTERVAL:  document.getElementById('lv-poll').value,
    BETBURGER_FILTER_ID_LIVE: document.getElementById('lv-fid').value,
    // Resell
    VB_RESELL_ENABLED:  document.getElementById('c-vb-resell').checked?'true':'false',
    VB_RESELL_MARKUP:   document.getElementById('c-vb-markup').value,
    VB_RESELL_FALLBACK: document.getElementById('c-vb-fallback').value,
    PM_ORDER_TTL_SECS:  document.getElementById('c-pm-ttl').value,
    LV_RESELL_ENABLED:  document.getElementById('c-lv-resell').checked?'true':'false',
    LV_RESELL_MARKUP:   document.getElementById('c-lv-markup').value,
    LV_RESELL_FALLBACK: document.getElementById('c-lv-fallback').value,
    // Фильтры
    EXCLUDED_SPORTS:    document.getElementById('c-excl-sports').value.trim(),
    EXCLUDED_LEAGUES:   document.getElementById('c-excl-leagues').value.trim(),
    ESPORT_MAX_MAP:     document.getElementById('c-maxmap').value,
  };
  if(DEMO){ toast('Демо: настройки не сохранены на сервере'); }
  else {
    const d=await api('/api/config',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});
    if(!d.ok) { toast('\u2717 '+d.error,true); return; }
  }
  cachedCfg={...cachedCfg,...body};
  const sm=document.getElementById('savemsg');
  sm.style.opacity='1'; setTimeout(()=>sm.style.opacity='0',2500);
  toast('\u2713 Настройки сохранены');
  loadStats();
}

// ═══════════════════════════════════════════════════════
//  BANKROLL
// ═══════════════════════════════════════════════════════
async function saveBankroll() {
  const v=parseFloat(document.getElementById('brl-inp').value);
  if(!v||v<=0){toast('Введи корректную сумму',true);return;}
  if(DEMO){toast(`Демо: банкролл = $${v.toFixed(2)}`);return;}
  const d=await api('/api/bankroll',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({amount:v})});
  if(d.ok){toast(`✓ Банкролл: $${v.toFixed(2)}`);loadStats();}
  else toast('✗ '+d.error,true);
}

// ═══════════════════════════════════════════════════════
//  BOT CONTROL
// ═══════════════════════════════════════════════════════
async function startBot() {
  if(DEMO){toast('Демо: сервер не запущен — нельзя управлять ботом',true);return;}
  const d=await api('/api/bot/start',{method:'POST'});
  if(d.ok){toast('\u25b6 Бот запускается...'); setTimeout(pollBotStatus,1000);}
  else toast('\u2717 '+d.error,true);
}
async function stopBot() {
  if(DEMO){toast('Демо: сервер не запущен',true);return;}
  const d=await api('/api/bot/stop',{method:'POST'});
  if(d.ok){toast('\u25a0 Бот останавливается...'); setTimeout(pollBotStatus,1000);}
  else toast('\u2717 '+d.error,true);
}

async function startLiveBot() {
  if(DEMO){toast('Демо: сервер не запущен',true);return;}
  const d=await api('/api/live/start',{method:'POST'});
  if(d.ok){toast('\u26a1 Live бот запускается...'); setTimeout(pollBotStatus,1000);}
  else toast('\u2717 '+d.error,true);
}
async function stopLiveBot() {
  if(DEMO){toast('Демо: сервер не запущен',true);return;}
  const d=await api('/api/live/stop',{method:'POST'});
  if(d.ok){toast('\u25a0 Live бот останавливается...'); setTimeout(pollBotStatus,1000);}
  else toast('\u2717 '+d.error,true);
}

async function pollBotStatus() {
  if(DEMO)return;
  const d=await api('/api/bot/status');
  if(d.ok) updateBotUI(d.running, d.uptime, d.live_running, d.live_uptime);
}

// ── Wallet snapshot (хедер: PM Cash / Позиции / Итого / Редим) ───────────────
let _walletTimer = null;
async function loadWallet(force) {
  if(DEMO) return;
  try {
    const url = force ? '/api/wallet/invalidate' : null;
    if(force) await api('/api/wallet/invalidate', {method:'POST'});
    const d = await api('/api/wallet');
    if(!d.ok) return;
    const setH = (id, val) => { const el=document.getElementById(id); if(el) el.textContent=val; };
    setH('hpm-cash',    d.cash    >= 0 ? '$'+d.cash.toFixed(2)    : '—');
    setH('hpm-portval', d.portfolio_value >= 0 ? '$'+d.portfolio_value.toFixed(2) : '—');
    setH('hpm-total',   d.total   >= 0 ? '$'+d.total.toFixed(2)   : '—');
    const redeemEl = document.getElementById('hpm-redeem');
    const redeemBtn = document.getElementById('hpm-redeem-btn');
    if(redeemEl) {
      redeemEl.textContent = d.redeemable > 0 ? '$'+d.redeemable.toFixed(2) : '—';
      redeemEl.style.color = d.redeemable > 0 ? '#2ecc71' : '#555';
    }
    if(redeemBtn) {
      redeemBtn.style.opacity = d.redeemable > 0 ? '1' : '0.4';
    }
  } catch(e) { /* silent */ }
}
async function headerRedeem() {
  const btn = document.getElementById('hpm-redeem-btn');
  if(btn) { btn.textContent='⏳ ...'; btn.disabled=true; }
  try {
    const d = await api('/api/redeem', {method:'POST'});
    if(d.ok) {
      if(d.redeemed > 0) {
        toast('✅ Redeem: '+d.redeemed+' позиций · $'+d.amount.toFixed(2));
      } else if(d.msg) {
        toast('ℹ️ '+d.msg);
      } else {
        toast('✅ Redeem завершён');
      }
      if(d.errors && d.errors.length) {
        console.warn('Redeem errors:', d.errors);
      }
      setTimeout(()=>loadWallet(true), 2000);
    } else {
      toast('⚠️ Redeem: '+(d.error||'ошибка'), true);
      console.error('Redeem error:', d);
    }
  } finally {
    if(btn) { btn.textContent='↑ REDEEM'; btn.disabled=false; }
  }
}
// Авто-обновление хедера каждые 5 мин
function startWalletRefresh() {
  if(_walletTimer) clearInterval(_walletTimer);
  loadWallet(false);
  _walletTimer = setInterval(()=>loadWallet(false), 5*60*1000);
}
function updateBotUI(running, uptime, liveRun, liveUp) {
  botRunning=!!running; botUptime=uptime||0;
  liveRunning=!!liveRun; liveUptime=liveUp||0;
  // Прематч статус
  document.getElementById('dot').className='dot'+(running?' on':'');
  document.getElementById('bst').textContent=running?'РАБОТАЕТ':'ОСТАНОВЛЕН';
  document.getElementById('btn-start').style.display=running?'none':'';
  document.getElementById('btn-stop').style.display=running?'':'none';
  // Лайв кнопки
  const btnLs = document.getElementById('btn-live-start');
  const btnLx = document.getElementById('btn-live-stop');
  if(btnLs) btnLs.style.display=liveRun?'none':'';
  if(btnLx) btnLx.style.display=liveRun?'':'none';
  if(liveRun && btnLx) btnLx.textContent='\u25a0 СТОП ЛАЙВ \u26a1';
}

// ═══════════════════════════════════════════════════════
//  LOG
// ═══════════════════════════════════════════════════════
async function loadLog() {
  let lines;
  if(DEMO) lines=DEMO_LOG;
  else {
    const d=await api('/api/bot/log?lines=100');
    lines=d.ok?d.lines:DEMO_LOG;
  }
  const el=document.getElementById('logbox');
  el.innerHTML=lines.map(line=>{
    let cls='li';
    if(/error|exception|traceback/i.test(line)) cls='le';
    else if(/warn/i.test(line)) cls='lw';
    else if(/✅|поставлено|placed/i.test(line)) cls='lb';
    else if(/skip|пропуск/i.test(line)) cls='ls';
    return `<div class="ll ${cls}">${line.replace(/</g,'&lt;')}</div>`;
  }).join('');
  if(document.getElementById('log-auto')?.checked) el.scrollTop=el.scrollHeight;
}

// ═══════════════════════════════════════════════════════
//  SETTLE
// ═══════════════════════════════════════════════════════
function openSettle(id, bet) {
  settleId=id; settleBet=bet; settleRes=null;
  document.querySelectorAll('.rbtn').forEach(b=>b.classList.remove('on'));
  document.getElementById('pnl-inp').value='';
  const cost   = bet.cost_usdc   != null ? bet.cost_usdc   : (bet.shares||bet.stake||0) * (bet.stake_price||bet.bb_price||0);
  const payout = bet.payout_target != null ? bet.payout_target : (bet.shares||bet.stake||0);
  document.getElementById('minfo').innerHTML=`
    <div><span class="ml">Событие: </span><span class="mv">${bet.home} vs ${bet.away}</span></div>
    <div><span class="ml">Исход:   </span><span class="mv">${bet.outcome_name}${bet.market_param?' ('+bet.market_param+')':''}</span></div>
    <div><span class="ml">Ставка:  </span><span class="mv">$${f(cost)} (${f(bet.shares||bet.stake||0,2)} shares @ ${f(bet.stake_price||bet.bb_price||0,3)})</span></div>
    <div><span class="ml">Выигрыш: </span><span class="mv">$${f(payout)} при победе</span></div>
    <div><span class="ml">Матч:    </span><span class="mv">${bet.started_at_fmt}</span></div>`;
  document.getElementById('moverlay').classList.add('on');
}
function closeSettle(){ document.getElementById('moverlay').classList.remove('on'); document.getElementById('sell-row').style.display='none'; settleId=null; }
let sellMode = 'price'; // 'price' или 'proceeds'

function selRes(r) {
  settleRes=r;
  document.querySelectorAll('.rbtn').forEach(b=>b.classList.remove('on'));
  document.querySelector('.rbtn.'+r)?.classList.add('on');
  const sellRow = document.getElementById('sell-row');

  if(r === 'sold') {
    // Показать блок продажи и запросить данные с API
    sellRow.style.display = 'block';
    document.getElementById('sell-inp').value = '';
    document.getElementById('pnl-inp').value = '';
    document.getElementById('sell-info').textContent = 'Загрузка данных с Polymarket...';
    fetchSellInfo();
  } else {
    sellRow.style.display = 'none';
    if(settleBet) {
      const cost   = settleBet.cost_usdc   != null ? settleBet.cost_usdc   : (settleBet.shares||settleBet.stake||0) * (settleBet.stake_price||settleBet.bb_price||0);
      const payout = settleBet.payout_target != null ? settleBet.payout_target : (settleBet.shares||settleBet.stake||0);
      if(r==='won')  document.getElementById('pnl-inp').value = (payout - cost).toFixed(2);
      else if(r==='lost') document.getElementById('pnl-inp').value = (-cost).toFixed(2);
      else document.getElementById('pnl-inp').value='0';
    }
  }
}

function setSellMode(mode) {
  sellMode = mode;
  document.getElementById('sm-price').classList.toggle('active', mode === 'price');
  document.getElementById('sm-proceeds').classList.toggle('active', mode === 'proceeds');
  document.getElementById('sell-label').textContent = mode === 'price' ? 'Цена продажи' : 'Сумма продажи $';
  document.getElementById('sell-inp').placeholder = mode === 'price' ? '0.00 - 1.00' : 'сумма в USDC';
  document.getElementById('sell-inp').step = mode === 'price' ? '0.001' : '0.01';
  document.getElementById('sell-inp').value = '';
  document.getElementById('pnl-inp').value = '';
}

function calcSoldPnl() {
  if(!settleBet) return;
  const shares = settleBet.shares || settleBet.stake || 0;
  const entryP = settleBet.stake_price || settleBet.bb_price || 0;
  const cost   = shares * entryP;
  const val    = parseFloat(document.getElementById('sell-inp').value) || 0;

  let profit = 0;
  if(sellMode === 'price') {
    const proceeds = shares * val;
    profit = proceeds - cost;
    document.getElementById('sell-info').textContent =
      `${f(shares,2)} shares × ${f(val,4)} = $${f(proceeds)} proceeds | cost $${f(cost)}`;
  } else {
    profit = val - cost;
    document.getElementById('sell-info').textContent =
      `Proceeds $${f(val)} − cost $${f(cost)}`;
  }
  document.getElementById('pnl-inp').value = profit.toFixed(2);
}

async function fetchSellInfo() {
  if(!settleId) return;
  try {
    const d = await api(`/api/bet/${settleId}/sell-info`);
    if(d.ok) {
      if(d.has_sell_trades) {
        // Есть реальные SELL-трейды — заполняем автоматически
        document.getElementById('sell-inp').value = d.sell_price.toFixed(4);
        document.getElementById('pnl-inp').value = d.profit.toFixed(2);
        document.getElementById('sell-info').innerHTML =
          `<span style="color:#00e87a">Найдено ${d.sell_trades.length} SELL-трейд(ов)</span> | ` +
          `Продано ${f(d.shares_sold,2)} shares @ ${f(d.sell_price,4)} = $${f(d.proceeds)} | P&L $${f(d.profit)}`;
      } else if(d.sell_price > 0) {
        // Нет SELL-трейдов, но есть текущая цена
        document.getElementById('sell-inp').value = d.sell_price.toFixed(4);
        document.getElementById('pnl-inp').value = d.profit.toFixed(2);
        document.getElementById('sell-info').innerHTML =
          `<span style="color:#ffb800">SELL-трейды не найдены</span> — используется текущая цена токена: ${f(d.sell_price,4)}`;
      } else {
        document.getElementById('sell-info').innerHTML =
          `<span style="color:#e74c3c">Не удалось получить цену</span> — введите вручную`;
      }
    } else {
      document.getElementById('sell-info').innerHTML =
        `<span style="color:#e74c3c">Ошибка: ${d.error}</span> — введите вручную`;
    }
  } catch(e) {
    document.getElementById('sell-info').innerHTML =
      `<span style="color:#e74c3c">Ошибка запроса</span> — введите вручную`;
  }
}

async function confirmSettle() {
  if(!settleId||!settleRes){toast('Выбери результат',true);return;}
  const profit=parseFloat(document.getElementById('pnl-inp').value)||0;

  // Для SOLD — получаем sell_price
  let sellPriceVal = 0;
  if(settleRes === 'sold') {
    const sellInp = parseFloat(document.getElementById('sell-inp').value) || 0;
    if(sellMode === 'price') {
      sellPriceVal = sellInp;
    } else if(settleBet) {
      // Режим "по сумме" — вычисляем цену из proceeds
      const shares = settleBet.shares || settleBet.stake || 0;
      sellPriceVal = shares > 0 ? (sellInp / shares) : 0;
    }
  }

  if(DEMO){
    toast(`✓ Демо: ставка #${settleId} → ${settleRes}  P&L ${profit>=0?'+':''}$${f(Math.abs(profit))}`);
    closeSettle(); return;
  }
  const body = {result: settleRes, profit};
  if(settleRes === 'sold') body.sell_price = sellPriceVal;

  const d=await api(`/api/settle/${settleId}`,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});
  if(d.ok){
    toast(`✓ Ставка #${settleId} → ${settleRes}  P&L ${profit>=0?'+':''}$${f(Math.abs(profit))}`);
    closeSettle(); loadStats(); loadBets();
    if(curPage==='bets') loadBetsPage();
  } else toast('✗ '+d.error,true);
}

// ═══════════════════════════════════════════════════════
//  UPTIME
// ═══════════════════════════════════════════════════════
setInterval(()=>{
  if(botRunning) botUptime++;
  const h=Math.floor(botUptime/3600), m=Math.floor(botUptime%3600/60), s=botUptime%60;
  document.getElementById('upt').textContent=
    String(h).padStart(2,'0')+':'+String(m).padStart(2,'0')+':'+String(s).padStart(2,'0');
},1000);

// ═══════════════════════════════════════════════════════
//  AUTO REFRESH
// ═══════════════════════════════════════════════════════
let _autoRefresh   = true;
let _lastBetsHash  = '';
let _refreshTimer  = null;

function toggleAutoRefresh() {
  _autoRefresh = !_autoRefresh;
  const btn = document.getElementById('ar-toggle');
  if (btn) {
    btn.textContent  = _autoRefresh ? '⏸ АВТО' : '▶ АВТО';
    btn.style.color  = _autoRefresh ? '#00e87a' : '#555';
    btn.style.borderColor = _autoRefresh ? '#00e87a44' : '#333';
  }
  if (_autoRefresh) _scheduleRefresh();
}

function _scheduleRefresh() {
  if (_refreshTimer) clearTimeout(_refreshTimer);
  _refreshTimer = setTimeout(_autoRefreshTick, 10000);
}

async function _autoRefreshTick() {
  if (!_autoRefresh) return;
  try {
    const d = await api('/api/stats');
    if (!d.ok) return;

    // Обновляем статистику всегда (лёгкий запрос)
    if (typeof renderStats === 'function') renderStats(d);
    else await loadStats();

    // Обновляем таблицы ТОЛЬКО если изменился хэш
    const newHash = d.bets_hash || '';
    if (newHash && newHash !== _lastBetsHash) {
      _lastBetsHash = newHash;
      if (curPage === 'dash') await loadBets();
      if (curPage === 'bets') await loadBetsPage();
    }
    if (curPage === 'log') await loadLog();
  } catch(e) {}
  _scheduleRefresh();
}

// Запускаем
_scheduleRefresh();
setInterval(pollBotStatus, 4000);

// ═══════════════════════════════════════════════════════
//  INIT
// ═══════════════════════════════════════════════════════
async function init() {
  await loadCfg();
  await loadStats();
  await loadBets();
  startWalletRefresh();  // Хедер: PM Cash / Позиции / Итого / Редим
}

// ── PORTFOLIO ────────────────────────────────────────────────────────────────

// Редактирование свободного USDC
function pmCashEditToggle() {
  const wrap = document.getElementById('pm-cash-edit-wrap');
  if (!wrap) return;
  const shown = wrap.style.display !== 'none';
  wrap.style.display = shown ? 'none' : 'block';
  if (!shown) {
    const inp = document.getElementById('pm-cash-inp');
    const cur = document.getElementById('pm-cash');
    if (inp && cur) {
      const v = parseFloat((cur.textContent||'').replace('$',''));
      if (!isNaN(v)) inp.value = v.toFixed(2);
      setTimeout(() => inp.focus(), 50);
    }
  }
}

async function pmCashSave() {
  const inp = document.getElementById('pm-cash-inp');
  if (!inp) return;
  const val = parseFloat(inp.value);
  if (isNaN(val) || val < 0) { alert('Введите корректную сумму'); return; }
  const r = await api('/api/free-usdc', {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({amount: val})});
  if (r.ok) {
    document.getElementById('pm-cash').textContent = '$' + val.toFixed(2);
    document.getElementById('hpm-cash') && (document.getElementById('hpm-cash').textContent = '$' + val.toFixed(2));
    document.getElementById('pm-cash-edit-wrap').style.display = 'none';
  } else {
    alert('Ошибка сохранения: ' + (r.error||'unknown'));
  }
}

// ── SIGNALS ──────────────────────────────────────────────────────────────────
async function loadSignals() {
  document.getElementById('signals-cards').innerHTML =
    '<div style="color:#555;text-align:center;padding:40px">Загрузка...</div>';

  const d = await api('/api/debug/betburger-raw');

  // Meta bar
  const meta = document.getElementById('signals-meta');
  if (meta) {
    const chip = (label, val, color) =>
      `<div style="background:#111;border:1px solid #1a1a1a;padding:6px 14px;border-radius:2px;font-family:monospace;font-size:11px">
        <span style="color:#555">${label}: </span><span style="color:${color};font-weight:700">${val}</span>
      </div>`;
    meta.innerHTML = d.ok
      ? chip('Сохранено', d.saved_at ? d.saved_at.replace('T',' ').slice(0,19) : '\u2014', '#aaa')
        + chip('Всего бетов', d.total_bets, '#00e87a')
        + chip('Всего арбов', d.total_arbs, '#9b59b6')
        + chip('Polymarket', d.polymarket_bets, '#3498db')
      : chip('Ошибка', d.error||'?', '#e74c3c');
  }

  if (!d.ok) {
    document.getElementById('signals-cards').innerHTML =
      `<div style="color:#e74c3c;text-align:center;padding:40px">${d.error}</div>`;
    return;
  }

  // Edge диагностика — arb.percent от BetBurger для каждого PM бета
  const edgeWrap = document.getElementById('signals-edge-summary');
  if (edgeWrap && d.edge_summary && d.edge_summary.length > 0) {
    edgeWrap.innerHTML = d.edge_summary.map(e => {
      const pct = e.arb_percent;
      const found = e.arb_found;
      return `<div style="display:flex;gap:12px;align-items:center;padding:4px 0;border-bottom:1px solid #0d0d0d;font-family:monospace;font-size:11px">
        <span style="color:#aaa;flex:1">${e.event||'—'}</span>
        ${found
          ? `<span style="color:#00e87a;font-weight:700">arb.percent = ${pct}%</span>`
          : `<span style="color:#e74c3c">⚠ arb не найден — edge = 0, ставка пропущена</span>`
        }
      </div>`;
    }).join('');
  } else if (edgeWrap) {
    edgeWrap.innerHTML = '<div style="color:#333;font-size:11px;font-family:monospace">— нет данных —</div>';
  }

  // Keys
  const bkEl = document.getElementById('signals-bet-keys');
  if (bkEl) bkEl.innerHTML = (d.all_bet_keys||[])
    .map(k => `<span style="display:inline-block;margin:2px 6px 2px 0;background:#0d1f2d;padding:2px 7px;border-radius:2px">${k}</span>`)
    .join('');

  const akEl = document.getElementById('signals-arb-keys');
  if (akEl) akEl.innerHTML = (d.all_arb_keys||[])
    .map(k => `<span style="display:inline-block;margin:2px 6px 2px 0;background:#1a0d2d;padding:2px 7px;border-radius:2px">${k}</span>`)
    .join('');

  // Cards
  const cards = document.getElementById('signals-cards');
  if (!d.sample_bets || d.sample_bets.length === 0) {
    cards.innerHTML = '<div style="color:#555;text-align:center;padding:40px">Нет Polymarket бетов в последнем тике</div>';
    return;
  }

  const fv = (v) => {
    if (v === null || v === undefined) return '<span style="color:#333">null</span>';
    if (typeof v === 'boolean') return `<span style="color:${v?'#00e87a':'#e74c3c'}">${v}</span>`;
    if (typeof v === 'number') return `<span style="color:#f39c12">${v}</span>`;
    if (typeof v === 'string' && v.length > 80) return `<span style="color:#aaa" title="${v.replace(/"/g,'&quot;')}">${v.slice(0,80)}…</span>`;
    return `<span style="color:#ddd">${String(v).replace(/</g,'&lt;')}</span>`;
  };

  const renderObj = (obj, color) => Object.entries(obj)
    .map(([k,v]) => `<div style="display:flex;gap:8px;padding:3px 0;border-bottom:1px solid #0d0d0d">
      <span style="color:${color};font-family:monospace;font-size:11px;min-width:200px;flex-shrink:0">${k}</span>
      <span style="font-family:monospace;font-size:11px;word-break:break-all">${fv(v)}</span>
    </div>`).join('');

  cards.innerHTML = d.sample_bets.map((bet, i) => {
    const arb = (d.sample_arbs||[])[i] || {};
    const hasArb = Object.keys(arb).length > 0;
    return `
    <div style="background:#080808;border:1px solid #1a1a1a;border-radius:3px;overflow:hidden">
      <div style="background:#0d1a0d;padding:10px 16px;display:flex;justify-content:space-between;align-items:center">
        <span style="color:#00e87a;font-family:monospace;font-weight:700;font-size:13px">
          ${bet.team1_name||'?'} vs ${bet.team2_name||'?'}
        </span>
        <div style="display:flex;gap:8px">
          <span style="background:#003d1a;color:#00e87a;padding:2px 8px;font-family:monospace;font-size:11px;border-radius:2px">
            koef: ${bet.koef||'?'}
          </span>
          <span style="background:#1a1a00;color:#f39c12;padding:2px 8px;font-family:monospace;font-size:11px;border-radius:2px">
            depth: $${bet.market_depth||0}
          </span>
          ${hasArb ? `<span style="background:#1a0d2d;color:#9b59b6;padding:2px 8px;font-family:monospace;font-size:11px;border-radius:2px">
            edge: ${arb.percent||0}%
          </span>` : ''}
        </div>
      </div>
      <div style="display:grid;grid-template-columns:${hasArb?'1fr 1fr':'1fr'};gap:0">
        <div style="padding:12px 16px">
          <div style="color:#3498db;font-size:10px;letter-spacing:1px;margin-bottom:8px">BET ОБЪЕКТ</div>
          ${renderObj(bet, '#3498db')}
        </div>
        ${hasArb ? `<div style="padding:12px 16px;border-left:1px solid #1a1a1a">
          <div style="color:#9b59b6;font-size:10px;letter-spacing:1px;margin-bottom:8px">ARB ОБЪЕКТ</div>
          ${renderObj(arb, '#9b59b6')}
        </div>` : ''}
      </div>
    </div>`;
  }).join('');
}

async function loadPortfolio() {
  ['pm-portval','pm-orders','pm-total'].forEach(id => {
    const el = document.getElementById(id);
    if (el) el.textContent = '...';
  });
  // После загрузки портфолио обновляем хедер через /api/wallet (со сбросом кеша)
  loadWallet(true);
  // Загружаем свободный USDC из нашего API
  const cashData = await api('/api/free-usdc');
  const freeUsdc = (cashData.ok && cashData.free_usdc !== null) ? cashData.free_usdc : null;
  const cashEl = document.getElementById('pm-cash');
  if (cashEl) cashEl.textContent = freeUsdc !== null ? '$' + freeUsdc.toFixed(2) : '— (введите)';
  const hCash = document.getElementById('hpm-cash');
  if (hCash) hCash.textContent = freeUsdc !== null ? '$' + freeUsdc.toFixed(2) : '\u2014';

  const body = document.getElementById('pm-positions-body');
  if (body) body.innerHTML = '<tr><td colspan="8" style="text-align:center;color:#555;padding:24px">Загрузка данных с Polymarket...</td></tr>';

  const d = await api('/api/portfolio');
  if (!d.ok) {
    if (body) body.innerHTML = '<tr><td colspan="8" style="text-align:center;color:#e74c3c;padding:24px">Ошибка: ' + (d.error||'неизвестная') + '</td></tr>';
    return;
  }

  const setEl = (id, val) => { const e = document.getElementById(id); if(e) e.textContent = val; };
  const posVal = d.portfolio_value || 0;
  const costVal = (d.positions||[]).reduce((s,p) => s + (p.cost||0), 0);
  const displayVal = posVal > 0 ? posVal : costVal;
  const cash = freeUsdc !== null ? freeUsdc : 0;
  const totalDisplay = cash + displayVal;

  setEl('pm-portval', posVal > 0 ? '$' + posVal.toFixed(2) : '~$' + costVal.toFixed(2) + ' (cost)');
  setEl('pm-orders',  '$' + (d.open_orders_value||0).toFixed(2));
  setEl('pm-total',   '$' + totalDisplay.toFixed(2));
  setEl('hpm-portval', posVal > 0 ? '$' + posVal.toFixed(2) : '~$' + costVal.toFixed(2));
  setEl('hpm-total',   '$' + totalDisplay.toFixed(2));

  if (!d.positions || d.positions.length === 0) {
    const msg = d.error ? 'Ошибка Gamma API: '+d.error : 'Нет открытых позиций';
    if (body) body.innerHTML = '<tr><td colspan="8" style="text-align:center;color:#555;padding:24px">'+msg+'</td></tr>';
    return;
  }
  if (body) body.innerHTML = d.positions.map(p => {
    const pnlClass = p.pnl >= 0 ? 'pos' : 'neg';
    const pnlSign  = p.pnl >= 0 ? '+' : '';
    return `<tr>
      <td style="max-width:220px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${p.question}">${p.question}</td>
      <td><span class="badge">${p.outcome}</span></td>
      <td style="text-align:right">${p.size.toFixed(2)}</td>
      <td style="text-align:right">${(p.avg_price*100).toFixed(1)}¢</td>
      <td style="text-align:right">${p.cur_price > 0 ? (p.cur_price*100).toFixed(1)+'¢' : '\u2014'}</td>
      <td style="text-align:right">${p.cur_price > 0 ? '$'+p.value.toFixed(2) : '~$'+p.cost.toFixed(2)}</td>
      <td style="text-align:right;color:${p.cur_price>0?(p.pnl>=0?'#00e87a':'#e74c3c'):'#555'}">${p.cur_price>0?pnlSign+'$'+p.pnl.toFixed(2):'\u2014'}</td>
      <td style="text-align:right;color:${p.cur_price>0?(p.pnl>=0?'#00e87a':'#e74c3c'):'#555'}">${p.cur_price>0?pnlSign+p.pnl_pct.toFixed(1)+'%':'\u2014'}</td>
    </tr>`;
  }).join('');
}

async function purgePending() {
  if (!confirm('Удалить все pending ставки без order_id? Они заново проставятся при следующем тике бота.')) return;
  try {
    const d = await api('/api/bets/purge-pending', {method:'POST', headers:{'Content-Type':'application/json'}, body:'{}'});
    const el = document.getElementById('purge-result');
    el.style.display = 'block';
    if (d.ok) {
      el.textContent = `✓ Удалено ${d.deleted} битых pending записей. Запусти бота — они проставятся заново.`;
      el.style.color = 'var(--g1)';
    } else {
      el.textContent = '✗ Ошибка: ' + d.error;
      el.style.color = '#e74c3c';
    }
  } catch(e) { alert('Ошибка: ' + e); }
}


// ═══════════════════════════════════════════════════════
//  FEED BB PAGE
// ═══════════════════════════════════════════════════════
let feedData = { pre: null, live: null };
let feedMode = 'pre';

function feedTab(mode) {
  feedMode = mode;
  const pre  = document.getElementById('feed-tab-pre');
  const live = document.getElementById('feed-tab-live');
  const act  = 'background:#00e87a22;color:#00e87a;border-bottom:2px solid #00e87a;';
  const inact= 'background:transparent;color:var(--tx3);border-bottom:2px solid transparent;';
  pre.style.cssText  = 'padding:8px 22px;border:none;font-family:monospace;font-size:11px;font-weight:900;letter-spacing:1px;cursor:pointer;' + (mode==='pre'  ? act : inact);
  live.style.cssText = 'padding:8px 22px;border:none;font-family:monospace;font-size:11px;font-weight:900;letter-spacing:1px;cursor:pointer;' + (mode==='live' ? act : inact);
  renderFeed();
}

async function loadFeed() {
  const listEl  = document.getElementById('feed-list');
  const statsEl = document.getElementById('feed-stats');
  const savedEl = document.getElementById('feed-saved-at');
  listEl.innerHTML = '<div style="color:#555;text-align:center;padding:40px;font-family:monospace">Загрузка...</div>';

  // Загружаем оба источника параллельно
  const [rPre, rLive] = await Promise.allSettled([
    fetch('/api/debug/betburger-feed?mode=pre').then(r=>r.json()),
    fetch('/api/debug/betburger-feed?mode=live').then(r=>r.json()),
  ]);

  feedData.pre  = rPre.status  === 'fulfilled' ? rPre.value  : { ok: false, error: rPre.reason?.message || 'network error' };
  feedData.live = rLive.status === 'fulfilled' ? rLive.value : { ok: false, error: rLive.reason?.message || 'network error' };

  const d = feedMode === 'live' ? feedData.live : feedData.pre;
  if (d && d.saved_at) savedEl.textContent = 'обновлено: ' + d.saved_at.replace('T',' ').slice(0,19);
  renderFeed();
}

function renderFeed() {
  const listEl  = document.getElementById('feed-list');
  const statsEl = document.getElementById('feed-stats');
  const savedEl = document.getElementById('feed-saved-at');
  const d = feedMode === 'live' ? feedData.live : feedData.pre;

  if (!d) {
    listEl.innerHTML = '<div style="color:#555;text-align:center;padding:60px;font-family:monospace">Нажмите ОБНОВИТЬ</div>';
    statsEl.innerHTML = '';
    return;
  }
  if (!d.ok) {
    listEl.innerHTML = '<div style="color:#e74c3c;text-align:center;padding:40px;font-family:monospace">⚠ ' + (d.error||'Ошибка') + '</div>';
    statsEl.innerHTML = '';
    return;
  }

  if (d.saved_at) savedEl.textContent = 'обновлено: ' + d.saved_at.replace('T',' ').slice(0,19);

  // Stats bar
  const positiveEdge = (d.bets||[]).filter(b => (b.computed_edge||0) > 0).length;
  const passFilter   = (d.bets||[]).filter(b => b.passes_filter).length;
  statsEl.innerHTML = `
    <div style="background:#0a0a0a;border:1px solid var(--line);border-radius:3px;padding:7px 12px;font-family:monospace;font-size:11px">
      <span style="color:var(--tx2)">Всего бетов BB:</span> <b style="color:var(--tx)">${d.total_bets||0}</b>
    </div>
    <div style="background:#0a0a0a;border:1px solid var(--line);border-radius:3px;padding:7px 12px;font-family:monospace;font-size:11px">
      <span style="color:var(--tx2)">Polymarket:</span> <b style="color:#3498db">${(d.bets||[]).length}</b>
    </div>
    <div style="background:#0a0a0a;border:1px solid var(--line);border-radius:3px;padding:7px 12px;font-family:monospace;font-size:11px">
      <span style="color:var(--tx2)">Edge > 0:</span> <b style="color:#00e87a">${positiveEdge}</b>
    </div>
    <div style="background:#0a0a0a;border:1px solid var(--line);border-radius:3px;padding:7px 12px;font-family:monospace;font-size:11px">
      <span style="color:var(--tx2)">Прошли фильтр:</span> <b style="color:${passFilter>0?'#00e87a':'#e74c3c'}">${passFilter}</b>
    </div>
    <div style="background:#0a0a0a;border:1px solid var(--line);border-radius:3px;padding:7px 12px;font-family:monospace;font-size:11px">
      <span style="color:var(--tx2)">arb.percent (BB):</span> <b style="color:#e67e22">${d.total_arbs||0} арбов</b>
    </div>
  `;

  const bets = d.bets || [];
  if (!bets.length) {
    listEl.innerHTML = '<div style="color:#555;text-align:center;padding:60px;font-family:monospace">Нет Polymarket бетов в ответе</div>';
    return;
  }

  listEl.innerHTML = bets.map((b, i) => {
    const edge    = b.computed_edge ?? 0;   // велью% = middle_value (как сайт BB)
    const arbPct  = b.arb_percent ?? null;  // доходность вилки
    const edgeCol = edge > 5 ? '#00e87a' : edge > 0 ? '#f1c40f' : '#e74c3c';
    const passed  = b.passes_filter;
    const isLive  = b.is_live;

    const dl = b.direct_link_params || {};
    const ob = b.order_book || [];
    const obHtml = ob.length
      ? ob.map(lvl => `<span style="color:#888">odds=${lvl.odds?.toFixed(4)} → price=<b style="color:#aaa">${lvl.price?.toFixed(4)}</b> size=$${(lvl.size||0).toFixed(0)}</span>`).join('<br>')
      : '<span style="color:#555">— нет данных —</span>';

    // Все поля бета для раскрывашки
    const betFields = Object.entries(b.raw || {})
      .filter(([k]) => !['direct_link','bookmaker_event_direct_link'].includes(k))
      .map(([k,v]) => `<tr><td style="color:#555;padding:2px 8px 2px 0;white-space:nowrap">${k}</td><td style="color:#aaa;font-family:monospace;font-size:11px;word-break:break-all">${JSON.stringify(v)}</td></tr>`)
      .join('');

    const arbFields = b.arb_raw
      ? Object.entries(b.arb_raw)
          .filter(([k]) => !['bet1_id','bet2_id','bet3_id'].includes(k))
          .map(([k,v]) => `<tr><td style="color:#555;padding:2px 8px 2px 0;white-space:nowrap">${k}</td><td style="color:#9b59b6;font-family:monospace;font-size:11px;word-break:break-all">${JSON.stringify(v)}</td></tr>`)
          .join('')
      : '<tr><td colspan="2" style="color:#555">arb не найден</td></tr>';

    return `
    <div style="background:#080808;border:1px solid ${passed?'#00e87a44':'#1a1a1a'};border-radius:3px;border-left:3px solid ${edgeCol};overflow:hidden">
      <!-- HEADER ROW — кликабельный -->
      <div onclick="feedToggle(${i})" style="display:flex;align-items:center;gap:12px;padding:10px 14px;cursor:pointer;user-select:none">
        <span style="color:#555;font-family:monospace;font-size:10px;min-width:24px">#${i+1}</span>
        ${isLive ? '<span style="background:#e74c3c;color:#fff;font-size:9px;padding:2px 5px;border-radius:2px;font-weight:900">LIVE</span>' : ''}
        <span style="flex:1;color:var(--tx);font-size:12px;font-weight:700">${b.event_name||'—'}</span>
        <span style="color:#888;font-size:10px;min-width:80px">${b.league||''}</span>
        <span style="color:#aaa;font-size:11px;font-family:monospace;min-width:60px">koef <b>${(b.koef||0).toFixed(3)}</b></span>
        <span style="font-family:monospace;font-size:12px;font-weight:900;min-width:80px;text-align:right;color:${edgeCol}">
          EV ${edge>=0?'+':''}${edge.toFixed(2)}%
        </span>
        ${arbPct!=null ? `<span style="font-family:monospace;font-size:10px;color:#e67e22;min-width:70px;text-align:right" title="Доходность вилки arbs[].percent">Арб ${arbPct.toFixed(1)}%</span>` : ''}
        <span style="color:#555;font-size:14px;margin-left:4px" id="feed-arrow-${i}">▶</span>
        ${passed ? '<span style="color:#00e87a;font-size:10px;font-weight:900;margin-left:4px">✓ ФИЛЬТР</span>' : ''}
      </div>
      <!-- DETAIL PANEL — скрытый -->
      <div id="feed-detail-${i}" style="display:none;border-top:1px solid #1a1a1a">
        <!-- Quick stats -->
        <div style="display:grid;grid-template-columns:repeat(4,1fr);gap:1px;background:#111">
          <div style="background:#0a0a0a;padding:10px 12px">
            <div style="color:#555;font-size:9px;letter-spacing:1px;margin-bottom:3px">BB КОЭФ</div>
            <div style="color:var(--tx);font-family:monospace;font-size:14px;font-weight:700">${(b.koef||0).toFixed(4)}</div>
            <div style="color:#888;font-size:10px">= price ${(1/(b.koef||1)).toFixed(4)}</div>
          </div>
          <div style="background:#0a0a0a;padding:10px 12px">
            <div style="color:#555;font-size:9px;letter-spacing:1px;margin-bottom:3px">PM BEST ASK</div>
            <div style="color:${edgeCol};font-family:monospace;font-size:14px;font-weight:700">${ob.length ? ob[0].price?.toFixed(4) : '—'}</div>
            <div style="color:#888;font-size:10px">${ob.length ? 'odds '+ob[0].odds?.toFixed(3) : 'нет стакана'}</div>
          </div>
          <div style="background:#0a0a0a;padding:10px 12px">
            <div style="color:#555;font-size:9px;letter-spacing:1px;margin-bottom:3px">EV EDGE (велью%)</div>
            <div style="color:${edgeCol};font-family:monospace;font-size:14px;font-weight:700">${edge>=0?'+':''}${edge.toFixed(3)}%</div>
            <div style="color:#888;font-size:10px">= middle_value BB</div>
          </div>
          <div style="background:#0a0a0a;padding:10px 12px">
            <div style="color:#555;font-size:9px;letter-spacing:1px;margin-bottom:3px">АРБ% (вилка)</div>
            <div style="color:#e67e22;font-family:monospace;font-size:14px;font-weight:700">${arbPct!=null ? arbPct.toFixed(3)+'%' : '—'}</div>
            <div style="color:#888;font-size:10px">arbs[].percent</div>
          </div>
          <div style="background:#0a0a0a;padding:10px 12px">
            <div style="color:#555;font-size:9px;letter-spacing:1px;margin-bottom:3px">ЛИКВИДНОСТЬ</div>
            <div style="color:#3498db;font-family:monospace;font-size:14px;font-weight:700">$${(b.liquidity||0).toFixed(0)}</div>
            <div style="color:#888;font-size:10px">competitive ${(b.competitive||0).toFixed(3)}</div>
          </div>
        </div>
        <!-- Order book -->
        <div style="padding:12px 14px;border-bottom:1px solid #111">
          <div style="color:#555;font-size:9px;letter-spacing:1px;margin-bottom:6px">📊 СТАКАН (bestOffers)</div>
          <div style="font-family:monospace;font-size:11px;line-height:2">${obHtml}</div>
        </div>
        <!-- Arb data -->
        <div style="padding:12px 14px;border-bottom:1px solid #111">
          <div style="color:#555;font-size:9px;letter-spacing:1px;margin-bottom:6px">🎯 ARB ДАННЫЕ (из arbs[])</div>
          <table style="font-size:11px;line-height:1.9">${arbFields}</table>
        </div>
        <!-- Raw bet fields -->
        <div style="padding:12px 14px">
          <div style="color:#555;font-size:9px;letter-spacing:1px;margin-bottom:6px">📋 RAW BET ПОЛЯ</div>
          <table style="font-size:11px;line-height:1.9">${betFields}</table>
        </div>
      </div>
    </div>`;
  }).join('');
}

function feedToggle(i) {
  const panel = document.getElementById('feed-detail-'+i);
  const arrow = document.getElementById('feed-arrow-'+i);
  if (!panel) return;
  const open = panel.style.display !== 'none';
  panel.style.display = open ? 'none' : 'block';
  arrow.textContent = open ? '▶' : '▼';
}

// ═══════════════════════════════════════════════════════
//  HEDGE CALCULATOR
// ═══════════════════════════════════════════════════════

function loadHedge() {
  loadSavedCalcs();
  loadHedgePositions();
  loadHedgePairs();
}

function hedgeCalc() {
  const scenarios = [];
  document.querySelectorAll('.h-scenario').forEach(el => {
    scenarios.push({
      name: el.querySelector('.h-sc-name').value,
      exit_a: parseFloat(el.querySelector('.h-sc-exit-a').value) / 100,
      exit_b: parseFloat(el.querySelector('.h-sc-exit-b').value) / 100,
    });
  });

  const body = {
    price_a: parseFloat(document.getElementById('h-a-price').value) / 100,
    price_b: parseFloat(document.getElementById('h-b-price').value) / 100,
    budget: parseFloat(document.getElementById('h-budget').value),
    scenarios: scenarios,
  };

  fetch('/api/hedge/calculate', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify(body),
  }).then(r=>r.json()).then(d => {
    if (!d.ok) { alert(d.error || 'Error'); return; }
    showHedgeResult(d.result);
  }).catch(e => alert('Error: ' + e));
}

function showHedgeResult(r) {
  const el = document.getElementById('h-result');
  el.style.display = '';

  const sideA = document.getElementById('h-a-side').value || 'Position A';
  const priceA = document.getElementById('h-a-price').value;
  const sideB = document.getElementById('h-b-player').value || 'Position B';
  const tourney = document.getElementById('h-b-tournament').value || '';
  const bSide = document.getElementById('h-b-side').value;
  const priceB = document.getElementById('h-b-price').value;

  document.getElementById('h-res-a-label').textContent = sideA + ' @ ' + priceA + '¢';
  document.getElementById('h-res-a-shares').textContent = Math.floor(r.size_a) + ' shares';
  document.getElementById('h-res-a-cost').textContent = 'Cost: $' + r.cost_a.toFixed(2);

  document.getElementById('h-res-b-label').textContent = sideB + ' ' + tourney + ' ' + bSide + ' @ ' + priceB + '¢';
  document.getElementById('h-res-b-shares').textContent = Math.floor(r.size_b) + ' shares';
  document.getElementById('h-res-b-cost').textContent = 'Cost: $' + r.cost_b.toFixed(2);

  // Scenarios P&L
  let html = '';
  r.scenarios.forEach(s => {
    const color = s.total_pnl >= 0 ? '#4CAF50' : '#F44336';
    html += `<div style="border:1px solid #333;padding:10px;border-radius:4px">
      <div style="color:#888;font-size:10px;margin-bottom:4px">${s.name}</div>
      <div style="font-size:18px;font-weight:700;color:${color}">
        ${s.total_pnl >= 0 ? '+' : ''}$${s.total_pnl.toFixed(2)}
      </div>
    </div>`;
  });

  // Avg ROI
  const avgPnl = r.scenarios.reduce((s,x) => s + x.total_pnl, 0) / r.scenarios.length;
  const avgRoi = (avgPnl / r.total_cost * 100).toFixed(1);
  html += `<div style="border:1px solid #333;padding:10px;border-radius:4px">
    <div style="color:#888;font-size:10px;margin-bottom:4px">Avg ROI</div>
    <div style="font-size:18px;font-weight:700;color:#7C4DFF">${avgRoi}%</div>
  </div>`;

  document.getElementById('h-res-scenarios').innerHTML = html;

  // Note
  const note = document.getElementById('h-res-note');
  if (r.is_delta_neutral) {
    note.style.background = '#1a3a1a';
    note.style.color = '#4CAF50';
    note.textContent = 'Delta-neutral — balanced profit across scenarios';
  } else {
    note.style.background = '#3a3a1a';
    note.style.color = '#FF9800';
    note.textContent = 'Warning: P&L variance = $' + r.max_pnl_variance.toFixed(2);
  }

  // Store last result for saving
  window._lastHedgeResult = r;
}

function hedgeSave() {
  if (!window._lastHedgeResult) { alert('Calculate first'); return; }
  const r = window._lastHedgeResult;
  const scenarios = [];
  document.querySelectorAll('.h-scenario').forEach(el => {
    scenarios.push({
      name: el.querySelector('.h-sc-name').value,
      exit_a: parseFloat(el.querySelector('.h-sc-exit-a').value) / 100,
      exit_b: parseFloat(el.querySelector('.h-sc-exit-b').value) / 100,
    });
  });

  const p1 = document.getElementById('h-a-player1').value;
  const p2 = document.getElementById('h-a-player2').value;
  const tp = document.getElementById('h-b-player').value;
  const tn = document.getElementById('h-b-tournament').value;

  const body = {
    label: p1 + '-' + p2 + ' / ' + tp + ' / ' + tn,
    pos_a_name: p1 + ' vs ' + p2,
    pos_a_side: document.getElementById('h-a-side').value,
    pos_a_price: parseFloat(document.getElementById('h-a-price').value) / 100,
    pos_a_token_id: document.getElementById('h-a-token').value,
    pos_b_name: tn,
    pos_b_player: tp,
    pos_b_side: document.getElementById('h-b-side').value,
    pos_b_price: parseFloat(document.getElementById('h-b-price').value) / 100,
    pos_b_token_id: document.getElementById('h-b-token').value,
    budget: parseFloat(document.getElementById('h-budget').value),
    scenarios: scenarios,
    result: r,
    profit: r.profit,
    roi_pct: r.roi_pct,
  };

  fetch('/api/hedge/save-calc', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify(body),
  }).then(r=>r.json()).then(d => {
    if (d.ok) loadSavedCalcs();
    else alert(d.error || 'Error');
  });
}

function loadSavedCalcs() {
  fetch('/api/hedge/saved-calcs').then(r=>r.json()).then(d => {
    if (!d.ok || !d.calcs || d.calcs.length === 0) {
      document.getElementById('h-saved').textContent = '—';
      return;
    }
    let html = '';
    d.calcs.forEach(c => {
      const color = c.profit >= 0 ? '#4CAF50' : '#F44336';
      html += `<div style="display:flex;justify-content:space-between;align-items:center;padding:4px 8px;border:1px solid #333;border-radius:4px;margin-bottom:4px">
        <div>
          <b>${c.label}</b>
          <span style="color:${color};margin-left:8px">+$${parseFloat(c.profit).toFixed(2)} ${parseFloat(c.roi_pct).toFixed(1)}%</span>
          <span style="color:#555;margin-left:8px;font-size:10px">${c.created_at || ''}</span>
        </div>
        <div>
          <button class="btn" style="padding:2px 8px;font-size:10px" onclick="hedgeLoadCalc(${c.id})">Load</button>
          <button class="btn" style="padding:2px 8px;font-size:10px;background:#F44336" onclick="hedgeDeleteCalc(${c.id})">×</button>
        </div>
      </div>`;
    });
    document.getElementById('h-saved').innerHTML = html;
  });
}

function hedgeLoadCalc(id) {
  fetch('/api/hedge/saved-calcs').then(r=>r.json()).then(d => {
    if (!d.ok) return;
    const c = d.calcs.find(x => x.id === id);
    if (!c) return;
    // Fill form
    const parts = (c.pos_a_name || '').split(' vs ');
    if (parts.length === 2) {
      document.getElementById('h-a-player1').value = parts[0];
      document.getElementById('h-a-player2').value = parts[1];
    }
    document.getElementById('h-a-side').value = c.pos_a_side || '';
    document.getElementById('h-a-price').value = Math.round((c.pos_a_price || 0) * 100);
    document.getElementById('h-a-token').value = c.pos_a_token_id || '';
    document.getElementById('h-b-tournament').value = c.pos_b_name || '';
    document.getElementById('h-b-player').value = c.pos_b_player || '';
    document.getElementById('h-b-side').value = c.pos_b_side || 'YES';
    document.getElementById('h-b-price').value = Math.round((c.pos_b_price || 0) * 100);
    document.getElementById('h-b-token').value = c.pos_b_token_id || '';
    document.getElementById('h-budget').value = c.budget || 10000;
    // Fill scenarios
    const sc = c.scenarios || [];
    const scEls = document.querySelectorAll('.h-scenario');
    sc.forEach((s, i) => {
      if (scEls[i]) {
        scEls[i].querySelector('.h-sc-name').value = s.name || '';
        scEls[i].querySelector('.h-sc-exit-a').value = Math.round((s.exit_a || 0) * 100);
        scEls[i].querySelector('.h-sc-exit-b').value = Math.round((s.exit_b || 0) * 100);
      }
    });
    // Show result if available
    if (c.result && c.result.size_a) showHedgeResult(c.result);
  });
}

function hedgeDeleteCalc(id) {
  fetch('/api/hedge/delete-calc', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({id: id}),
  }).then(r=>r.json()).then(d => {
    if (d.ok) loadSavedCalcs();
  });
}

function hedgeClear() {
  document.getElementById('h-a-player1').value = '';
  document.getElementById('h-a-player2').value = '';
  document.getElementById('h-a-side').value = '';
  document.getElementById('h-a-price').value = '55';
  document.getElementById('h-a-token').value = '';
  document.getElementById('h-b-tournament').value = '';
  document.getElementById('h-b-player').value = '';
  document.getElementById('h-b-side').value = 'YES';
  document.getElementById('h-b-price').value = '55';
  document.getElementById('h-b-token').value = '';
  document.getElementById('h-budget').value = '10000';
  document.getElementById('h-result').style.display = 'none';
  window._lastHedgeResult = null;
}

function hedgeAnalyze() {
  const el = document.getElementById('h-opportunities');
  const budget = parseFloat(document.getElementById('h-budget').value) || 10000;
  el.innerHTML = '<div style="color:#7C4DFF">Starting analysis...</div>';

  fetch('/api/hedge/analyze', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({budget: budget}),
  }).then(r=>r.json()).then(d => {
    if (!d.ok) { el.textContent = d.error || 'Error'; return; }
    pollAnalyze(d.task_id, budget);
  }).catch(e => { el.textContent = 'Error: ' + e; });
}

function pollAnalyze(taskId, budget) {
  fetch('/api/hedge/analyze-status/' + taskId).then(r=>r.json()).then(d => {
    if (!d.ok) { return; }
    const el = document.getElementById('h-opportunities');

    // Прогресс-бар
    let html = '<div style="margin-bottom:8px">';
    html += '<div style="background:#222;border-radius:4px;height:20px;position:relative;overflow:hidden">';
    html += '<div style="background:#7C4DFF;height:100%;width:' + d.progress_pct + '%;transition:width 0.3s"></div>';
    html += '<span style="position:absolute;left:50%;top:50%;transform:translate(-50%,-50%);font-size:10px;color:#fff">' +
      d.progress_pct + '% — ' + (d.current_sport || '...') + '</span></div></div>';

    // Результаты по спортам
    const sr = d.sport_results || {};
    if (Object.keys(sr).length > 0) {
      html += '<table style="width:100%;font-size:10px;margin-bottom:8px;border-collapse:collapse">';
      html += '<tr style="color:#555"><th style="text-align:left">Sport</th><th>Pairs</th><th>Opportunities</th><th>Best ROI</th></tr>';
      for (const [sport, r] of Object.entries(sr)) {
        const c = r.opps > 0 ? '#4CAF50' : '#555';
        html += '<tr><td>' + sport + '</td><td style="text-align:center">' + r.pairs +
          '</td><td style="text-align:center;color:' + c + '">' + r.opps +
          '</td><td style="text-align:center;color:' + c + '">' + (r.best_roi || 0) + '%</td></tr>';
      }
      html += '</table>';
    }

    if (d.status === 'done') {
      if (!d.opportunities || d.opportunities.length === 0) {
        html += '<div style="color:#888">No profitable opportunities found (' + (d.total_pairs||0) + ' pairs)</div>';
      } else {
        html += '<div style="margin-bottom:8px;color:#888;font-size:10px">' +
          d.opportunities.length + ' opportunities from ' + (d.total_pairs||0) + ' pairs (budget: $' + budget + ')</div>';
        html += renderOpportunities(d.opportunities);
      }
      el.innerHTML = html;
      return;
    }

    if (d.status === 'error') {
      html += '<div style="color:#F44336">Error: ' + (d.error || 'unknown') + '</div>';
      el.innerHTML = html;
      return;
    }

    el.innerHTML = html;
    setTimeout(() => pollAnalyze(taskId, budget), 2000);
  }).catch(e => {
    document.getElementById('h-opportunities').textContent = 'Poll error: ' + e;
  });
}

function renderOpportunities(opps) {
  let html = '<table style="width:100%;font-size:11px;border-collapse:collapse"><tr style="color:#555;border-bottom:1px solid #333">' +
    '<th style="text-align:left;padding:4px">Match</th>' +
    '<th>Tournament Player</th>' +
    '<th>Match @</th><th>Tourney @</th>' +
    '<th style="color:#4CAF50">ROI</th><th style="color:#4CAF50">Profit</th>' +
    '<th>Sizes</th><th>Liquidity</th><th>Scenarios</th></tr>';
  opps.forEach(o => {
    const roiColor = o.roi_pct >= 5 ? '#4CAF50' : o.roi_pct >= 2 ? '#FF9800' : '#888';
    let scenHtml = '';
    (o.scenarios||[]).forEach(s => {
      scenHtml += '<div style="font-size:9px">' + s.name + ': exit ' + s.exit_b + 'c -> $' + s.pnl.toFixed(0) + '</div>';
    });
    const liqA = o.liq_a || 0;
    const liqB = o.liq_b || 0;
    const liqColor = (liqA >= o.size_a && liqB >= o.size_b) ? '#4CAF50' : '#F44336';
    html += '<tr style="border-bottom:1px solid #222">' +
      '<td style="padding:4px"><b>' + o.match + '</b><br><span style="color:#555;font-size:9px">' + (o.event||'') + ' ' + (o.is_knockout ? '🏆' : '📊') + (o.parallel_share > 0 ? ' ⚡' + o.parallel_share + '%' : '') + '</span></td>' +
      '<td>' + o.tourney_player + '</td>' +
      '<td style="text-align:center">' + o.match_price + 'c</td>' +
      '<td style="text-align:center">' + o.tourney_price + 'c</td>' +
      '<td style="text-align:center;color:' + roiColor + ';font-weight:700">' + o.roi_pct + '%</td>' +
      '<td style="text-align:center;color:#4CAF50">+$' + o.profit.toFixed(0) + '</td>' +
      '<td style="text-align:center;font-size:10px">' + o.size_a + ' / ' + o.size_b + '</td>' +
      '<td style="text-align:center;font-size:10px;color:' + liqColor + '">' + Math.floor(liqA) + ' / ' + Math.floor(liqB) + '</td>' +
      '<td>' + scenHtml + '</td>' +
      '</tr>';
  });
  html += '</table>';
  return html;
}

function hedgeScan(sport) {
  document.getElementById('h-pairs').textContent = sport ? ('Scanning ' + sport + '...') : 'Scanning all...';
  const body = sport ? JSON.stringify({sport: sport}) : '{}';
  fetch('/api/hedge/scan', {method:'POST', headers:{'Content-Type':'application/json'}, body: body}).then(r=>r.json()).then(d => {
    if (d.ok) {
      document.getElementById('h-pairs').textContent = 'Found ' + (d.count || 0) + ' pairs';
      loadHedgePairs();
    } else {
      document.getElementById('h-pairs').textContent = d.error || 'Error';
    }
  }).catch(e => {
    document.getElementById('h-pairs').textContent = 'Error: ' + e;
  });
}

function loadHedgePairs() {
  fetch('/api/hedge/pairs').then(r=>r.json()).then(d => {
    if (!d.ok || !d.pairs || d.pairs.length === 0) {
      document.getElementById('h-pairs').textContent = 'No pairs found. Click "Scan Markets" to discover.';
      return;
    }
    let html = '<table style="width:100%;font-size:11px"><tr style="color:#555"><th>Sport</th><th>Event</th><th>Match</th><th>Tournament</th><th>Status</th><th></th></tr>';
    d.pairs.forEach(p => {
      html += `<tr>
        <td>${p.sport}</td>
        <td>${p.event_name}</td>
        <td>${p.match_question || p.player_a + ' vs ' + p.player_b}</td>
        <td>${p.tourney_question || p.tourney_player}</td>
        <td>${p.status}</td>
        <td><button class="btn" style="padding:1px 6px;font-size:9px;color:#eee" onclick="hedgeUsePair('${p.pair_id}')">Use</button></td>
      </tr>`;
    });
    html += '</table>';
    document.getElementById('h-pairs').innerHTML = html;
  });
}

function hedgeUsePair(pairId) {
  fetch('/api/hedge/pairs').then(r=>r.json()).then(d => {
    if (!d.ok) return;
    const p = d.pairs.find(x => x.pair_id === pairId);
    if (!p) return;
    document.getElementById('h-a-player1').value = p.player_a || '';
    document.getElementById('h-a-player2').value = p.player_b || '';
    document.getElementById('h-a-side').value = p.player_a || '';
    document.getElementById('h-a-token').value = p.match_token_id || '';
    document.getElementById('h-b-tournament').value = p.event_name || '';
    document.getElementById('h-b-player').value = p.tourney_player || '';
    document.getElementById('h-b-token').value = p.tourney_token_id || '';
  });
}

function loadHedgePositions() {
  fetch('/api/hedge/positions').then(r=>r.json()).then(d => {
    if (!d.ok || !d.positions || d.positions.length === 0) {
      document.getElementById('h-positions').textContent = 'No active hedges';
      return;
    }
    let html = '<table style="width:100%;font-size:11px"><tr style="color:#555"><th>#</th><th>Pair</th><th>Size A</th><th>Size B</th><th>Budget</th><th>Expected</th><th>Status</th></tr>';
    d.positions.forEach(p => {
      const color = p.expected_profit >= 0 ? '#4CAF50' : '#F44336';
      html += `<tr>
        <td>${p.id}</td>
        <td>${p.pair_id}</td>
        <td>${parseFloat(p.size_a).toFixed(0)}</td>
        <td>${parseFloat(p.size_b).toFixed(0)}</td>
        <td>$${parseFloat(p.budget).toFixed(2)}</td>
        <td style="color:${color}">+$${parseFloat(p.expected_profit).toFixed(2)}</td>
        <td>${p.status}</td>
      </tr>`;
    });
    html += '</table>';
    document.getElementById('h-positions').innerHTML = html;
  });
}

// ═══════════════════════════════════════════════════
// DUTCHING
// ═══════════════════════════════════════════════════
// ═══════════════════════════════════════════════════
// MARKET MAKING
// ═══════════════════════════════════════════════════
let _mmLogTimer = null;
async function loadMM() {
  loadMMLog();
  if(_mmLogTimer) clearInterval(_mmLogTimer);
  _mmLogTimer = setInterval(()=>{
    if(document.getElementById('page-mm').style.display!=='none') { loadMMStatus(); loadMMLog(); }
    else { clearInterval(_mmLogTimer); _mmLogTimer=null; }
  }, 10000);
  await loadMMStatus();
}
async function loadMMStatus() {
  const st = await api('/api/mm/status');
  if(st.ok) {
    const el = document.getElementById('mm-status');
    if(st.running) { const m=Math.floor(st.uptime/60); el.textContent='Running '+m+'m'; el.style.color='var(--g)'; }
    else { el.textContent='Stopped'; el.style.color='#666'; }
  }
  const ss = await api('/api/mm/stats');
  if(ss.ok) {
    document.getElementById('mm-s-markets').textContent = ss.active_markets||0;
    document.getElementById('mm-s-fills').textContent = ss.total_fills||0;
    document.getElementById('mm-s-spent').textContent = '$'+(ss.total_spent||0).toFixed(0);
    document.getElementById('mm-s-received').textContent = '$'+(ss.total_received||0).toFixed(0);
    const p = ss.total_pnl||0;
    const pe = document.getElementById('mm-s-pnl');
    pe.textContent = (p>=0?'+$':'-$')+Math.abs(p).toFixed(2); pe.style.color = p>=0?'var(--g)':'var(--r)';
    const ppnl = ss.paired_pnl||0;
    const ppEl = document.getElementById('mm-s-paired');
    ppEl.textContent = (ppnl>=0?'+$':'-$')+Math.abs(ppnl).toFixed(0); ppEl.style.color = ppnl>=0?'var(--g)':'var(--r)';
    document.getElementById('mm-s-spread').textContent = (ss.captured_spread||0).toFixed(1)+'%';
    document.getElementById('mm-s-exposure').textContent = '$'+(ss.net_exposure||0).toFixed(0);
  }
  // Markets table
  const mm = await api('/api/mm/markets');
  if(mm.ok && mm.markets) {
    const tb = document.getElementById('mm-markets-tbody');
    const noEl = document.getElementById('mm-no-markets');
    if(mm.markets.length===0) { tb.innerHTML=''; noEl.style.display=''; return; }
    noEl.style.display='none';
    tb.innerHTML = mm.markets.map(m => {
      const bids = (m.bid_orders||[]).map(x=>x[0].toFixed(2)).join(' ');
      const asks = (m.ask_orders||[]).map(x=>(1-x[0]).toFixed(2)).join(' ');
      const ys = m.yes_shares||0, ns = m.no_shares||0;
      const cost = m.total_cost||0;
      const pYes = m.pnl_if_yes||0, pNo = m.pnl_if_no||0;
      return '<tr>'+
        '<td style="max-width:220px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;font-size:11px">'+
          (m.event||'')+'<br><span style="color:#888;font-size:10px">'+(m.question||'')+'</span></td>'+
        '<td>'+(m.mid||0).toFixed(2)+'<br><span style="color:#888;font-size:9px">'+(m.mid>0?(1/m.mid).toFixed(2):'—')+'</span></td>'+
        '<td style="color:var(--g);font-size:10px">'+bids+'</td>'+
        '<td style="color:var(--r);font-size:10px">'+asks+'</td>'+
        '<td style="color:var(--g);font-size:11px">'+(ys>0?ys.toFixed(0):'-')+
          (m.yes_cost>0?'<br><span style="color:#888;font-size:9px">$'+m.yes_cost.toFixed(0)+'</span>':'')+'</td>'+
        '<td style="color:var(--r);font-size:11px">'+(ns>0?ns.toFixed(0):'-')+
          (m.no_cost>0?'<br><span style="color:#888;font-size:9px">$'+m.no_cost.toFixed(0)+'</span>':'')+'</td>'+
        '<td style="font-size:11px">$'+cost.toFixed(0)+'</td>'+
        '<td>'+(m.fills_count||0)+'</td>'+
        '<td style="color:#9b59b6;font-size:11px">'+(m.margin_pct||0).toFixed(1)+'%</td>'+
        '<td style="color:'+(pYes>=0?'var(--g)':'var(--r)')+';font-size:11px">'+
          (pYes>=0?'+':'')+pYes.toFixed(0)+'</td>'+
        '<td style="color:'+(pNo>=0?'var(--g)':'var(--r)')+';font-size:11px">'+
          (pNo>=0?'+':'')+pNo.toFixed(0)+'</td>'+
        '<td style="white-space:nowrap">'+
          '<button class="savebtn" style="padding:2px 6px;font-size:9px;'+(m.paused?'background:#2ecc71':'background:#e67e22')+'" '+
            'onclick="mmTogglePause(&quot;'+m.condition_id+'&quot;,'+(m.paused?'false':'true')+')">'+(m.paused?'▶':'⏸')+'</button> '+
          '<button class="savebtn" style="padding:2px 6px;font-size:8px;'+(m.prematch_only?'background:#3498db':'background:#555')+'" '+
            'onclick="mmTogglePrematch(&quot;'+m.condition_id+'&quot;,'+(m.prematch_only?'false':'true')+')" '+
            'title="Prematch Only">'+(m.prematch_only?'PRE':'ALL')+'</button> '+
          '<button class="savebtn" style="padding:2px 6px;font-size:9px;background:#c0392b" '+
            'onclick="mmRemove(&quot;'+m.condition_id+'&quot;)">X</button></td></tr>';
    }).join('');
  }
  // Config
  const cfg = await api('/api/config');
  if(cfg.ok) {
    const c = cfg.config||{};
    document.getElementById('mm-levels').value = c.MM_LEVELS||3;
    document.getElementById('mm-step').value = c.MM_STEP||1;
    document.getElementById('mm-size').value = c.MM_ORDER_SIZE||20;
    document.getElementById('mm-poll').value = c.MM_POLL_INTERVAL||30;
    document.getElementById('mm-maxmkt').value = c.MM_MAX_MARKETS||5;
    document.getElementById('mm-skewstep').value = c.MM_SKEW_STEP||50;
    document.getElementById('mm-skewmax').value = c.MM_SKEW_MAX||3;
    document.getElementById('mm-maxpos').value = c.MM_MAX_POSITION||200;
    document.getElementById('mm-panic').value = c.MM_SPREAD_PANIC||5;
    document.getElementById('mm-anchor').value = c.MM_ANCHOR||'mid';
    document.getElementById('mm-sell').checked = c.MM_SELL_ENABLED==='true'||c.MM_SELL_ENABLED===true||c.MM_SELL_ENABLED==='1';
  }
}
async function startMM() {
  const d = await api('/api/mm/start',{method:'POST'});
  if(d.ok) setTimeout(loadMM, 500); else alert(d.error||'Error');
}
async function stopMM() {
  await api('/api/mm/stop',{method:'POST'}); setTimeout(loadMM, 500);
}
async function mmSearch() {
  const q = document.getElementById('mm-search-q').value.trim();
  if(!q) return;
  const sport = document.getElementById('mm-search-sport').value;
  const url = '/api/mm/search?q='+encodeURIComponent(q)+(sport?'&sport='+sport:'');
  const d = await api(url);
  const el = document.getElementById('mm-search-results');
  if(!d.ok||!d.markets||d.markets.length===0) { el.innerHTML='<div style="color:#555;padding:8px">No results</div>'; return; }
  el.innerHTML = d.markets.map((m,i) =>
    '<div style="display:flex;align-items:center;justify-content:space-between;padding:5px 0;border-bottom:1px solid #1a1a1a">'+
    '<div style="flex:1"><span style="color:#eee;font-size:11px">'+(m.question||'')+'</span>'+
    '<span style="color:#555;font-size:10px;margin-left:8px">mid='+(m.mid||'?')+'  '+(m.event||'')+'</span></div>'+
    '<button class="savebtn" style="padding:2px 10px;font-size:10px" onclick="mmAddByIdx('+i+')">+ MM</button></div>'
  ).join('');
  window._mmSearchResults = d.markets;
}
async function mmAddByIdx(idx) {
  const m = (window._mmSearchResults||[])[idx];
  if(!m) return;
  const d = await api('/api/mm/add',{method:'POST',headers:{'Content-Type':'application/json'},
    body:JSON.stringify({condition_id:m.condition_id,token_yes:m.token_yes,token_no:m.token_no,
      question:m.question||'',event_name:m.event||'',sport:m.sport||'',neg_risk:m.neg_risk||false,tick_size:m.tick_size||'0.01'})});
  if(d.ok) { document.getElementById('mm-search-results').innerHTML='<div style="color:var(--g);padding:8px">Added!</div>'; setTimeout(loadMMStatus,500); }
  else alert(d.error||'Error');
}
async function mmRemove(cid) {
  await api('/api/mm/remove',{method:'POST',headers:{'Content-Type':'application/json'},
    body:JSON.stringify({condition_id:cid})});
  loadMM();
}
async function mmClearStats() {
  await api('/api/mm/clear_stats',{method:'POST'});
  loadMM();
}
async function mmTogglePause(cid, paused) {
  await api('/api/mm/pause',{method:'POST',headers:{'Content-Type':'application/json'},
    body:JSON.stringify({condition_id:cid, paused:paused})});
  loadMM();
}
async function mmTogglePrematch(cid, val) {
  await api('/api/mm/prematch_only',{method:'POST',headers:{'Content-Type':'application/json'},
    body:JSON.stringify({condition_id:cid, prematch_only:val})});
  loadMM();
}
async function loadMMLog() {
  const d = await api('/api/mm/log?lines=40');
  if(!d.ok) return;
  const el = document.getElementById('mm-log');
  if(d.lines&&d.lines.length>0) { el.textContent=d.lines.join(String.fromCharCode(10)); el.scrollTop=el.scrollHeight; }
  else { el.textContent='Start the bot to see logs'; }
}
async function saveMMCfg() {
  const body = {
    MM_LEVELS: document.getElementById('mm-levels').value,
    MM_STEP: document.getElementById('mm-step').value,
    MM_ORDER_SIZE: document.getElementById('mm-size').value,
    MM_POLL_INTERVAL: document.getElementById('mm-poll').value,
    MM_MAX_MARKETS: document.getElementById('mm-maxmkt').value,
    MM_SKEW_STEP: document.getElementById('mm-skewstep').value,
    MM_SKEW_MAX: document.getElementById('mm-skewmax').value,
    MM_MAX_POSITION: document.getElementById('mm-maxpos').value,
    MM_SPREAD_PANIC: document.getElementById('mm-panic').value,
    MM_ANCHOR: document.getElementById('mm-anchor').value,
    MM_SELL_ENABLED: document.getElementById('mm-sell').checked?'true':'false',
  };
  const d = await api('/api/config',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});
  const msg = document.getElementById('mm-savemsg');
  msg.textContent = d.ok?'Saved':'Error'; msg.style.opacity=1;
  setTimeout(()=>msg.style.opacity=0, 2000);
}

// ═══════════════════════════════════════════════════════
//  BACKLOG / KANBAN
// ═══════════════════════════════════════════════════════
let _blItems = [];
const BL_COLS = {idea:'bl-col-idea', todo:'bl-col-todo', progress:'bl-col-progress', done:'bl-col-done'};
const BL_STATUSES = ['idea','todo','progress','done'];
const BL_STATUS_NAMES = {idea:'Idea',todo:'Todo',progress:'In Progress',done:'Done'};
const BL_PRIO_COLORS = {high:'#e74c3c',medium:'#e67e22',low:'#555'};

async function loadBacklog() {
  const d = await api('/api/backlog');
  if(!d.ok) return;
  _blItems = d.items || [];
  renderBacklog();
}

function renderBacklog() {
  for(const s of BL_STATUSES) {
    const el = document.getElementById(BL_COLS[s]);
    const items = _blItems.filter(i => i.status === s);
    el.innerHTML = items.map(i => {
      const pc = BL_PRIO_COLORS[i.priority] || '#e67e22';
      const cat = i.category ? '<span style="color:#555;font-size:8px;background:#1a1a1a;padding:1px 4px;border-radius:2px">'+i.category+'</span> ' : '';
      return '<div style="background:#1a1a1a;border:1px solid #2a2a2a;border-left:3px solid '+pc+';border-radius:4px;padding:8px;margin-bottom:6px;cursor:pointer" onclick="blEditCard('+i.id+')">'+
        '<div style="font-size:11px;color:#eee;margin-bottom:4px">'+i.title+'</div>'+
        (i.description ? '<div style="font-size:9px;color:#666;margin-bottom:4px;max-height:40px;overflow:hidden">'+i.description+'</div>' : '')+
        '<div style="display:flex;gap:4px;align-items:center;flex-wrap:wrap">'+cat+
          '<span style="color:'+pc+';font-size:8px;font-weight:bold">'+i.priority.toUpperCase()+'</span>'+
          '<span style="color:#333;font-size:8px;margin-left:auto">#'+i.id+'</span>'+
        '</div>'+
      '</div>';
    }).join('');
    if(!items.length) el.innerHTML += '<div style="color:#333;font-size:10px;text-align:center;padding:20px">Empty</div>';
  }
}

async function blAddCard() {
  const title = prompt('Task title:');
  if(!title) return;
  const desc = prompt('Description (optional):', '') || '';
  const cat = prompt('Category (optional):', '') || '';
  const prio = prompt('Priority (high/medium/low):', 'medium') || 'medium';
  await api('/api/backlog',{method:'POST',headers:{'Content-Type':'application/json'},
    body:JSON.stringify({title, description:desc, category:cat, priority:prio, status:'idea'})});
  loadBacklog();
}

async function blEditCard(id) {
  const item = _blItems.find(i => i.id === id);
  if(!item) return;
  const actions = ['Edit Title','Edit Description','Change Status','Change Priority','Delete','Cancel'];
  const choice = prompt(
    '#'+id+' '+item.title+
    '\\nStatus: '+item.status+' | Priority: '+item.priority+
    (item.description ? '\\n'+item.description : '')+
    '\\n\\n1=Edit Title  2=Description  3=Status  4=Priority  5=Delete  6=Cancel', '3');
  if(!choice) return;
  const n = parseInt(choice);
  if(n===1) {
    const v = prompt('New title:', item.title);
    if(v) { await api('/api/backlog/'+id,{method:'PUT',headers:{'Content-Type':'application/json'},body:JSON.stringify({title:v})}); loadBacklog(); }
  } else if(n===2) {
    const v = prompt('New description:', item.description);
    if(v!==null) { await api('/api/backlog/'+id,{method:'PUT',headers:{'Content-Type':'application/json'},body:JSON.stringify({description:v})}); loadBacklog(); }
  } else if(n===3) {
    const v = prompt('New status (idea/todo/progress/done):', item.status);
    if(v && BL_STATUSES.includes(v)) { await api('/api/backlog/'+id,{method:'PUT',headers:{'Content-Type':'application/json'},body:JSON.stringify({status:v})}); loadBacklog(); }
  } else if(n===4) {
    const v = prompt('Priority (high/medium/low):', item.priority);
    if(v) { await api('/api/backlog/'+id,{method:'PUT',headers:{'Content-Type':'application/json'},body:JSON.stringify({priority:v})}); loadBacklog(); }
  } else if(n===5) {
    if(confirm('Delete task #'+id+'?')) { await api('/api/backlog/'+id,{method:'DELETE'}); loadBacklog(); }
  }
}

// ═══════════════════════════════════════════════════════
//  SETTLEMENT SNIPER
// ═══════════════════════════════════════════════════════
let _snipeTimer = null;
let _snipeCands = [];  // global cache of candidates for client-side filtering

function _endsLabel(h) {
  if(h === null || h === undefined) return {str:'?', color:'#555'};
  if(h < 0) return {str:'ended', color:'#e74c3c'};
  if(h < 1) return {str:Math.round(h*60)+'m', color:'#e74c3c'};
  if(h < 24) return {str:h.toFixed(1)+'h', color:'#e67e22'};
  return {str:(h/24).toFixed(1)+'d', color:'#f1c40f'};
}

function renderCands() {
  const candTb   = document.getElementById('sn-cand-tbody');
  const candEmpty = document.getElementById('sn-cand-empty');
  const countEl  = document.getElementById('sn-cand-count');

  // Read filter values
  const fQ       = (document.getElementById('snf-q')?.value||'').toLowerCase().trim();
  const fSport   = document.getElementById('snf-sport')?.value||'';
  const fSide    = document.getElementById('snf-side')?.value||'';
  const fPmin    = parseFloat(document.getElementById('snf-pmin')?.value)||0;
  const fPmax    = parseFloat(document.getElementById('snf-pmax')?.value)||1;
  const fProfMin = parseFloat(document.getElementById('snf-profmin')?.value)||0;
  const fEndH    = parseFloat(document.getElementById('snf-endh')?.value)||Infinity;
  const fSort    = document.getElementById('snf-sort')?.value||'profit_pct';

  let filtered = _snipeCands.filter(c => {
    if(fQ && !(c.question||'').toLowerCase().includes(fQ) && !(c.event||'').toLowerCase().includes(fQ)) return false;
    if(fSport && c.tag !== fSport) return false;
    if(fSide && c.side !== fSide) return false;
    if(c.price < fPmin || c.price > fPmax) return false;
    if(c.profit_pct < fProfMin) return false;
    if(fEndH < Infinity) {
      const h = c.hours_to_end;
      if(h === null || h === undefined || h > fEndH) return false;
    }
    return true;
  });

  // Sort
  filtered.sort((a,b) => {
    if(fSort==='price')   return a.price - b.price;
    if(fSort==='ends')    return (a.hours_to_end??9999) - (b.hours_to_end??9999);
    if(fSort==='age')     return b.age_min - a.age_min;
    return b.profit_pct - a.profit_pct;  // default: profit_pct desc
  });

  if(countEl) countEl.textContent = '('+filtered.length+' / '+_snipeCands.length+')';

  if(!filtered.length) {
    candTb.innerHTML='';
    candEmpty.style.display='';
    candEmpty.textContent = _snipeCands.length ? 'No candidates match filters.' : 'No candidates yet. Bot is scanning...';
    return;
  }
  candEmpty.style.display='none';
  candTb.innerHTML = filtered.map(c => {
    const odds = c.price > 0 ? (1/c.price).toFixed(2) : '?';
    const {str:endsStr, color:endsColor} = _endsLabel(c.hours_to_end);
    return '<tr>'+
      '<td style="font-size:11px;max-width:220px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">'+
        (c.question||'')+'<br><span style="color:#555;font-size:9px">'+(c.event||'')+'</span></td>'+
      '<td style="color:'+(c.side==='YES'?'var(--g)':'var(--r)')+';font-weight:bold">'+c.side+'</td>'+
      '<td>'+c.price.toFixed(3)+' <span style="color:#555;font-size:9px">('+odds+')</span></td>'+
      '<td style="color:var(--g)">+'+c.profit_pct.toFixed(1)+'%</td>'+
      '<td style="color:var(--g)">+$'+c.expected_profit.toFixed(2)+'</td>'+
      '<td style="font-size:10px;color:#aaa">'+( c.tag||'' )+'</td>'+
      '<td style="font-size:10px;color:'+endsColor+';font-weight:bold">'+endsStr+'</td>'+
      '<td style="color:#555;font-size:10px">'+c.age_min+'m</td>'+
      '<td style="white-space:nowrap">'+
        '<button class="savebtn" style="padding:2px 8px;font-size:9px;background:#2ecc71" '+
          'onclick="snipeApprove(&quot;'+c.condition_id+'&quot;)">BUY</button> '+
        '<button class="savebtn" style="padding:2px 8px;font-size:9px;background:#555" '+
          'onclick="snipeReject(&quot;'+c.condition_id+'&quot;)">SKIP</button>'+
      '</td></tr>';
  }).join('');
}

function snfReset() {
  ['snf-q','snf-pmin','snf-pmax','snf-profmin','snf-endh'].forEach(id=>{
    const el=document.getElementById(id); if(el) el.value='';
  });
  ['snf-sport','snf-side'].forEach(id=>{
    const el=document.getElementById(id); if(el) el.value='';
  });
  document.getElementById('snf-sort').value='profit_pct';
  renderCands();
}

async function snipeRejectAll() {
  if(!_snipeCands.length) return;
  if(!confirm('Skip all '+_snipeCands.length+' candidates?')) return;
  for(const c of _snipeCands) {
    await api('/api/snipe/reject',{method:'POST',
      headers:{'Content-Type':'application/json'},
      body:JSON.stringify({condition_id:c.condition_id})});
  }
  loadSnipe();
}
async function loadSnipe() {
  // Status
  const st = await api('/api/snipe/status');
  if(st.ok) {
    const el = document.getElementById('snipe-status');
    const btn = document.getElementById('snipe-start');
    if(st.running) {
      el.textContent='Running '+Math.floor(st.uptime/60)+'m'; el.style.color='var(--g)';
      btn.textContent='STOP'; btn.style.background='#c0392b';
    } else {
      el.textContent='Stopped'; el.style.color='#555';
      btn.textContent='START'; btn.style.background='';
    }
  }
  // Stats
  const ss = await api('/api/snipe/stats');
  if(ss.ok) {
    // Mode button
    const modeBtn = document.getElementById('snipe-mode');
    if(modeBtn) {
      modeBtn.textContent = ss.auto_mode ? 'AUTO' : 'MANUAL';
      modeBtn.style.background = ss.auto_mode ? '#c0392b' : '#2ecc71';
    }

    document.getElementById('sn-active').textContent = ss.active||0;
    document.getElementById('sn-scanned').textContent = ss.total_scanned||0;
    document.getElementById('sn-sniped').textContent = ss.total_sniped||0;
    document.getElementById('sn-settled').textContent = ss.total_settled||0;

    // Populate settings from current bot config
    if(ss.settings) {
      const s = ss.settings;
      if(s.min_price) document.getElementById('sn-minprice').value = s.min_price;
      if(s.max_price) document.getElementById('sn-maxprice').value = s.max_price;
      if(s.order_size) document.getElementById('sn-size').value = s.order_size;
      if(s.max_positions) document.getElementById('sn-maxpos').value = s.max_positions;
      if(s.scan_interval) document.getElementById('sn-interval').value = s.scan_interval;
      if(s.max_days_to_end !== undefined) document.getElementById('sn-maxdays').value = s.max_days_to_end;
      if(s.tags) document.getElementById('sn-tags').value = s.tags;
    }
    const p = ss.total_profit||0;
    const pe = document.getElementById('sn-profit');
    pe.textContent = (p>=0?'+$':'-$')+Math.abs(p).toFixed(2);
    pe.style.color = p>=0?'var(--g)':'var(--r)';

    // Active snipes table
    const tb = document.getElementById('sn-tbody');
    const empty = document.getElementById('sn-empty');
    const snipes = ss.snipes||[];
    if(!snipes.length) { tb.innerHTML=''; empty.style.display=''; }
    else {
      empty.style.display='none';
      tb.innerHTML = snipes.map(s => {
        const stC = s.status==='filled'?'var(--g)':s.status==='ordered'?'#e67e22':'#666';
        return '<tr>'+
          '<td style="font-size:11px;max-width:200px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">'+(s.question||'')+'</td>'+
          '<td style="color:'+(s.side==='YES'?'var(--g)':'var(--r)')+'">'+s.side+'</td>'+
          '<td>'+s.price.toFixed(3)+'</td>'+
          '<td>'+s.amount.toFixed(0)+'</td>'+
          '<td>$'+s.cost.toFixed(2)+'</td>'+
          '<td style="color:var(--g)">+$'+s.profit_target.toFixed(2)+'</td>'+
          '<td style="color:'+stC+'">'+s.status+'</td></tr>';
      }).join('');
    }

    // Candidates — store globally, render via renderCands()
    _snipeCands = ss.candidate_list || [];
    // Auto-extend tag dropdown from actual candidate data
    const tagSel = document.getElementById('snf-sport');
    const knownTags = new Set(Array.from(tagSel.options).map(o=>o.value).filter(v=>v));
    _snipeCands.forEach(c => {
      if(c.tag && !knownTags.has(c.tag)) {
        const opt = document.createElement('option'); opt.value=c.tag; opt.textContent=c.tag;
        tagSel.appendChild(opt); knownTags.add(c.tag);
      }
    });
    renderCands();
  }
  // Log
  const lg = await api('/api/snipe/log?lines=30');
  if(lg.ok && lg.lines) {
    const el = document.getElementById('sn-log');
    el.textContent = lg.lines.join(String.fromCharCode(10));
    el.scrollTop = el.scrollHeight;
  }
  // Auto-refresh
  if(_snipeTimer) clearInterval(_snipeTimer);
  _snipeTimer = setInterval(()=>{
    if(document.getElementById('page-snipe').style.display!=='none') loadSnipe();
    else { clearInterval(_snipeTimer); _snipeTimer=null; }
  }, 5000);
}
async function snipeToggle() {
  const st = await api('/api/snipe/status');
  if(st.running) await api('/api/snipe/stop',{method:'POST'});
  else await api('/api/snipe/start',{method:'POST'});
  setTimeout(loadSnipe, 500);
}
async function saveSnipeCfg() {
  const body = {
    SNIPE_MIN_PRICE: document.getElementById('sn-minprice').value,
    SNIPE_MAX_PRICE: document.getElementById('sn-maxprice').value,
    SNIPE_ORDER_SIZE: document.getElementById('sn-size').value,
    SNIPE_MAX_POSITIONS: document.getElementById('sn-maxpos').value,
    SNIPE_SCAN_INTERVAL: document.getElementById('sn-interval').value,
    SNIPE_MAX_DAYS_TO_END: document.getElementById('sn-maxdays').value,
    SNIPE_TAGS: document.getElementById('sn-tags').value,
  };
  await api('/api/config',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});
}
async function snipeApprove(cid) {
  const sizeStr = prompt('Order size in $ (0 = default from settings):', '0');
  if(sizeStr === null) return;
  const size = parseFloat(sizeStr) || 0;
  const d = await api('/api/snipe/approve',{method:'POST',headers:{'Content-Type':'application/json'},
    body:JSON.stringify({condition_id:cid, size:size})});
  if(d.ok) { loadSnipe(); } else { alert('Error: '+(d.error||'unknown')); }
}
async function snipeReject(cid) {
  await api('/api/snipe/reject',{method:'POST',headers:{'Content-Type':'application/json'},
    body:JSON.stringify({condition_id:cid})});
  loadSnipe();
}
async function snipeModeToggle() {
  const ss = await api('/api/snipe/stats');
  const newMode = !(ss.auto_mode || false);
  await api('/api/snipe/mode',{method:'POST',headers:{'Content-Type':'application/json'},
    body:JSON.stringify({auto:newMode})});
  loadSnipe();
}

let _dutchLogTimer = null;
async function loadDutch() {
  loadDutchLog();
  // Auto-refresh log every 10s while on page
  if(_dutchLogTimer) clearInterval(_dutchLogTimer);
  _dutchLogTimer = setInterval(()=>{
    if(document.getElementById('page-dutch').style.display!=='none') loadDutchLog();
    else { clearInterval(_dutchLogTimer); _dutchLogTimer=null; }
  }, 10000);
  // Status
  const st = await api('/api/dutch/status');
  if(st.ok) {
    const el = document.getElementById('dutch-status');
    if(st.running) {
      const m = Math.floor(st.uptime/60);
      el.textContent = `Работает ${m} мин`;
      el.style.color = 'var(--g)';
    } else {
      el.textContent = 'Остановлен';
      el.style.color = '#666';
    }
  }
  // Stats
  const ss = await api('/api/dutch/stats');
  if(ss.ok) {
    document.getElementById('ds-pairs').textContent = ss.total_pairs||0;
    document.getElementById('ds-settled').textContent = ss.settled_pairs||0;
    document.getElementById('ds-active').textContent = ss.active_pairs||0;
    const p = ss.total_profit||0;
    const pe = document.getElementById('ds-profit');
    pe.textContent = (p>=0?'+':'')+p.toFixed(2);
    pe.style.color = p>=0?'var(--g)':'var(--r)';
    document.getElementById('ds-volume').textContent = '$'+(ss.total_volume||0).toFixed(0);
  }
  // Pairs
  const pp = await api('/api/dutch/pairs');
  if(pp.ok && pp.pairs) {
    const tb = document.getElementById('dutch-tbody');
    tb.innerHTML = pp.pairs.map(p => {
      const statuses = (p.statuses||'').split(',').map(s =>
        s==='settled'?'<span style="color:var(--g)">filled</span>':
        s==='placed'?'<span style="color:#e67e22">open</span>':
        '<span style="color:#666">'+s+'</span>'
      ).join(' / ');
      const pnl = (p.profit||0);
      const pnlC = pnl>=0?'var(--g)':'var(--r)';
      return `<tr>
        <td style="max-width:200px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${p.event||''}</td>
        <td style="font-size:10px">${p.sides||''}</td>
        <td>$${(p.total_cost||0).toFixed(2)}</td>
        <td>${statuses}</td>
        <td style="color:${pnlC}">${pnl>=0?'+':''}${pnl.toFixed(4)}</td>
        <td style="font-size:10px">${(p.created_at||'').slice(5,16)}</td>
      </tr>`;
    }).join('');
  }
  // Config
  const cfg = await api('/api/config');
  if(cfg.ok) {
    const c = cfg.config||{};
    document.getElementById('d-spread').value = (parseFloat(c.DUTCH_MIN_SPREAD)||0.005)*100;
    document.getElementById('d-liq').value = c.DUTCH_MIN_LIQUIDITY||50;
    document.getElementById('d-stake').value = c.DUTCH_STAKE||5;
    document.getElementById('d-maxstake').value = c.DUTCH_MAX_STAKE||50;
    document.getElementById('d-poll').value = c.DUTCH_POLL_INTERVAL||60;
    document.getElementById('d-ttl').value = c.DUTCH_ORDER_TTL_SECS||120;
    document.getElementById('d-sports').value = c.DUTCH_SPORTS||'tennis,nba,nhl,soccer,mlb,mma,nfl';
  }
}
async function startDutch() {
  const d = await api('/api/dutch/start',{method:'POST'});
  if(d.ok) setTimeout(loadDutch, 500);
  else alert(d.error||'Error');
}
async function stopDutch() {
  await api('/api/dutch/stop',{method:'POST'});
  setTimeout(loadDutch, 500);
}
async function loadDutchLog() {
  const d = await api('/api/dutch/log?lines=60');
  if(!d.ok) return;
  const el = document.getElementById('dutch-log');
  if(d.lines && d.lines.length > 0) {
    el.textContent = d.lines.join(String.fromCharCode(10));
    el.scrollTop = el.scrollHeight;
  } else {
    el.textContent = 'Лог пуст — запустите бот';
  }
}
async function saveDutchCfg() {
  const body = {
    DUTCH_MIN_SPREAD: String((parseFloat(document.getElementById('d-spread').value)||0.5)/100),
    DUTCH_MIN_LIQUIDITY: document.getElementById('d-liq').value,
    DUTCH_STAKE: document.getElementById('d-stake').value,
    DUTCH_MAX_STAKE: document.getElementById('d-maxstake').value,
    DUTCH_POLL_INTERVAL: document.getElementById('d-poll').value,
    DUTCH_ORDER_TTL_SECS: document.getElementById('d-ttl').value,
    DUTCH_SPORTS: document.getElementById('d-sports').value,
  };
  const d = await api('/api/config',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});
  const msg = document.getElementById('d-savemsg');
  msg.textContent = d.ok?'Сохранено':'Ошибка'; msg.style.opacity=1;
  setTimeout(()=>msg.style.opacity=0, 2000);
}

document.addEventListener('DOMContentLoaded', init);
</script>
</body>
</html>
"""

app = Flask(__name__)
log = logging.getLogger(__name__)

BOT_LOG_FILE = os.getenv("BOT_LOG_FILE", str(BASE_DIR / "valuebet_bot.log"))

@app.after_request
def add_cors(r):
    r.headers["Access-Control-Allow-Origin"]  = "*"
    r.headers["Access-Control-Allow-Headers"] = "Content-Type"
    r.headers["Access-Control-Allow-Methods"] = "GET,POST,OPTIONS"
    return r

# ── Bot state ─────────────────────────────────────────────────────────────────
_bot_thread   = None
_bot_loop     = None
_bot_running  = False
_bot_start_ts = None

# Live bot state
_live_thread   = None
_live_loop     = None
_live_running  = False
_live_start_ts = None

# ── Auto-redeem background thread ─────────────────────────────────────────────
_auto_redeem_last_ts: float = 0.0   # когда последний раз запускали redeem
_AUTO_REDEEM_INTERVAL = 3600        # раз в час

def _auto_redeem_loop():
    """Фоновый поток: раз в час проверяет redeemable позиции и выкупает их,
    если запущен основной бот (valuebet)."""
    global _auto_redeem_last_ts
    while True:
        try:
            time.sleep(60)  # проверяем каждую минуту
            now = time.time()
            # Только если основной бот запущен
            if not _bot_running:
                continue
            # Раз в час
            if now - _auto_redeem_last_ts < _AUTO_REDEEM_INTERVAL:
                continue
            _auto_redeem_last_ts = now
            log.info("[auto-redeem] Запускаем автоматический redeem settled позиций")
            try:
                from polymarket_client import PolymarketClient
                pk     = os.getenv("POLYMARKET_PRIVATE_KEY", "")
                funder = os.getenv("POLYMARKET_FUNDER", "")
                if pk and funder:
                    pm = PolymarketClient(pk, funder)
                    result = pm.redeem_positions()
                    _wallet_cache["ts"] = 0.0  # инвалидируем кеш
                    if result.get("redeemed", 0) > 0:
                        log.info("[auto-redeem] ✅ Выкуплено %d позиций на $%.2f",
                                 result["redeemed"], result["amount"])
                    elif result.get("error"):
                        log.warning("[auto-redeem] ⚠️ %s", result["error"])
                    else:
                        log.info("[auto-redeem] Нечего выкупать")
            except Exception as e:
                log.error("[auto-redeem] Ошибка: %s", e)
        except Exception:
            pass

_auto_redeem_thread = threading.Thread(
    target=_auto_redeem_loop, daemon=True, name="auto-redeem")
_auto_redeem_thread.start()


try:
    from auto_settle import AutoSettleWorker
    _HAS_AUTO_SETTLE = True
except ImportError:
    _HAS_AUTO_SETTLE = False
    log.warning("auto_settle.py не найден — авто-расчёт недоступен")


def _get_db():
    from db_bets import BetDatabase
    from config import Config
    cfg = Config()
    return BetDatabase(getattr(cfg, "DB_PATH_VALUEBET", "valuebets.db"))


def _read_log(n=100):
    try:
        # Папка где лежит БД — скорее всего там и лог
        db_path = os.getenv("DB_PATH_VALUEBET", "valuebets.db")
        db_dir  = os.path.dirname(os.path.abspath(db_path)) if db_path else ""

        candidates = [
            BOT_LOG_FILE,                                          # из .env или BASE_DIR
            os.path.join(os.getcwd(), "valuebet_bot.log"),         # CWD
            os.path.join(db_dir, "valuebet_bot.log") if db_dir else "",  # рядом с БД
            str(BASE_DIR / "valuebet_bot.log"),                    # папка dashboard_server.py
            str(BASE_DIR.parent / "valuebet_bot.log"),             # на уровень выше
        ]
        log_file = next((p for p in candidates if p and os.path.exists(p)), None)
        if not log_file:
            searched = "\n  ".join(p for p in candidates if p)
            return [f"(лог не найден, искал в: {searched})"]
        # Читаем только хвост файла (последние ~100KB), НЕ весь файл
        tail_bytes = max(n * 300, 50000)  # ~300 байт/строка
        file_size = os.path.getsize(log_file)
        with open(log_file, encoding="utf-8", errors="replace") as f:
            if file_size > tail_bytes:
                f.seek(file_size - tail_bytes)
                f.readline()  # пропустить обрезанную первую строку
            lines = f.readlines()
        return [l.rstrip() for l in lines[-n:]]
    except Exception as e:
        return [f"(ошибка чтения лога: {e})"]


# ── Routes ────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return Response(_HTML, mimetype="text/html; charset=utf-8")

@app.route("/favicon.ico")
def favicon():
    return Response("", status=204)

@app.route("/api/stats")
def api_stats():
    try:
        db    = _get_db()
        stats = db.get_stats()
        brl   = db.get_bankroll()
        daily = [dict(r) for r in db.conn.execute("""
            SELECT DATE(created_at) day,
                   COUNT(*) cnt,
                   SUM(CASE WHEN status IN('placed','settled') THEN 1 ELSE 0 END) placed,
                   SUM(CASE WHEN outcome_result='won'  THEN 1 ELSE 0 END) won,
                   SUM(CASE WHEN outcome_result='lost' THEN 1 ELSE 0 END) lost,
                   ROUND(SUM(CASE WHEN stake_price>0 THEN stake*stake_price ELSE stake*bb_price END),2) volume,
                   ROUND(SUM(profit_actual),2) profit,
                   ROUND(AVG(value_pct),2) avg_edge
            FROM bets WHERE created_at >= DATE('now','-14 days')
            GROUP BY day ORDER BY day DESC
        """).fetchall()]
        try:
            from polymarket_bet import SPORT_NAMES
        except Exception:
            SPORT_NAMES = {}

        sports_raw = []
        for r in db.conn.execute("""
            SELECT sport_id, COUNT(*) cnt,
                   SUM(CASE WHEN outcome_result='won'  THEN 1 ELSE 0 END) won,
                   SUM(CASE WHEN outcome_result='lost' THEN 1 ELSE 0 END) lost,
                   ROUND(SUM(profit_actual),2) profit,
                   ROUND(SUM(CASE WHEN stake_price>0 THEN stake*stake_price ELSE stake*bb_price END),2) volume
            FROM bets WHERE status IN('placed','settled')
            GROUP BY sport_id ORDER BY cnt DESC
        """).fetchall():
            d = dict(r)
            d["sport_name"] = SPORT_NAMES.get(d["sport_id"], f"sport {d['sport_id']}")
            sports_raw.append(d)

        # Группируем все киберспортивные в один пункт "🎮 Киберспорт"
        sports = []
        esports_agg = {"sport_id": -1, "sport_name": "🎮 Киберспорт",
                       "cnt": 0, "won": 0, "lost": 0, "profit": 0.0, "volume": 0.0}
        esports_found = False
        for d in sports_raw:
            if d["sport_id"] in ESPORTS_IDS:
                esports_agg["cnt"]    += d.get("cnt") or 0
                esports_agg["won"]    += d.get("won") or 0
                esports_agg["lost"]   += d.get("lost") or 0
                esports_agg["profit"] = round(esports_agg["profit"] + (d.get("profit") or 0), 2)
                esports_agg["volume"] = round(esports_agg["volume"] + (d.get("volume") or 0), 2)
                esports_found = True
            else:
                sports.append(d)
        if esports_found:
            sports.append(esports_agg)
        sports.sort(key=lambda x: x["cnt"], reverse=True)
        uptime = int(time.time() - _bot_start_ts) if _bot_start_ts else 0
        # Хэш для определения изменений в БД (без лишних запросов)
        try:
            chk = db.conn.execute(
                "SELECT COUNT(*) cnt, MAX(id) mid, MAX(updated_at) mup FROM bets"
            ).fetchone()
            bets_hash = f"{chk['cnt']}-{chk['mid']}-{chk['mup']}"
        except Exception:
            bets_hash = ""
        free_usdc = db.get_free_usdc()
        # Суммарные комиссии
        fee_row = db.conn.execute(
            "SELECT ROUND(SUM(fee_usdc),4) as total_fees, "
            "COUNT(CASE WHEN fee_rate > 0 THEN 1 END) as fee_bets "
            "FROM bets WHERE fee_usdc > 0"
        ).fetchone()
        total_fees = dict(fee_row) if fee_row else {"total_fees": 0, "fee_bets": 0}
        return jsonify(ok=True, stats=stats, bankroll=brl, free_usdc=free_usdc,
                       daily=daily, sports=sports, bot_running=_bot_running,
                       bot_uptime=uptime, bets_hash=bets_hash,
                       total_fees=total_fees.get("total_fees") or 0,
                       fee_bets=total_fees.get("fee_bets") or 0)
    except Exception as e:
        log.exception("api_stats"); return jsonify(ok=False, error=str(e)), 500

@app.route("/api/bets")
def api_bets():
    try:
        db      = _get_db()
        limit   = min(int(request.args.get("limit", 50)), 500)
        offset  = int(request.args.get("offset", 0))
        status  = request.args.get("status", "")
        sport   = request.args.get("sport", "")       # sport_id
        league  = request.args.get("league", "")      # substring match
        result  = request.args.get("result", "")      # won/lost/pending
        date_from = request.args.get("date_from", "") # YYYY-MM-DD
        date_to   = request.args.get("date_to", "")   # YYYY-MM-DD
        odds_min  = request.args.get("odds_min", "")
        odds_max  = request.args.get("odds_max", "")
        liq_min   = request.args.get("liq_min",  "")
        liq_max   = request.args.get("liq_max",  "")
        edge_min  = request.args.get("edge_min", "")
        edge_max  = request.args.get("edge_max", "")
        arb_min   = request.args.get("arb_min",  "")
        arb_max   = request.args.get("arb_max",  "")
        mode      = request.args.get("mode", "")      # prematch/live

        # Build dynamic query
        where = []
        params = []
        if status == "active":
            where.append("status = 'placed' AND outcome_result = 'pending'")
        elif status:
            where.append("status = ?"); params.append(status)
        if result:
            where.append("outcome_result = ?"); params.append(result)
        if sport:
            if int(sport) == -1:
                # Киберспорт — все esports IDs
                placeholders = ",".join("?" * len(ESPORTS_IDS))
                where.append(f"sport_id IN ({placeholders})")
                params.extend(sorted(ESPORTS_IDS))
            else:
                where.append("sport_id = ?"); params.append(int(sport))
        if league:
            where.append("(league LIKE ? OR home LIKE ? OR away LIKE ?)")
            params += [f"%{league}%", f"%{league}%", f"%{league}%"]
        if date_from:
            where.append("created_at >= ?"); params.append(date_from)
        if date_to:
            where.append("created_at <= ?"); params.append(date_to + "T23:59:59")
        if odds_min:
            where.append("bb_odds >= ?"); params.append(float(odds_min))
        if odds_max:
            where.append("bb_odds <= ?"); params.append(float(odds_max))
        if liq_min:
            where.append("total_liquidity >= ?"); params.append(float(liq_min))
        if liq_max:
            where.append("total_liquidity <= ?"); params.append(float(liq_max))
        if edge_min:
            where.append("value_pct >= ?"); params.append(float(edge_min))
        if edge_max:
            where.append("value_pct <= ?"); params.append(float(edge_max))
        if arb_min:
            where.append("arb_pct >= ?"); params.append(float(arb_min))
        if arb_max:
            where.append("arb_pct <= ?"); params.append(float(arb_max))
        if mode == "prematch":
            # NULL/пустое/отсутствующее = прематч (старые записи)
            where.append("(bet_mode IS NULL OR bet_mode = '' OR bet_mode = 'prematch')")
        elif mode == "live":
            where.append("bet_mode = 'live'")
        elif mode == "resell":
            where.append("resell_enabled = 1")

        where_sql = "WHERE " + " AND ".join(where) if where else ""

        # Count total for pagination
        total_count = db.conn.execute(
            f"SELECT COUNT(*) FROM bets {where_sql}", params
        ).fetchone()[0]

        rows = db.conn.execute(
            f"""SELECT * FROM bets {where_sql}
                ORDER BY created_at DESC LIMIT ? OFFSET ?""",
            params + [limit, offset]
        ).fetchall()

        recs = rows  # sqlite3.Row objects from fetchall
        import datetime as dt
        def to_dict(r):
            keys = r.keys()
            def g(k, default=None):
                return r[k] if k in keys else default
            started_ts = g("started_at", 0)
            started = dt.datetime.fromtimestamp(started_ts).strftime("%d.%m %H:%M") if started_ts else "?"
            shares      = g("stake") or 0
            stake_price = float(g("stake_price") or 0)
            bb_price    = float(g("bb_price") or 0)
            entry_price = stake_price if stake_price > 0 else bb_price
            cost        = round(shares * entry_price, 2)
            payout      = shares
            profit_tgt  = round(shares * (1 - entry_price), 2)
            bb_odds     = float(g("bb_odds") or 0) or (round(1/entry_price, 4) if entry_price > 0 else 0)
            return dict(
                id=g("id"), created_at=g("created_at",""), home=g("home",""), away=g("away",""),
                league=g("league",""), sport_id=g("sport_id",0),
                outcome_name=g("outcome_name",""),
                market_type_name=g("market_type_name",""),
                market_param=g("market_param",0),
                bb_odds=bb_odds, bb_price=bb_price,
                stake_price=entry_price,
                value_pct=float(g("value_pct") or 0),
                arb_pct=float(g("arb_pct") or 0),
                bet_mode=g("bet_mode","prematch") or "prematch",
                total_liquidity=float(g("total_liquidity") or 0),
                depth_at_price=float(g("depth_at_price") or 0),
                shares=shares,
                cost_usdc=cost,
                payout_target=payout,
                profit_target=profit_tgt,
                outcome_id=g("outcome_id","") or "",
                status=g("status","pending"), order_id=g("order_id","") or "",
                outcome_result=g("outcome_result","pending") or "pending",
                profit_actual=float(g("profit_actual") or 0),
                settled_at=g("settled_at","") or "",
                started_at_fmt=started, error_msg=g("error_msg","") or "",
                resell_status=g("resell_status","") or "",
                sell_price_target=float(g("sell_price_target") or 0),
                sell_order_id=g("sell_order_id","") or "",
                fee_rate=float(g("fee_rate") or 0),
                fee_usdc=float(g("fee_usdc") or 0)
            )
        bets_list = [to_dict(r) for r in recs]

        # Агрегированная статистика по ВСЕЙ выборке (не только по странице)
        agg = db.conn.execute(f"""
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN outcome_result='won'  THEN 1 ELSE 0 END) as won,
                SUM(CASE WHEN outcome_result='lost' THEN 1 ELSE 0 END) as lost,
                SUM(CASE WHEN outcome_result='sold' THEN 1 ELSE 0 END) as sold,
                SUM(CASE WHEN outcome_result='void' THEN 1 ELSE 0 END) as void_cnt,
                SUM(CASE WHEN outcome_result='pending' THEN 1 ELSE 0 END) as pending_cnt,
                SUM(CASE WHEN status='failed' THEN 1 ELSE 0 END) as failed_cnt,
                ROUND(SUM(CASE WHEN stake_price>0 THEN stake*stake_price ELSE stake*bb_price END),2) as total_cost,
                ROUND(SUM(CASE WHEN outcome_result IN('won','lost','sold')
                    THEN (CASE WHEN stake_price>0 THEN stake*stake_price ELSE stake*bb_price END)
                    ELSE 0 END),2) as settled_volume,
                ROUND(SUM(profit_actual),2) as total_pnl,
                ROUND(AVG(value_pct),2) as avg_edge,
                ROUND(AVG(bb_odds),2) as avg_odds
            FROM bets {where_sql}
        """, params).fetchone()
        agg_d = dict(agg) if agg else {}
        settled_cnt = (agg_d.get("won") or 0) + (agg_d.get("lost") or 0) + (agg_d.get("sold") or 0)
        total_cost  = agg_d.get("total_cost") or 0
        total_pnl   = agg_d.get("total_pnl") or 0
        agg_d["winrate"] = round(agg_d["won"] / settled_cnt * 100, 1) if settled_cnt > 0 else None
        # ROI считается от оборота РАСЧИТАННЫХ ставок (won+lost), не от всех
        settled_volume = agg_d.get("settled_volume") or 0
        agg_d["roi"]   = round(total_pnl / settled_volume * 100, 1) if settled_volume > 0 else None

        # Resell-specific aggregation when filtered by resell mode
        if mode == "resell":
            resell_agg = db.conn.execute(f"""
                SELECT
                    SUM(CASE WHEN resell_status='sold' THEN 1 ELSE 0 END) as resold,
                    SUM(CASE WHEN resell_status IN('pending_sell','selling') THEN 1 ELSE 0 END) as on_sale,
                    SUM(CASE WHEN resell_status='expired' THEN 1 ELSE 0 END) as expired,
                    SUM(CASE WHEN resell_status='cancelled' THEN 1 ELSE 0 END) as cancelled,
                    ROUND(AVG(CASE WHEN resell_status='sold' AND sell_price_target>0 AND stake_price>0
                        THEN (sell_price_target - stake_price) / stake_price * 100 END),2) as avg_markup_pct,
                    ROUND(SUM(CASE WHEN resell_status='sold' THEN profit_actual ELSE 0 END),2) as resell_profit
                FROM bets {where_sql}
            """, params).fetchone()
            if resell_agg:
                agg_d["resell"] = dict(resell_agg)

        return jsonify(ok=True, bets=bets_list, count=len(bets_list),
                       total=total_count, offset=offset, limit=limit,
                       has_more=(offset + limit) < total_count,
                       agg=agg_d)
    except Exception as e:
        log.exception("api_bets"); return jsonify(ok=False, error=str(e)), 500

SAFE_KEYS = [
    "BETBURGER_EMAIL","BETBURGER_FILTER_ID_VALUEBET","BETBURGER_FILTER_ID",
    "VB_MIN_ROI","MIN_LIQUIDITY","VB_MIN_STAKE","VB_MAX_STAKE_PCT",
    "VB_STAKE_PCT","VB_USE_KELLY","VB_MAX_ODDS","VB_MAX_EDGE","VB_FULL_LIMIT","POLL_INTERVAL","DB_PATH_VALUEBET","POLYMARKET_FUNDER",
    # Лайв
    "BETBURGER_FILTER_ID_LIVE",
    "LV_MIN_ROI","LV_MIN_LIQUIDITY","LV_MIN_STAKE","LV_MAX_STAKE_PCT",
    "LV_STAKE_PCT","LV_USE_KELLY","LV_MAX_ODDS","LV_MAX_EDGE","LV_ORDER_TTL_SECS","LV_POLL_INTERVAL",
    # Resell
    "VB_RESELL_ENABLED","VB_RESELL_MARKUP","VB_RESELL_FALLBACK",
    "LV_RESELL_ENABLED","LV_RESELL_MARKUP","LV_RESELL_FALLBACK",
    "PM_ORDER_TTL_SECS",
    # Фильтры спортов
    "EXCLUDED_SPORTS","EXCLUDED_LEAGUES","ESPORT_MAX_MAP",
    # Dutching
    "DUTCH_MIN_SPREAD","DUTCH_MIN_LIQUIDITY","DUTCH_STAKE","DUTCH_MAX_STAKE",
    "DUTCH_POLL_INTERVAL","DUTCH_SPORTS","DUTCH_ORDER_TTL_SECS",
    # Market Making
    "MM_LEVELS","MM_STEP","MM_ORDER_SIZE","MM_POLL_INTERVAL","MM_MAX_MARKETS",
    "MM_REQUOTE_THRESHOLD","MM_AUTO_SEARCH","MM_AUTO_SPORTS","MM_AUTO_MIN_LIQ",
    "MM_SKEW_STEP","MM_SKEW_MAX","MM_MAX_POSITION","MM_SPREAD_PANIC","MM_ANCHOR","MM_SELL_ENABLED",
    # Settlement Sniper
    "SNIPE_MIN_PRICE","SNIPE_MAX_PRICE","SNIPE_ORDER_SIZE","SNIPE_MAX_POSITIONS",
    "SNIPE_SCAN_INTERVAL","SNIPE_SPORTS","SNIPE_TAGS","SNIPE_AUTO_MODE","SNIPE_MAX_DAYS_TO_END",
]

# ── Backlog / Kanban ──────────────────────────────────────────────────────────

@app.route("/api/backlog", methods=["GET"])
def api_backlog_list():
    try:
        db = _get_db()
        rows = db.conn.execute(
            "SELECT * FROM backlog ORDER BY sort_order, id"
        ).fetchall()
        return jsonify(ok=True, items=[dict(r) for r in rows])
    except Exception as e:
        return jsonify(ok=False, error=str(e)), 500

@app.route("/api/backlog", methods=["POST"])
def api_backlog_add():
    try:
        db = _get_db()
        body = request.get_json(force=True) or {}
        title = body.get("title", "").strip()
        if not title:
            return jsonify(ok=False, error="title required"), 400
        db.conn.execute(
            "INSERT INTO backlog (title, description, status, priority, category) VALUES (?,?,?,?,?)",
            (title, body.get("description", ""), body.get("status", "idea"),
             body.get("priority", "medium"), body.get("category", ""))
        )
        db.conn.commit()
        return jsonify(ok=True)
    except Exception as e:
        return jsonify(ok=False, error=str(e)), 500

@app.route("/api/backlog/<int:item_id>", methods=["PUT"])
def api_backlog_update(item_id):
    try:
        db = _get_db()
        body = request.get_json(force=True) or {}
        sets, vals = [], []
        for k in ["title", "description", "status", "priority", "category", "sort_order"]:
            if k in body:
                sets.append(f"{k}=?")
                vals.append(body[k])
        if not sets:
            return jsonify(ok=False, error="nothing to update"), 400
        sets.append("updated_at=datetime('now')")
        vals.append(item_id)
        db.conn.execute(f"UPDATE backlog SET {','.join(sets)} WHERE id=?", vals)
        db.conn.commit()
        return jsonify(ok=True)
    except Exception as e:
        return jsonify(ok=False, error=str(e)), 500

@app.route("/api/backlog/<int:item_id>", methods=["DELETE"])
def api_backlog_delete(item_id):
    try:
        db = _get_db()
        db.conn.execute("DELETE FROM backlog WHERE id=?", (item_id,))
        db.conn.commit()
        return jsonify(ok=True)
    except Exception as e:
        return jsonify(ok=False, error=str(e)), 500

@app.route("/api/config", methods=["GET","POST"])
def api_config():
    if request.method == "GET":
        try:
            load_dotenv(str(ENV_FILE), override=True)
            from config import Config
            cfg  = Config()
            data = {k: getattr(cfg, k, os.getenv(k, "")) for k in SAFE_KEYS}
            return jsonify(ok=True, config=data)
        except Exception as e:
            return jsonify(ok=False, error=str(e)), 500
    else:
        try:
            body = request.get_json(force=True) or {}
            for k, v in body.items():
                if k in SAFE_KEYS:
                    set_key(str(ENV_FILE), k, str(v))
            return jsonify(ok=True, updated=list(body.keys()))
        except Exception as e:
            return jsonify(ok=False, error=str(e)), 500

@app.route("/api/bankroll", methods=["POST"])
def api_bankroll():
    try:
        amount = float((request.get_json(force=True) or {}).get("amount", 0))
        if amount <= 0:
            return jsonify(ok=False, error="amount must be > 0"), 400
        _get_db().set_bankroll(amount)
        return jsonify(ok=True, bankroll=amount)
    except Exception as e:
        return jsonify(ok=False, error=str(e)), 500


@app.route("/api/free-usdc", methods=["GET", "POST"])
def api_free_usdc():
    """GET — читает свободный USDC. POST {amount: X} — устанавливает вручную."""
    db = _get_db()
    if request.method == "POST":
        try:
            amount = float((request.get_json(force=True) or {}).get("amount", -1))
            if amount < 0:
                return jsonify(ok=False, error="amount must be >= 0"), 400
            db.set_free_usdc(amount)
            return jsonify(ok=True, free_usdc=amount)
        except Exception as e:
            return jsonify(ok=False, error=str(e)), 500
    else:
        val = db.get_free_usdc()
        return jsonify(ok=True, free_usdc=val)

@app.route("/api/settle/<int:bet_id>", methods=["POST"])
def api_settle(bet_id):
    try:
        body   = request.get_json(force=True) or {}
        result = body.get("result", "")
        if result not in ("won","lost","void","push","sold"):
            return jsonify(ok=False, error="result must be won/lost/void/push/sold"), 400

        profit_raw = body.get("profit")
        sell_price_raw = body.get("sell_price")

        # Если profit передан явно (из pnl-inp) — используем его
        # Но проверяем что он не равен -shares (старая ошибка)
        db = _get_db()
        row = db.conn.execute("SELECT * FROM bets WHERE id=?", (bet_id,)).fetchone()

        sell_price_val = 0.0
        if row:
            keys        = row.keys()
            shares      = float(row["stake"] or 0)
            stake_price = float(row["stake_price"] or 0) if "stake_price" in keys else 0
            bb_price    = float(row["bb_price"] or 0)
            entry_price = stake_price if stake_price > 0 else bb_price
            cost        = round(shares * entry_price, 2)  # реально потрачено USDC
            payout      = shares                           # $1 × shares при победе

            if result == "sold":
                # Досрочная продажа
                sell_price_val = float(sell_price_raw) if sell_price_raw is not None else 0
                if profit_raw is not None:
                    profit = float(profit_raw)
                elif sell_price_val > 0:
                    proceeds = round(shares * sell_price_val, 2)
                    profit = round(proceeds - cost, 2)
                else:
                    profit = 0.0
            elif profit_raw is not None:
                profit = float(profit_raw)
                # Защита от старой ошибки: если lost и profit == -shares (не cost) — пересчитываем
                if result == "lost" and abs(profit + shares) < 0.01 and abs(cost - shares) > 0.01:
                    profit = round(-cost, 2)
            else:
                # Автоматический расчёт
                if result == "won":
                    profit = round(payout - cost, 2)
                elif result == "lost":
                    profit = round(-cost, 2)
                else:
                    profit = 0.0
        else:
            profit = float(profit_raw or 0)

        n = db.settle_by_id(bet_id, result, profit, sell_price=sell_price_val)
        # Корректируем свободный USDC
        if result == "won" and row:
            db.adjust_free_usdc(round(float(row["stake"] or 0), 2))  # payout = shares
        elif result == "sold" and row:
            # При продаже возвращаем proceeds (sell_price × shares)
            if sell_price_val > 0:
                proceeds = round(shares * sell_price_val, 2)
            else:
                proceeds = round(cost + profit, 2)  # cost + profit = proceeds
            db.adjust_free_usdc(proceeds)
        return jsonify(ok=True, updated=n, profit=profit)
    except Exception as e:
        return jsonify(ok=False, error=str(e)), 500

@app.route("/api/stats/resell")
def api_stats_resell():
    try:
        db = _get_db()
        rs = db.get_resell_stats()
        return jsonify(ok=True, **rs)
    except Exception as e:
        log.exception("api_stats_resell")
        return jsonify(ok=False, error=str(e)), 500

def _bot_thread_fn():
    global _bot_running, _bot_loop, _bot_start_ts
    try:
        load_dotenv(override=True)
        # ── Настраиваем запись лога в файл (если ещё не настроена) ──────────
        log_path = BOT_LOG_FILE
        root_logger = logging.getLogger()
        already_has_file = any(
            isinstance(h, logging.FileHandler) and getattr(h, 'baseFilename', '') == os.path.abspath(log_path)
            for h in root_logger.handlers
        )
        if not already_has_file:
            try:
                fh = logging.FileHandler(log_path, encoding="utf-8")
                fh.setFormatter(logging.Formatter(
                    "%(asctime)s  %(levelname)-7s  %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S"
                ))
                root_logger.addHandler(fh)
                root_logger.setLevel(logging.INFO)
                log.info("Лог бота → %s", log_path)
            except Exception as e:
                log.warning("Не удалось открыть лог-файл %s: %s", log_path, e)
        _bot_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(_bot_loop)
        from valuebet_bot import ValueBetBot
        bot = ValueBetBot()
        _bot_start_ts = time.time()
        _bot_running  = True
        _bot_loop.run_until_complete(bot.run())
    except Exception as e:
        log.error("Bot thread error: %s", e, exc_info=True)
    finally:
        _bot_running  = False
        _bot_start_ts = None

@app.route("/api/bot/start", methods=["POST"])
def api_bot_start():
    global _bot_thread, _bot_running
    if _bot_running:
        return jsonify(ok=False, error="Bot already running")
    t = threading.Thread(target=_bot_thread_fn, daemon=True, name="valuebet-bot")
    t.start(); _bot_thread = t; time.sleep(0.3)
    return jsonify(ok=True, status="starting")

@app.route("/api/bot/stop", methods=["POST"])
def api_bot_stop():
    global _bot_running, _bot_loop
    if not _bot_running:
        return jsonify(ok=False, error="Bot not running")
    _bot_running = False
    if _bot_loop and _bot_loop.is_running():
        _bot_loop.call_soon_threadsafe(_bot_loop.stop)
    return jsonify(ok=True, status="stopping")

@app.route("/api/bot/status")
def api_bot_status():
    up = int(time.time() - _bot_start_ts) if _bot_start_ts else 0
    live_up = int(time.time() - _live_start_ts) if _live_start_ts else 0
    return jsonify(ok=True, running=_bot_running, uptime=up,
                   live_running=_live_running, live_uptime=live_up)

@app.route("/api/bot/log")
def api_bot_log():
    n = int(request.args.get("lines", 80))
    return jsonify(ok=True, lines=_read_log(n))


def _live_bot_thread_fn():
    global _live_running, _live_loop, _live_start_ts
    try:
        load_dotenv(override=True)
        # Логируем в тот же файл
        log_path = BOT_LOG_FILE
        root_logger = logging.getLogger()
        already_has_file = any(
            isinstance(h, logging.FileHandler) and getattr(h, 'baseFilename', '') == os.path.abspath(log_path)
            for h in root_logger.handlers
        )
        if not already_has_file:
            try:
                fh = logging.FileHandler(log_path, encoding="utf-8")
                fh.setFormatter(logging.Formatter(
                    "%(asctime)s  %(levelname)-7s  %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S"
                ))
                root_logger.addHandler(fh)
                root_logger.setLevel(logging.INFO)
            except Exception as e:
                log.warning("Live бот: не удалось открыть лог-файл: %s", e)
        _live_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(_live_loop)
        from live_bot import LiveValueBetBot
        bot = LiveValueBetBot()
        _live_start_ts = time.time()
        _live_running  = True
        _live_loop.run_until_complete(bot.run())
    except Exception as e:
        log.error("[LIVE] Bot thread error: %s", e, exc_info=True)
    finally:
        _live_running  = False
        _live_start_ts = None

@app.route("/api/live/start", methods=["POST"])
def api_live_start():
    global _live_thread, _live_running
    if _live_running:
        return jsonify(ok=False, error="Live bot already running")
    t = threading.Thread(target=_live_bot_thread_fn, daemon=True, name="live-valuebet-bot")
    t.start(); _live_thread = t; time.sleep(0.3)
    return jsonify(ok=True, status="starting")

@app.route("/api/live/stop", methods=["POST"])
def api_live_stop():
    global _live_running, _live_loop
    if not _live_running:
        return jsonify(ok=False, error="Live bot not running")
    _live_running = False
    if _live_loop and _live_loop.is_running():
        _live_loop.call_soon_threadsafe(_live_loop.stop)
    return jsonify(ok=True, status="stopping")


# ── Dutching Bot ──────────────────────────────────────────────────────────────

_dutch_thread   = None
_dutch_loop     = None
_dutch_running  = False
_dutch_start_ts = None
_dutch_bot      = None

def _dutch_thread_fn():
    global _dutch_running, _dutch_loop, _dutch_start_ts, _dutch_bot
    try:
        load_dotenv(override=True)
        # Логируем в тот же файл
        log_path = BOT_LOG_FILE
        root_logger = logging.getLogger()
        already_has_file = any(
            isinstance(h, logging.FileHandler) and getattr(h, 'baseFilename', '') == os.path.abspath(log_path)
            for h in root_logger.handlers
        )
        if not already_has_file:
            try:
                fh = logging.FileHandler(log_path, encoding="utf-8")
                fh.setFormatter(logging.Formatter(
                    "%(asctime)s  %(levelname)-7s  %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S"
                ))
                root_logger.addHandler(fh)
                root_logger.setLevel(logging.INFO)
            except Exception as e:
                log.warning("Dutch log file error: %s", e)
        _dutch_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(_dutch_loop)
        from dutching_bot import DutchingBot
        _dutch_bot = DutchingBot()
        _dutch_start_ts = time.time()
        _dutch_running  = True
        _dutch_loop.run_until_complete(_dutch_bot.run())
    except Exception as e:
        log.error("Dutch thread error: %s", e, exc_info=True)
    finally:
        _dutch_running  = False
        _dutch_start_ts = None
        _dutch_bot      = None

@app.route("/api/dutch/start", methods=["POST"])
def api_dutch_start():
    global _dutch_thread, _dutch_running
    if _dutch_running:
        return jsonify(ok=False, error="Dutching bot already running")
    t = threading.Thread(target=_dutch_thread_fn, daemon=True, name="dutching-bot")
    t.start(); _dutch_thread = t; time.sleep(0.3)
    return jsonify(ok=True, status="starting")

@app.route("/api/dutch/stop", methods=["POST"])
def api_dutch_stop():
    global _dutch_running, _dutch_loop, _dutch_bot
    if not _dutch_running:
        return jsonify(ok=False, error="Dutching bot not running")
    if _dutch_bot:
        _dutch_bot.stop()
    _dutch_running = False
    if _dutch_loop and _dutch_loop.is_running():
        _dutch_loop.call_soon_threadsafe(_dutch_loop.stop)
    return jsonify(ok=True, status="stopping")

@app.route("/api/dutch/status")
def api_dutch_status():
    up = int(time.time() - _dutch_start_ts) if _dutch_start_ts else 0
    return jsonify(ok=True, running=_dutch_running, uptime=up)

@app.route("/api/dutch/stats")
def api_dutch_stats():
    try:
        db = _get_db()
        stats = db.get_dutch_stats()
        return jsonify(ok=True, **stats)
    except Exception as e:
        return jsonify(ok=False, error=str(e)), 500

@app.route("/api/dutch/pairs")
def api_dutch_pairs():
    try:
        db = _get_db()
        pairs = db.get_dutch_pairs(limit=50)
        return jsonify(ok=True, pairs=pairs)
    except Exception as e:
        return jsonify(ok=False, error=str(e)), 500

@app.route("/api/dutch/log")
def api_dutch_log():
    n = int(request.args.get("lines", 60))
    all_lines = _read_log(max(n * 10, 500))  # читаем больше, потом фильтруем
    dutch_lines = [l for l in all_lines if "[dutch" in l.lower() or "dutching" in l.lower() or "Dutching" in l]
    return jsonify(ok=True, lines=dutch_lines[-n:])

# ── Settlement Sniper Bot ─────────────────────────────────────────────────────

_snipe_thread   = None
_snipe_loop     = None
_snipe_running  = False
_snipe_start_ts = None
_snipe_bot      = None

def _snipe_thread_fn():
    global _snipe_running, _snipe_loop, _snipe_start_ts, _snipe_bot
    try:
        load_dotenv(override=True)
        log_path = BOT_LOG_FILE
        root_logger = logging.getLogger()
        already = any(isinstance(h, logging.FileHandler) and
                      getattr(h, 'baseFilename', '') == os.path.abspath(log_path)
                      for h in root_logger.handlers)
        if not already:
            try:
                fh = logging.FileHandler(log_path, encoding="utf-8")
                fh.setFormatter(logging.Formatter("%(asctime)s  %(levelname)-7s  %(message)s",
                                                   datefmt="%Y-%m-%d %H:%M:%S"))
                root_logger.addHandler(fh)
                root_logger.setLevel(logging.INFO)
            except Exception:
                pass
        _snipe_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(_snipe_loop)
        from settlement_bot import SettlementSniper
        _snipe_bot = SettlementSniper()
        _snipe_start_ts = time.time()
        _snipe_running = True
        _snipe_loop.run_until_complete(_snipe_bot.run())
    except Exception as e:
        log.error("Settlement Sniper thread error: %s", e, exc_info=True)
    finally:
        _snipe_running = False
        _snipe_start_ts = None
        _snipe_bot = None

@app.route("/api/snipe/start", methods=["POST"])
def api_snipe_start():
    global _snipe_thread, _snipe_running
    if _snipe_running:
        return jsonify(ok=False, error="Settlement Sniper already running")
    t = threading.Thread(target=_snipe_thread_fn, daemon=True, name="settlement-sniper")
    t.start(); _snipe_thread = t; time.sleep(0.3)
    return jsonify(ok=True, status="starting")

@app.route("/api/snipe/stop", methods=["POST"])
def api_snipe_stop():
    global _snipe_running, _snipe_loop, _snipe_bot
    if not _snipe_running:
        return jsonify(ok=False, error="Settlement Sniper not running")
    if _snipe_bot:
        _snipe_bot.stop()
    _snipe_running = False
    if _snipe_loop and _snipe_loop.is_running():
        _snipe_loop.call_soon_threadsafe(_snipe_loop.stop)
    return jsonify(ok=True, status="stopping")

@app.route("/api/snipe/status")
def api_snipe_status():
    up = int(time.time() - _snipe_start_ts) if _snipe_start_ts else 0
    return jsonify(ok=True, running=_snipe_running, uptime=up)

@app.route("/api/snipe/stats")
def api_snipe_stats():
    try:
        if _snipe_bot and _snipe_running:
            return jsonify(ok=True, **_snipe_bot.get_stats())
        return jsonify(ok=True, active=0, total_scanned=0, total_sniped=0,
                       total_settled=0, total_profit=0, snipes=[])
    except Exception as e:
        return jsonify(ok=False, error=str(e)), 500

@app.route("/api/snipe/log")
def api_snipe_log():
    n = int(request.args.get("lines", 40))
    all_lines = _read_log(max(n * 10, 500))
    snipe_lines = [l for l in all_lines if "[snipe" in l.lower()]
    return jsonify(ok=True, lines=snipe_lines[-n:])

@app.route("/api/snipe/approve", methods=["POST"])
def api_snipe_approve():
    if not _snipe_bot or not _snipe_running:
        return jsonify(ok=False, error="Bot not running")
    body = request.get_json(force=True) or {}
    cid = body.get("condition_id", "")
    size = float(body.get("size", 0))
    if not cid:
        return jsonify(ok=False, error="condition_id required")
    import asyncio
    future = asyncio.run_coroutine_threadsafe(
        _snipe_bot.approve_candidate(cid, custom_size=size), _snipe_loop)
    err = future.result(timeout=10)
    if err:
        return jsonify(ok=False, error=err)
    return jsonify(ok=True)

@app.route("/api/snipe/reject", methods=["POST"])
def api_snipe_reject():
    if not _snipe_bot or not _snipe_running:
        return jsonify(ok=False, error="Bot not running")
    body = request.get_json(force=True) or {}
    cid = body.get("condition_id", "")
    if not cid:
        return jsonify(ok=False, error="condition_id required")
    _snipe_bot.reject_candidate(cid)
    return jsonify(ok=True)

@app.route("/api/snipe/mode", methods=["POST"])
def api_snipe_mode():
    if not _snipe_bot or not _snipe_running:
        return jsonify(ok=False, error="Bot not running")
    body = request.get_json(force=True) or {}
    auto = body.get("auto", False)
    _snipe_bot.set_auto_mode(bool(auto))
    return jsonify(ok=True, auto_mode=bool(auto))

# ── Market Making Bot ────────────────────────────────────────────────────────

_mm_thread   = None
_mm_loop     = None
_mm_running  = False
_mm_start_ts = None
_mm_bot      = None

def _mm_thread_fn():
    global _mm_running, _mm_loop, _mm_start_ts, _mm_bot
    try:
        load_dotenv(override=True)
        log_path = BOT_LOG_FILE
        root_logger = logging.getLogger()
        already = any(isinstance(h, logging.FileHandler) and
                      getattr(h, 'baseFilename', '') == os.path.abspath(log_path)
                      for h in root_logger.handlers)
        if not already:
            try:
                fh = logging.FileHandler(log_path, encoding="utf-8")
                fh.setFormatter(logging.Formatter("%(asctime)s  %(levelname)-7s  %(message)s",
                                                  datefmt="%Y-%m-%d %H:%M:%S"))
                root_logger.addHandler(fh); root_logger.setLevel(logging.INFO)
            except Exception:
                pass
        _mm_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(_mm_loop)
        from mm_bot import MarketMaker
        _mm_bot = MarketMaker()
        _mm_start_ts = time.time()
        _mm_running = True
        _mm_loop.run_until_complete(_mm_bot.run())
    except Exception as e:
        log.error("MM thread error: %s", e, exc_info=True)
    finally:
        _mm_running = False; _mm_start_ts = None; _mm_bot = None

@app.route("/api/mm/start", methods=["POST"])
def api_mm_start():
    global _mm_thread, _mm_running
    if _mm_running:
        return jsonify(ok=False, error="MM already running")
    t = threading.Thread(target=_mm_thread_fn, daemon=True, name="mm-bot")
    t.start(); _mm_thread = t; time.sleep(0.3)
    return jsonify(ok=True, status="starting")

@app.route("/api/mm/stop", methods=["POST"])
def api_mm_stop():
    global _mm_running, _mm_loop, _mm_bot
    if not _mm_running:
        return jsonify(ok=False, error="MM not running")
    if _mm_bot:
        _mm_bot.stop()
    _mm_running = False
    if _mm_loop and _mm_loop.is_running():
        _mm_loop.call_soon_threadsafe(_mm_loop.stop)
    return jsonify(ok=True, status="stopping")

@app.route("/api/mm/status")
def api_mm_status():
    up = int(time.time() - _mm_start_ts) if _mm_start_ts else 0
    return jsonify(ok=True, running=_mm_running, uptime=up)

@app.route("/api/mm/stats")
def api_mm_stats():
    try:
        db = _get_db()
        return jsonify(ok=True, **db.mm_get_stats())
    except Exception as e:
        return jsonify(ok=False, error=str(e)), 500

@app.route("/api/mm/markets")
def api_mm_markets():
    try:
        if _mm_bot and _mm_running:
            return jsonify(ok=True, markets=_mm_bot.get_active_markets())
        db = _get_db()
        markets = db.mm_get_active_markets()
        return jsonify(ok=True, markets=markets)
    except Exception as e:
        return jsonify(ok=False, error=str(e)), 500

@app.route("/api/mm/add", methods=["POST"])
def api_mm_add():
    try:
        body = request.get_json(force=True) or {}
        cid = body.get("condition_id", "")
        if not cid:
            return jsonify(ok=False, error="condition_id required"), 400
        if _mm_bot and _mm_running:
            err = _mm_bot.add_market(
                cid, body.get("token_yes",""), body.get("token_no",""),
                body.get("question",""), body.get("event_name",""),
                body.get("sport",""), body.get("neg_risk", False),
                body.get("tick_size","0.01"))
            if err:
                return jsonify(ok=False, error=err)
        else:
            db = _get_db()
            db.mm_add_market(cid, body.get("token_yes",""), body.get("token_no",""),
                             body.get("question",""), body.get("event_name",""),
                             body.get("sport",""), body.get("neg_risk", False),
                             body.get("tick_size","0.01"))
        return jsonify(ok=True)
    except Exception as e:
        return jsonify(ok=False, error=str(e)), 500

@app.route("/api/mm/remove", methods=["POST"])
def api_mm_remove():
    try:
        body = request.get_json(force=True) or {}
        cid = body.get("condition_id", "")
        if _mm_bot and _mm_running:
            asyncio.run_coroutine_threadsafe(_mm_bot.remove_market(cid), _mm_loop)
        else:
            db = _get_db()
            db.mm_remove_market(cid)
        return jsonify(ok=True)
    except Exception as e:
        return jsonify(ok=False, error=str(e)), 500

@app.route("/api/mm/pause", methods=["POST"])
def api_mm_pause():
    try:
        body = request.get_json(force=True) or {}
        cid = body.get("condition_id", "")
        paused = body.get("paused", True)
        db = _get_db()
        db.conn.execute("UPDATE mm_markets SET paused=? WHERE condition_id=?",
                        (1 if paused else 0, cid))
        db.conn.commit()
        if _mm_bot and cid in _mm_bot._markets:
            _mm_bot._markets[cid]["paused"] = bool(paused)
        return jsonify(ok=True, paused=paused)
    except Exception as e:
        return jsonify(ok=False, error=str(e)), 500

@app.route("/api/mm/prematch_only", methods=["POST"])
def api_mm_prematch_only():
    try:
        body = request.get_json(force=True) or {}
        cid = body.get("condition_id", "")
        val = body.get("prematch_only", True)
        db = _get_db()
        db.conn.execute("UPDATE mm_markets SET prematch_only=? WHERE condition_id=?",
                        (1 if val else 0, cid))
        db.conn.commit()
        if _mm_bot and cid in _mm_bot._markets:
            _mm_bot._markets[cid]["prematch_only"] = bool(val)
        return jsonify(ok=True, prematch_only=val)
    except Exception as e:
        return jsonify(ok=False, error=str(e)), 500

@app.route("/api/mm/search")
def api_mm_search():
    try:
        q_raw = request.args.get("q", "")
        q = q_raw.lower()
        sport = request.args.get("sport", "")
        if not q:
            return jsonify(ok=False, error="query required"), 400
        from gamma_client import GammaClient
        gamma = GammaClient()

        markets = []

        # ── Прямой URL → ищем по slug ────────────────────────
        if 'polymarket.com' in q:
            path = q.split('polymarket.com/')[-1].split('?')[0].split('#')[0]
            slug = [p for p in path.split('/') if p][-1] if path else ""
            if slug:
                ev_data = gamma._get("/events", {"slug": slug, "limit": 1})
                if ev_data:
                    ev = ev_data[0] if isinstance(ev_data, list) else ev_data
                    for raw in ev.get("markets", []):
                        if raw.get("closed") == True or raw.get("acceptingOrders") == False:
                            continue
                        m = gamma._parse_market(raw)
                        if m and m.token_id_yes:
                            markets.append(_mm_market_dict(m, ev.get("title", ""), sport))
            return jsonify(ok=True, markets=markets[:30])

        # ── Condition ID напрямую ────────────────────────────
        if q.startswith('0x'):
            mkt = gamma._get(f"/markets/{q_raw}")
            if mkt:
                m = gamma._parse_market(mkt)
                if m and m.token_id_yes:
                    markets.append(_mm_market_dict(m, mkt.get("groupItemTitle", m.question), sport))
            return jsonify(ok=True, markets=markets)

        # ── Поиск по названию через events ───────────────────
        # Основные теги (без дублей типа epl/premier-league)
        all_sports = ["nba","nhl","mlb","nfl","soccer","tennis","mma",
                       "ncaa","esports","cs2","league-of-legends","dota","valorant",
                       "cricket","rugby","boxing"]
        sports_to_search = [sport] if sport else all_sports

        for sp in sports_to_search:
            try:
                events = gamma.get_events(tag=sp, limit=50)
            except Exception:
                continue
            for ev in events:
                title = (ev.get("title") or "").lower()
                if q not in title:
                    continue
                for raw in ev.get("markets", []):
                    # Фильтр: только открытые маркеты принимающие ордера
                    if raw.get("closed") == True or raw.get("acceptingOrders") == False:
                        continue
                    m = gamma._parse_market(raw)
                    if m and m.token_id_yes:
                        markets.append(_mm_market_dict(m, ev.get("title", ""), sp))
            if len(markets) >= 30:
                break

        return jsonify(ok=True, markets=markets[:20])
    except Exception as e:
        return jsonify(ok=False, error=str(e)), 500

def _mm_market_dict(m, event_title: str, sport: str) -> dict:
    """Helper: HedgeMarket → dict для UI."""
    prices = []
    try:
        if m.outcome_prices:
            prices = json.loads(m.outcome_prices) if isinstance(m.outcome_prices, str) else m.outcome_prices
    except Exception:
        pass
    mid_val = round((float(prices[0]) + (1 - float(prices[1]))) / 2, 2) if len(prices) >= 2 else "?"
    return {
        "condition_id": m.condition_id,
        "token_yes": m.token_id_yes,
        "token_no": m.token_id_no,
        "question": m.question,
        "event": event_title,
        "sport": sport,
        "neg_risk": m.neg_risk,
        "tick_size": "0.01",
        "mid": mid_val,
        "liq": "?",
    }

@app.route("/api/mm/fills")
def api_mm_fills():
    try:
        db = _get_db()
        cid = request.args.get("condition_id")
        fills = db.mm_get_fills(condition_id=cid, limit=50)
        return jsonify(ok=True, fills=fills)
    except Exception as e:
        return jsonify(ok=False, error=str(e)), 500

@app.route("/api/mm/log")
def api_mm_log():
    n = int(request.args.get("lines", 40))
    all_lines = _read_log(max(n * 10, 500))
    mm_lines = [l for l in all_lines if "[mm" in l.lower() or "market maker" in l.lower()]
    return jsonify(ok=True, lines=mm_lines[-n:])

@app.route("/api/mm/clear_stats", methods=["POST"])
def api_mm_clear_stats():
    try:
        db = _get_db()
        db.conn.execute("DELETE FROM mm_fills")
        db.conn.commit()
        # Reset in-memory positions if bot running
        if _mm_bot:
            for cid, mkt in _mm_bot._markets.items():
                mkt["yes_shares"] = 0
                mkt["no_shares"] = 0
                mkt["total_cost"] = 0
                mkt["fills_count"] = 0
                mkt["pnl"] = 0
        log.info("[mm] Статистика очищена")
        return jsonify(ok=True)
    except Exception as e:
        return jsonify(ok=False, error=str(e)), 500

# ── Line Movement ─────────────────────────────────────────────────────────────

@app.route("/api/bets/<int:bet_id>/line")
def api_bet_line(bet_id):
    try:
        db = _get_db()
        data = db.line_get_movement(bet_id)
        return jsonify(ok=True, **data)
    except Exception as e:
        return jsonify(ok=False, error=str(e)), 500

@app.route("/api/stats/line_movement")
def api_line_movement_stats():
    try:
        db = _get_db()
        data = db.line_get_stats()
        return jsonify(ok=True, **data)
    except Exception as e:
        return jsonify(ok=False, error=str(e)), 500

# ── /api/portfolio ────────────────────────────────────────────────────────────

@app.route("/api/portfolio")
def api_portfolio():
    try:
        import os
        from polymarket_client import PolymarketClient
        pk     = os.getenv("POLYMARKET_PRIVATE_KEY", "")
        funder = os.getenv("POLYMARKET_FUNDER", "")
        if not pk or not funder:
            return jsonify(ok=False, error="POLYMARKET_PRIVATE_KEY / FUNDER не настроены"), 400
        pm   = PolymarketClient(pk, funder)
        data = pm.get_portfolio()
        data.setdefault("ok", True)
        return jsonify(data)
    except Exception as e:
        log.exception("api_portfolio")
        return jsonify(ok=False, error=str(e)), 500


# ── /api/wallet — лёгкий снэпшот для хедера (кешируется 5 мин) ───────────────

_wallet_cache: dict = {"data": None, "ts": 0.0}
_WALLET_CACHE_TTL = 300  # секунд

@app.route("/api/wallet")
def api_wallet():
    """Cash + portfolio_value + redeemable — для отображения в хедере. Кеш 5 мин."""
    now = time.time()
    if _wallet_cache["data"] and now - _wallet_cache["ts"] < _WALLET_CACHE_TTL:
        cached = dict(_wallet_cache["data"])
        cached["cached"] = True
        cached["age"] = int(now - _wallet_cache["ts"])
        return jsonify(cached)
    try:
        from polymarket_client import PolymarketClient
        pk     = os.getenv("POLYMARKET_PRIVATE_KEY", "")
        funder = os.getenv("POLYMARKET_FUNDER", "")
        if not pk or not funder:
            return jsonify(ok=False, error="POLYMARKET_PRIVATE_KEY / FUNDER не настроены"), 400
        pm   = PolymarketClient(pk, funder)
        data = pm.get_wallet_snapshot()
        data["ts"] = int(now)
        _wallet_cache["data"] = data
        _wallet_cache["ts"]   = now
        return jsonify(data)
    except Exception as e:
        log.exception("api_wallet")
        return jsonify(ok=False, error=str(e)), 500


@app.route("/api/wallet/invalidate", methods=["POST"])
def api_wallet_invalidate():
    """Сбрасывает кеш wallet snapshot."""
    _wallet_cache["ts"] = 0.0
    return jsonify(ok=True)


@app.route("/api/wallet/debug")
def api_wallet_debug():
    """Диагностика: сырые данные баланса (без кеша)."""
    import json as _json, urllib.request as _ureq
    try:
        from polymarket_client import PolymarketClient
        from py_clob_client.clob_types import BalanceAllowanceParams, AssetType
        pk     = os.getenv("POLYMARKET_PRIVATE_KEY", "")
        funder = os.getenv("POLYMARKET_FUNDER", "")
        if not pk or not funder:
            return jsonify(ok=False, error="no keys"), 400
        pm = PolymarketClient(pk, funder)
        client = pm._get_client()

        result = {}

        # 1. CLOB balance_allowance COLLATERAL
        try:
            ba = client.get_balance_allowance(
                BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
            )
            result["clob_collateral"] = ba
        except Exception as e:
            result["clob_collateral_err"] = str(e)

        # 2. CLOB balance_allowance CONDITIONAL (пример)
        try:
            ba2 = client.get_balance_allowance(
                BalanceAllowanceParams(asset_type=AssetType.CONDITIONAL)
            )
            result["clob_conditional"] = ba2
        except Exception as e:
            result["clob_conditional_err"] = str(e)

        # 3. _usdc_balance (on-chain)
        try:
            result["onchain_usdc"] = pm._usdc_balance(funder.lower())
        except Exception as e:
            result["onchain_usdc_err"] = str(e)

        # 4. Data API positions (первые 3 для примера)
        for url in [
            f"https://data-api.polymarket.com/positions?user={funder.lower()}&limit=5",
            f"https://data-api.polymarket.com/positions?proxyWallet={funder.lower()}&limit=5",
        ]:
            try:
                req = _ureq.Request(url, headers={"User-Agent": "PolyBot/1.0"})
                with _ureq.urlopen(req, timeout=10) as r:
                    items = _json.loads(r.read())
                    if isinstance(items, list) and items:
                        result["data_api_sample"] = items[:2]
                        result["data_api_keys"]   = list(items[0].keys()) if items else []
                        break
            except Exception as e:
                result[f"data_api_err_{url[-20:]}"] = str(e)

        return jsonify(ok=True, **result)
    except Exception as e:
        return jsonify(ok=False, error=str(e)), 500


# ── /api/redeem ───────────────────────────────────────────────────────────────

@app.route("/api/redeem", methods=["POST"])
def api_redeem():
    """On-chain redemption всех settled позиций через CTF contract."""
    try:
        from polymarket_client import PolymarketClient
        pk     = os.getenv("POLYMARKET_PRIVATE_KEY", "")
        funder = os.getenv("POLYMARKET_FUNDER", "")
        if not pk or not funder:
            return jsonify(ok=False, error="POLYMARKET_PRIVATE_KEY / FUNDER не настроены"), 400
        pm     = PolymarketClient(pk, funder)
        result = pm.redeem_positions()
        # Инвалидируем кеш чтобы хедер обновился
        _wallet_cache["ts"] = 0.0
        return jsonify(result)
    except Exception as e:
        log.exception("api_redeem")
        return jsonify(ok=False, error=str(e)), 500


# ── /api/bets/fix-pnl ────────────────────────────────────────────────────────

@app.route("/api/bets/fix-pnl", methods=["POST"])
def api_fix_pnl():
    """
    Пересчитывает profit_actual для settled ставок где P&L записан неверно.
    Старая ошибка: при LOST записывался profit = -stake (shares), а не -cost_usdc.
    Исправляет: profit_lost = -(stake * stake_price), profit_won = stake - cost.
    """
    try:
        db = _get_db()
        rows = db.conn.execute("""
            SELECT id, stake, stake_price, bb_price, outcome_result, profit_actual
            FROM bets
            WHERE outcome_result IN ('won','lost')
              AND status IN ('settled','placed')
        """).fetchall()

        fixed = 0
        for r in rows:
            shares      = float(r["stake"] or 0)
            stake_price = float(r["stake_price"] or 0)
            bb_price    = float(r["bb_price"] or 0)
            entry_price = stake_price if stake_price > 0 else bb_price
            if entry_price <= 0:
                continue

            cost   = round(shares * entry_price, 2)
            payout = shares  # $1 × shares

            old_profit = float(r["profit_actual"] or 0)

            if r["outcome_result"] == "lost":
                correct = round(-cost, 2)
            else:  # won
                correct = round(payout - cost, 2)

            # Обновляем если отличие больше 1 цента
            if abs(old_profit - correct) > 0.01:
                db.conn.execute(
                    "UPDATE bets SET profit_actual=? WHERE id=?",
                    (correct, r["id"])
                )
                log.info("fix-pnl: #%d %s  old=%.2f → new=%.2f  (shares=%.2f cost=%.2f)",
                         r["id"], r["outcome_result"], old_profit, correct, shares, cost)
                fixed += 1

        db.conn.commit()
        return jsonify(ok=True, fixed=fixed, total=len(rows))
    except Exception as e:
        log.exception("api_fix_pnl")
        return jsonify(ok=False, error=str(e)), 500


# ── /api/bets/fix-wrong-won ──────────────────────────────────────────────────

@app.route("/api/bets/fix-wrong-won", methods=["POST"])
def api_fix_wrong_won():
    """
    Находит ставки где outcome_result='won' но токен на самом деле проиграл.
    Исправляет через Gamma API: проверяет outcomePrices для каждого outcome_id.
    Логика: если цена токена <= 0.01 — это LOST, >= 0.99 — WON (подтверждаем).
    """
    import urllib.request, json as _json, time as _time

    def gamma_price(outcome_id: str):
        """Возвращает текущую/финальную цену токена. Пробует Gamma, потом CLOB."""
        # 1. Пробуем CLOB API (не требует авторизации, более надёжный)
        try:
            url = f"https://clob.polymarket.com/prices-history?market={outcome_id}&fidelity=1&interval=all"
            req = urllib.request.Request(url, headers={
                "User-Agent": "Mozilla/5.0", "Accept": "application/json"
            })
            with urllib.request.urlopen(req, timeout=8) as r:
                data = _json.loads(r.read())
            history = data.get("history") or []
            if history:
                last_price = float(history[-1].get("p", -1))
                if 0 <= last_price <= 1:
                    return last_price
        except Exception as e:
            log.debug("clob_price err %s: %s", outcome_id, e)

        # 2. Fallback: Gamma API
        try:
            url = f"https://gamma-api.polymarket.com/markets?clobTokenIds={outcome_id}&limit=1"
            req = urllib.request.Request(url, headers={
                "User-Agent": "Mozilla/5.0", "Accept": "application/json"
            })
            with urllib.request.urlopen(req, timeout=10) as r:
                data = _json.loads(r.read())
            if not data:
                return None
            mkt = data[0]
            try:
                tids   = _json.loads(mkt.get("clobTokenIds") or "[]")
                prices = _json.loads(mkt.get("outcomePrices") or "[]")
            except Exception:
                tids, prices = [], []
            oid = str(outcome_id)
            for i, tid in enumerate(tids):
                if tid == oid and i < len(prices):
                    return float(prices[i])
        except Exception as e:
            log.debug("gamma_price err %s: %s", outcome_id, e)
        return None

    try:
        db = _get_db()
        # Берём все ставки с outcome_result='won' и status='settled'
        rows = db.conn.execute("""
            SELECT id, outcome_id, stake, stake_price, bb_price, profit_actual
            FROM bets
            WHERE outcome_result = 'won'
              AND status IN ('settled', 'placed')
        """).fetchall()

        checked = 0
        fixed   = 0
        errors  = []

        for row in rows:
            oid        = str(row["outcome_id"] or "")
            shares     = float(row["stake"] or 0)
            stake_price= float(row["stake_price"] or 0)
            bb_price   = float(row["bb_price"] or 0)
            entry_price= stake_price if stake_price > 0 else bb_price
            cost       = round(shares * entry_price, 2)

            if not oid:
                continue

            price = gamma_price(oid)
            checked += 1

            if price is None:
                # Не смогли получить цену — пропускаем
                continue

            if price <= 0.01:
                # Это LOST — исправляем
                correct_profit = round(-cost, 2)
                db.conn.execute("""
                    UPDATE bets
                    SET outcome_result='lost', profit_actual=?, status='settled'
                    WHERE id=?
                """, (correct_profit, row["id"]))
                log.info("fix-wrong-won: #%d WON→LOST  price=%.4f  profit %.2f→%.2f",
                         row["id"], price, row["profit_actual"], correct_profit)
                fixed += 1
            elif price >= 0.99:
                # Подтверждаем WON, но проверяем правильность profit
                correct_profit = round(shares - cost, 2)
                if abs(float(row["profit_actual"] or 0) - correct_profit) > 0.01:
                    db.conn.execute("""
                        UPDATE bets SET profit_actual=? WHERE id=?
                    """, (correct_profit, row["id"]))
                    fixed += 1

            # Небольшая пауза чтобы не перегружать Gamma API
            if checked % 10 == 0:
                _time.sleep(0.3)

        db.conn.commit()
        return jsonify(ok=True, checked=checked, fixed=fixed,
                       message=f"Проверено: {checked}, исправлено: {fixed}")
    except Exception as e:
        log.exception("api_fix_wrong_won")
        return jsonify(ok=False, error=str(e)), 500

@app.route("/api/bets/purge-pending", methods=["POST"])
def api_purge_pending():
    """
    Удаляет из БД ставки со статусом 'pending' которые не имеют order_id
    (то есть не были реально размещены на Polymarket).
    Возвращает количество удалённых строк.
    """
    try:
        db = _get_db()
        cur = db.conn.execute(
            "DELETE FROM bets WHERE status='pending' AND (order_id IS NULL OR order_id='')"
        )
        db.conn.commit()
        deleted = cur.rowcount
        log.info("purge-pending: удалено %d записей", deleted)
        return jsonify(ok=True, deleted=deleted)
    except Exception as e:
        log.exception("api_purge_pending")
        return jsonify(ok=False, error=str(e)), 500


@app.route("/api/portfolio/raw")
def api_portfolio_raw():
    """Debug: возвращает сырые данные с Polymarket API"""
    import os, urllib.request, json as _json
    funder = os.getenv("POLYMARKET_FUNDER","").lower()
    results = {}
    for name, url in [
        ("data_api", f"https://data-api.polymarket.com/positions?user={funder}&sizeThreshold=0.01&limit=3"),
        ("gamma_api", f"https://gamma-api.polymarket.com/positions?user={funder}&sizeThreshold=0.01&limit=3"),
    ]:
        try:
            req = urllib.request.Request(url, headers={"User-Agent":"Mozilla/5.0","Accept":"application/json"})
            with urllib.request.urlopen(req, timeout=10) as r:
                data = _json.loads(r.read())
            items = data if isinstance(data,list) else data.get("positions", data.get("data",[]))
            results[name] = {"first": items[0] if items else None, "count": len(items) if items else 0}
        except Exception as e:
            results[name] = {"error": str(e)}
    return jsonify(ok=True, results=results)


@app.route("/api/debug/betburger-feed")
def api_debug_betburger_feed():
    """
    Возвращает ВСЕ Polymarket беты из последнего raw файла BetBurger.
    ?mode=pre|live
    Включает вычисленный EV-edge, данные стакана, arb поля, passes_filter.
    """
    import json as _json, pathlib
    from urllib.parse import parse_qs, unquote
    try:
        load_dotenv(override=True)
        mode = request.args.get("mode", "pre")
        db_path = os.getenv("DB_PATH_VALUEBET", "valuebet.db")

        fname = "betburger_last_raw.json" if mode == "pre" else "betburger_last_raw_live.json"
        raw_path = pathlib.Path(db_path).parent / fname
        if not raw_path.exists():
            raw_path = BASE_DIR / fname
        if not raw_path.exists():
            return jsonify(ok=False, error=f"Файл {fname} не найден — запусти бота"), 404

        with open(raw_path, encoding="utf-8") as f:
            saved = _json.load(f)

        data = saved.get("data", {})
        bets = data.get("bets", []) if isinstance(data, dict) else (data if isinstance(data, list) else [])
        arbs = data.get("arbs", []) if isinstance(data, dict) else []
        source = data.get("source", {}) if isinstance(data, dict) else {}

        # Индекс arb по bet_id
        arb_by_bet = {}
        for arb in arbs:
            for key in ("bet1_id", "bet2_id", "bet3_id"):
                bid = arb.get(key)
                if bid:
                    arb_by_bet[bid] = arb

        # Индекс source.valueBets по bet_id — правильный велью% (= то что сайт BB показывает)
        source_vb_by_id = {}
        for vb in (source.get("valueBets") or []):
            bid = vb.get("bet_id") or vb.get("id")
            if bid:
                source_vb_by_id[bid] = vb

        # Параметры фильтра
        min_roi       = float(os.getenv("LV_MIN_ROI" if mode=="live" else "VB_MIN_ROI", "0.04"))
        min_liquidity = float(os.getenv("LV_MIN_LIQUIDITY" if mode=="live" else "VB_MIN_LIQUIDITY", "50"))
        max_odds      = float(os.getenv("LV_MAX_ODDS" if mode=="live" else "VB_MAX_ODDS", "0"))

        pm_bets = [b for b in bets if b.get("bookmaker_id") == 483]

        def _sf(v, d=0.0):
            if v is None or v == '' or v == 'null' or v == 'undefined': return d
            try: return float(v)
            except (ValueError, TypeError): return d

        result = []
        for bet in pm_bets:
            bid = bet.get("id", "")
            arb = arb_by_bet.get(bid, {})

            # Парсим direct_link
            dl_raw = bet.get("direct_link", "") or bet.get("bookmaker_event_direct_link", "")
            dl_params = {}
            order_book = []
            if dl_raw:
                decoded = unquote(dl_raw)
                parsed  = parse_qs(decoded, keep_blank_values=True)
                def _g(k, d=None):
                    v = parsed.get(k, [d]); return v[0] if v else d
                dl_params = {
                    "marketId":    _g("marketId",""),
                    "outcomeId":   _g("outcomeId",""),
                    "outcomeName": _g("outcomeName",""),
                    "competitive": _g("competitive",""),
                    "liquidityNum":_g("liquidityNum",""),
                    "negRisk":     _g("negRisk",""),
                }
                for pair in (_g("bestOffers","") or "").split(","):
                    try:
                        parts = pair.strip().split(":")
                        if len(parts) == 2:
                            odds = float(parts[0]); size = float(parts[1])
                            order_book.append({"odds": round(odds,6), "price": round(1/odds,6), "size": round(size,2)})
                    except Exception:
                        pass

            # Считаем велью% и арб%
            koef = _sf(bet.get("koef"))
            bb_price = round(1/koef, 6) if koef > 0 else 0
            best_ask = order_book[0]["price"] if order_book else None

            # middle_value = велью% как показывает сайт BetBurger
            middle_value = _sf(arb.get("middle_value"))
            # source.valueBets[].percent — тот же велью%, резервный источник
            svb = source_vb_by_id.get(bid, {})
            svb_pct = _sf(svb.get("percent"))
            # Приоритет: middle_value → source.valueBets.percent
            if middle_value > 0:
                computed_edge = round(middle_value, 3)
            elif svb_pct > 0:
                computed_edge = round(svb_pct if svb_pct > 1 else svb_pct * 100, 3)
            else:
                computed_edge = 0.0

            # arb_percent = доходность вилки (arbs[].percent) — сохраняем отдельно
            arb_percent = _sf(arb.get("percent"))

            liquidity = _sf(dl_params.get("liquidityNum") or bet.get("market_depth"))
            competitive = _sf(dl_params.get("competitive"))

            passes = (
                computed_edge >= min_roi * 100
                and liquidity >= min_liquidity
                and (max_odds == 0 or koef <= max_odds)
            )

            result.append({
                "event_name":    bet.get("event_name") or bet.get("name", ""),
                "league":        bet.get("league_name") or bet.get("league", ""),
                "is_live":       bool(bet.get("is_live")),
                "koef":          koef,
                "bb_price":      bb_price,
                "computed_edge": computed_edge,   # велью% = middle_value (как сайт BB)
                "arb_percent":   arb_percent,     # доходность вилки arbs[].percent
                "svb_percent":   svb_pct,         # source.valueBets[].percent (резерв)
                "best_ask":      best_ask,
                "liquidity":     liquidity,
                "competitive":   competitive,
                "order_book":    order_book,
                "passes_filter": passes,
                "direct_link_params": dl_params,
                "arb_raw":  {k:v for k,v in arb.items() if not k.endswith("_id") and k != "id"},
                "raw":      {k:v for k,v in bet.items() if k not in ("direct_link","bookmaker_event_direct_link","id")},
            })

        # Сортируем по велью% убыванию
        result.sort(key=lambda x: x["computed_edge"], reverse=True)

        return jsonify(
            ok         = True,
            mode       = mode,
            saved_at   = saved.get("saved_at"),
            total_bets = len(bets),
            total_arbs = len(arbs),
            filter     = {"min_roi_pct": min_roi*100, "min_liquidity": min_liquidity, "max_odds": max_odds},
            bets       = result,
        )
    except Exception as e:
        log.exception("api_debug_betburger_feed")
        return jsonify(ok=False, error=str(e)), 500


@app.route("/api/debug/betburger-raw")
def api_debug_betburger_raw():
    """
    Читает последний сырой ответ BetBurger сохранённый ботом.
    Файл обновляется автоматически при каждом тике бота.
    ?pretty=0 — без форматирования
    """
    import json as _json, pathlib
    try:
        load_dotenv(override=True)
        db_path  = os.getenv("DB_PATH_VALUEBET", "valuebet.db")
        raw_path = pathlib.Path(db_path).parent / "betburger_last_raw.json"
        if not raw_path.exists():
            # fallback — рядом со скриптом
            raw_path = BASE_DIR / "betburger_last_raw.json"
        if not raw_path.exists():
            return jsonify(ok=False, error="Файл ещё не создан — запусти бота и подожди один тик (~3 сек)"), 404

        with open(raw_path, encoding="utf-8") as f:
            saved = _json.load(f)

        data = saved.get("data", {})
        bets = data.get("bets", []) if isinstance(data, dict) else (data if isinstance(data, list) else [])
        arbs = data.get("arbs", []) if isinstance(data, dict) else []

        # Строим индекс arb по bet_id
        arb_by_bet = {}
        for arb in arbs:
            for key in ("bet1_id", "bet2_id", "bet3_id"):
                bid = arb.get(key)
                if bid:
                    arb_by_bet[bid] = arb

        # Берём первые 3 Polymarket бета + их arb (содержит готовый edge = arb.percent)
        pm_bets = [b for b in bets if b.get("bookmaker_id") == 483][:3]
        pm_arbs = [arb_by_bet.get(b.get("id"), {}) for b in pm_bets]

        # Edge summary для диагностики
        edge_summary = []
        for b, a in zip(pm_bets, pm_arbs):
            edge_summary.append({
                "event":       b.get("event_name", ""),
                "arb_percent": a.get("percent"),   # готовый edge от BetBurger
                "arb_found":   bool(a),
            })

        return jsonify(
            ok              = True,
            saved_at        = saved.get("saved_at"),
            total_bets      = len(bets),
            total_arbs      = len(arbs),
            polymarket_bets = len(pm_bets),
            all_bet_keys    = sorted(set(k for b in bets for k in b.keys())) if bets else [],
            all_arb_keys    = sorted(set(k for a in arbs for k in a.keys())) if arbs else [],
            sample_bets     = pm_bets,
            sample_arbs     = pm_arbs,
            edge_summary    = edge_summary,
        )
    except Exception as e:
        log.exception("api_debug_betburger_raw")
        return jsonify(ok=False, error=str(e)), 500


@app.route("/api/debug/betburger-raw-live")
def api_debug_betburger_raw_live():
    """Читает последний сырой ответ BetBurger Live API"""
    import json as _json, pathlib
    try:
        load_dotenv(override=True)
        db_path  = os.getenv("DB_PATH_VALUEBET", "valuebet.db")
        raw_path = pathlib.Path(db_path).parent / "betburger_last_raw_live.json"
        if not raw_path.exists():
            raw_path = BASE_DIR / "betburger_last_raw_live.json"
        if not raw_path.exists():
            return jsonify(ok=False, error="Live файл не создан — запусти лайв бот и подожди один тик"), 404
        with open(raw_path, encoding="utf-8") as f:
            saved = _json.load(f)
        data = saved.get("data", {})
        bets = data.get("bets", []) if isinstance(data, dict) else (data if isinstance(data, list) else [])
        arbs = data.get("arbs", []) if isinstance(data, dict) else []
        arb_by_bet = {}
        for arb in arbs:
            for key in ("bet1_id", "bet2_id", "bet3_id"):
                bid = arb.get(key)
                if bid:
                    arb_by_bet[bid] = arb
        pm_bets = [b for b in bets if b.get("bookmaker_id") == 483][:3]
        pm_arbs = [arb_by_bet.get(b.get("id"), {}) for b in pm_bets]
        return jsonify(
            ok              = True,
            saved_at        = saved.get("saved_at"),
            total_bets      = len(bets),
            total_arbs      = len(arbs),
            polymarket_bets = len(pm_bets),
            all_bet_keys    = sorted(set(k for b in bets for k in b.keys())) if bets else [],
            sample_bets     = pm_bets,
            sample_arbs     = pm_arbs,
        )
    except Exception as e:
        return jsonify(ok=False, error=str(e)), 500


@app.route("/api/debug/activity")
def api_debug_activity():
    import os, urllib.request, json as _json
    funder = os.getenv("POLYMARKET_FUNDER", "").lower()
    if not funder:
        return jsonify(ok=False, error="POLYMARKET_FUNDER не задан")
    try:
        def fetch(url):
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"})
            with urllib.request.urlopen(req, timeout=15) as r:
                return _json.loads(r.read())

        # ── БД ──────────────────────────────────────────────────────────────
        db = _get_db()
        active_bets   = db.get_active_bets()
        db_outcome_ids = {str(b.outcome_id or "") for b in active_bets if b.outcome_id}
        db_sample = [{
            "id": b.id, "home": b.home, "away": b.away,
            "outcome_id": str(b.outcome_id or ""),
            "market_id":  str(b.market_id  or ""),
        } for b in active_bets[:5]]

        # ── REDEEM активности ───────────────────────────────────────────────
        redeems = []
        for url in [
            f"https://data-api.polymarket.com/activity?user={funder}&type=REDEEM&limit=500",
            f"https://data-api.polymarket.com/activity?proxyWallet={funder}&type=REDEEM&limit=500",
        ]:
            try:
                data = fetch(url)
                if isinstance(data, list) and data:
                    redeems = data; break
            except Exception:
                pass

        # ── Тест: slug первого REDEEM → Gamma → token_ids ───────────────────
        slug_test = {}
        if redeems:
            test_act  = redeems[0]
            test_slug = test_act.get("slug") or ""
            if test_slug:
                try:
                    mkt = fetch(f"https://gamma-api.polymarket.com/markets?slug={test_slug}&limit=1")
                    if isinstance(mkt, list) and mkt:
                        mkt = mkt[0]
                        # clobTokenIds — JSON-строка, не массив!
                        raw = mkt.get("clobTokenIds") or "[]"
                        tid_list = _json.loads(raw) if isinstance(raw, str) else raw
                        tokens = [str(t) for t in tid_list if t]
                        matched = [t for t in tokens if t in db_outcome_ids]
                        slug_test = {
                            "slug":        test_slug,
                            "gamma_found": True,
                            "gamma_closed": mkt.get("closed"),
                            "token_ids":   tokens,
                            "matched_in_db": matched,
                        }
                    else:
                        slug_test = {"slug": test_slug, "gamma_found": False}
                except Exception as e:
                    slug_test = {"slug": test_slug, "error": str(e)}

        # ── Полная проверка всех REDEEM slug → сколько ставок найдено ───────
        total_matched_tokens = set()
        slugs_with_match = []
        for act in redeems[:30]:  # первые 30 REDEEM
            slug = act.get("slug") or ""
            if not slug:
                continue
            try:
                mkt = fetch(f"https://gamma-api.polymarket.com/markets?slug={slug}&limit=1")
                if isinstance(mkt, list) and mkt:
                    raw = mkt[0].get("clobTokenIds") or "[]"
                    tid_list = _json.loads(raw) if isinstance(raw, str) else raw
                    for t in tid_list:
                        tid = str(t)
                        if tid and tid in db_outcome_ids:
                            total_matched_tokens.add(tid)
                            slugs_with_match.append(slug)
            except Exception:
                pass

        # ── Вся activity — сколько token_ids совпадают с БД ─────────────────
        acts_all = []
        try:
            acts_all = fetch(f"https://data-api.polymarket.com/activity?user={funder}&limit=200") or []
        except Exception:
            pass
        all_act_assets = {str(a.get("asset") or "") for a in acts_all if a.get("asset")}
        direct_match = db_outcome_ids & all_act_assets

        return jsonify(
            ok=True,
            funder_prefix=funder[:10] + "...",
            db_active_count=len(active_bets),
            db_sample=db_sample,
            activity_types=sorted({(a.get("type") or "?") for a in acts_all}),
            redeem_count=len(redeems),
            redeem_example=redeems[0] if redeems else None,
            # Тест цепочки slug→Gamma→token_ids
            slug_gamma_test=slug_test,
            redeem_slugs_checked=min(30, len(redeems)),
            redeem_slugs_with_db_match=len(set(slugs_with_match)),
            tokens_found_via_redeem=len(total_matched_tokens),
            # Прямые совпадения asset в activity
            direct_asset_match_count=len(direct_match),
            direct_asset_match_sample=list(direct_match)[:3],
        )
    except Exception as e:
        log.exception("api_debug_activity")
        return jsonify(ok=False, error=str(e))


@app.route("/api/bet/<int:bet_id>/sell-info")
def api_bet_sell_info(bet_id):
    """Получает информацию о продаже позиции с Polymarket CLOB API."""
    try:
        import os
        db = _get_db()
        row = db.conn.execute("SELECT * FROM bets WHERE id=?", (bet_id,)).fetchone()
        if not row:
            return jsonify(ok=False, error="Ставка не найдена")

        outcome_id = row["outcome_id"]
        shares     = float(row["stake"] or 0)
        keys       = row.keys()
        stake_price = float(row["stake_price"] or 0) if "stake_price" in keys else 0
        bb_price    = float(row["bb_price"] or 0)
        entry_price = stake_price if stake_price > 0 else bb_price
        cost        = round(shares * entry_price, 2)
        placed_at   = row["placed_at"] or row["created_at"] or ""

        pk     = os.getenv("POLYMARKET_PRIVATE_KEY", "")
        funder = os.getenv("POLYMARKET_FUNDER", "")
        if not pk or not funder:
            return jsonify(ok=False, error="POLYMARKET_PRIVATE_KEY/FUNDER не заданы")

        from polymarket_client import PolymarketClient
        pm = PolymarketClient(pk, funder)

        # Получаем трейды для этого токена
        trades = pm.get_trades(asset_id=outcome_id)

        # Фильтруем SELL трейды
        sell_trades = []
        for t in trades:
            side = (t.get("side") or "").upper()
            if side == "SELL":
                sell_trades.append({
                    "price": float(t.get("price", 0)),
                    "size":  float(t.get("size", 0)),
                    "time":  t.get("matchTime") or t.get("createdAt") or "",
                })

        if sell_trades:
            total_proceeds = sum(t["price"] * t["size"] for t in sell_trades)
            total_shares_sold = sum(t["size"] for t in sell_trades)
            avg_sell_price = total_proceeds / total_shares_sold if total_shares_sold > 0 else 0
            profit = round(total_proceeds - cost, 2)
        else:
            # Нет SELL трейдов — берём текущую цену токена
            mid = pm.get_midpoint(outcome_id)
            avg_sell_price = mid if mid else 0
            total_proceeds = round(shares * avg_sell_price, 2) if avg_sell_price else 0
            total_shares_sold = 0
            profit = round(total_proceeds - cost, 2) if avg_sell_price else 0

        return jsonify(
            ok=True,
            outcome_id=outcome_id,
            shares=shares,
            entry_price=entry_price,
            cost=cost,
            sell_price=round(avg_sell_price, 4),
            proceeds=round(total_proceeds, 2),
            shares_sold=round(total_shares_sold, 2),
            profit=profit,
            sell_trades=sell_trades,
            has_sell_trades=len(sell_trades) > 0,
        )
    except Exception as e:
        log.exception("api_bet_sell_info")
        return jsonify(ok=False, error=str(e)), 500


@app.route("/api/settle/auto", methods=["POST"])
def api_settle_auto():
    """Немедленный запуск авто-расчёта ставок."""
    if not _HAS_AUTO_SETTLE:
        return jsonify(ok=False, error="auto_settle.py не найден")
    try:
        import os
        funder = os.getenv("POLYMARKET_FUNDER", "")
        if not funder:
            return jsonify(ok=False, error="POLYMARKET_FUNDER не задан")
        db = _get_db()
        worker = AutoSettleWorker(db, funder, interval=3600)
        n = worker.run_now()

        # Дополнительно: закрываем cancelled/failed ставки как void
        # Они не были реально размещены → P&L = 0, outcome_result = void
        voided = _void_unplaced_bets(db)

        return jsonify(ok=True, settled=n, voided=voided, message=f"Расчитано {n} ставок, закрыто {voided} отменённых")
    except Exception as e:
        log.exception("api_settle_auto"); return jsonify(ok=False, error=str(e)), 500


@app.route("/api/bets/fix-cancelled", methods=["POST"])
def api_fix_cancelled():
    """Закрывает cancelled/failed ставки как void (ставка не состоялась, P&L=0)."""
    try:
        db = _get_db()
        n = _void_unplaced_bets(db)
        return jsonify(ok=True, voided=n, message=f"Закрыто {n} отменённых ставок как void")
    except Exception as e:
        log.exception("api_fix_cancelled"); return jsonify(ok=False, error=str(e)), 500


def _void_unplaced_bets(db):
    """
    Ставки со статусом cancelled/failed никогда не были реально размещены.
    Помечаем их outcome_result=void, profit_actual=0.
    Это убирает их из 'нерасчитанных' и не искажает P&L/winrate.
    """
    import datetime as _dt
    now = _dt.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    cur = db.conn.execute("""
        UPDATE bets
        SET outcome_result = 'void',
            profit_actual  = 0,
            settled_at     = ?
        WHERE status IN ('cancelled', 'failed')
          AND outcome_result = 'pending'
    """, (now,))
    db.conn.commit()
    n = cur.rowcount
    if n > 0:
        log.info("_void_unplaced_bets: закрыто %d cancelled/failed ставок как void", n)
    return n

# ═══════════════════════════════════════════════════════
#  HEDGE API ROUTES
# ═══════════════════════════════════════════════════════

_hedge_db = None
def _get_hedge_db():
    global _hedge_db
    if _hedge_db is None:
        from db_hedge import HedgeDatabase
        from config import Config
        cfg = Config()
        _hedge_db = HedgeDatabase(cfg.HEDGE_DB_PATH)
    return _hedge_db

_gamma = None
def _get_gamma():
    global _gamma
    if _gamma is None:
        from gamma_client import GammaClient
        _gamma = GammaClient()
    return _gamma


# ── Price Cache ──────────────────────────────────────────────────────────
import time as _time
import threading as _threading
import uuid as _uuid

class PriceCache:
    """Thread-safe price cache with TTL."""
    def __init__(self, ttl=60):
        self._data = {}   # token -> (price, ts)
        self._lock = _threading.Lock()
        self.ttl = ttl

    def get(self, token_id):
        with self._lock:
            entry = self._data.get(token_id)
            if entry and (_time.time() - entry[1]) < self.ttl:
                return entry[0]
        return None

    def set(self, token_id, price):
        with self._lock:
            self._data[token_id] = (price, _time.time())

    def bulk_set(self, prices):
        now = _time.time()
        with self._lock:
            for tid, price in prices.items():
                self._data[tid] = (price, now)

    def missing(self, token_ids):
        now = _time.time()
        with self._lock:
            return {tid for tid in token_ids
                    if tid not in self._data or (now - self._data[tid][1]) >= self.ttl}

    def get_all(self, token_ids):
        now = _time.time()
        result = {}
        with self._lock:
            for tid in token_ids:
                entry = self._data.get(tid)
                if entry and (now - entry[1]) < self.ttl:
                    result[tid] = entry[0]
        return result

_price_cache = PriceCache(ttl=60)

# ── Async Analyze Tasks ──────────────────────────────────────────────────
_analyze_tasks: dict[str, dict] = {}
_analyze_lock = _threading.Lock()


@app.route("/api/hedge/calculate", methods=["POST"])
def api_hedge_calculate():
    """Рассчитать дельта-нейтральные позиции."""
    try:
        from hedge_calculator import calc_delta_neutral, calc_multi_scenario
        data = request.json
        price_a = float(data.get("price_a", 0))
        price_b = float(data.get("price_b", 0))
        budget = float(data.get("budget", 0))
        scenarios = data.get("scenarios", [])

        if len(scenarios) == 2:
            result = calc_delta_neutral(
                price_a, price_b,
                scenarios[0]["exit_a"], scenarios[0]["exit_b"],
                scenarios[1]["exit_a"], scenarios[1]["exit_b"],
                budget,
                scenario_names=(scenarios[0].get("name", "Scenario 1"),
                                scenarios[1].get("name", "Scenario 2")),
            )
        else:
            result = calc_multi_scenario(price_a, price_b, scenarios, budget)

        return jsonify(ok=True, result=result.to_dict())
    except Exception as e:
        log.exception("api_hedge_calculate")
        return jsonify(ok=False, error=str(e)), 500


@app.route("/api/hedge/save-calc", methods=["POST"])
def api_hedge_save_calc():
    """Сохранить расчёт калькулятора."""
    try:
        db = _get_hedge_db()
        data = request.json
        calc_id = db.save_calc(data)
        return jsonify(ok=True, id=calc_id)
    except Exception as e:
        log.exception("api_hedge_save_calc")
        return jsonify(ok=False, error=str(e)), 500


@app.route("/api/hedge/saved-calcs", methods=["GET"])
def api_hedge_saved_calcs():
    """Список сохранённых расчётов."""
    try:
        db = _get_hedge_db()
        calcs = db.get_saved_calcs()
        return jsonify(ok=True, calcs=calcs)
    except Exception as e:
        log.exception("api_hedge_saved_calcs")
        return jsonify(ok=False, error=str(e)), 500


@app.route("/api/hedge/delete-calc", methods=["POST"])
def api_hedge_delete_calc():
    """Удалить сохранённый расчёт."""
    try:
        db = _get_hedge_db()
        calc_id = request.json.get("id")
        db.delete_saved_calc(calc_id)
        return jsonify(ok=True)
    except Exception as e:
        log.exception("api_hedge_delete_calc")
        return jsonify(ok=False, error=str(e)), 500


@app.route("/api/hedge/scan", methods=["POST"])
def api_hedge_scan():
    """Сканировать Polymarket на пары матч+турнир."""
    try:
        from config import Config
        cfg = Config()
        gamma = _get_gamma()
        db = _get_hedge_db()
        # Поддержка фильтра по одному виду спорта
        single_sport = (request.json or {}).get("sport")
        if single_sport:
            sports = [single_sport]
        else:
            sports = [s.strip() for s in cfg.HEDGE_SPORTS.split(",") if s.strip()]
        pairs = gamma.find_hedge_pairs(
            sport_tags=sports, cross_tournament=cfg.HEDGE_CROSS_TOURNEY,
            knockout_only=cfg.HEDGE_KNOCKOUT_ONLY, min_tourney_price=cfg.HEDGE_MIN_TOURNEY_PRICE)
        count = 0
        for p in pairs:
            row_id = db.insert_pair(p)
            if row_id:
                count += 1
        return jsonify(ok=True, count=count, total=len(pairs))
    except Exception as e:
        log.exception("api_hedge_scan")
        return jsonify(ok=False, error=str(e)), 500


@app.route("/api/hedge/pairs", methods=["GET"])
def api_hedge_pairs():
    """Список найденных пар."""
    try:
        db = _get_hedge_db()
        status = request.args.get("status")
        pairs = db.get_pairs(status=status)
        return jsonify(ok=True, pairs=pairs)
    except Exception as e:
        log.exception("api_hedge_pairs")
        return jsonify(ok=False, error=str(e)), 500


@app.route("/api/hedge/positions", methods=["GET"])
def api_hedge_positions():
    """Список хедж-позиций."""
    try:
        db = _get_hedge_db()
        status = request.args.get("status")
        if status:
            positions = db.get_positions(status=status)
        else:
            positions = db.get_positions()
        return jsonify(ok=True, positions=positions)
    except Exception as e:
        log.exception("api_hedge_positions")
        return jsonify(ok=False, error=str(e)), 500


@app.route("/api/hedge/execute", methods=["POST"])
def api_hedge_execute():
    """Исполнить хедж — разместить оба ордера."""
    try:
        from config import Config
        from polymarket_client import PolymarketClient
        cfg = Config()
        pm = PolymarketClient(cfg.POLYMARKET_PRIVATE_KEY, cfg.POLYMARKET_FUNDER)
        db = _get_hedge_db()

        data = request.json
        token_a = data.get("token_id_a", "")
        token_b = data.get("token_id_b", "")
        price_a = float(data.get("price_a", 0))
        price_b = float(data.get("price_b", 0))
        size_a = float(data.get("size_a", 0))
        size_b = float(data.get("size_b", 0))
        neg_risk_a = data.get("neg_risk_a", False)
        neg_risk_b = data.get("neg_risk_b", False)

        if not token_a or not token_b:
            return jsonify(ok=False, error="Token IDs required"), 400

        # Запись позиции
        pos_id = db.insert_position({
            "pair_id": data.get("pair_id", "manual"),
            "token_id_a": token_a,
            "entry_price_a": price_a,
            "size_a": size_a,
            "cost_a": size_a * price_a,
            "neg_risk_a": 1 if neg_risk_a else 0,
            "token_id_b": token_b,
            "entry_price_b": price_b,
            "size_b": size_b,
            "cost_b": size_b * price_b,
            "neg_risk_b": 1 if neg_risk_b else 0,
            "budget": size_a * price_a + size_b * price_b,
            "expected_profit": data.get("expected_profit", 0),
            "expected_roi": data.get("expected_roi", 0),
            "scenarios": data.get("scenarios", []),
        })

        # Размещение ордера A
        import asyncio
        loop = asyncio.new_event_loop()
        res_a = loop.run_until_complete(pm.place_order(
            token_id=token_a, price=price_a,
            size=size_a * price_a,  # size in USDC for BUY
            neg_risk=neg_risk_a,
        ))
        if res_a.success:
            db.update_order(pos_id, "a", res_a.bet_id, "placed")
        else:
            db.update_order(pos_id, "a", "", "failed")

        # Размещение ордера B
        res_b = loop.run_until_complete(pm.place_order(
            token_id=token_b, price=price_b,
            size=size_b * price_b,  # size in USDC for BUY
            neg_risk=neg_risk_b,
        ))
        if res_b.success:
            db.update_order(pos_id, "b", res_b.bet_id, "placed")
        else:
            db.update_order(pos_id, "b", "", "failed")

        loop.close()

        return jsonify(ok=True, position_id=pos_id,
                       order_a={"success": res_a.success, "id": res_a.bet_id, "error": res_a.error},
                       order_b={"success": res_b.success, "id": res_b.bet_id, "error": res_b.error})
    except Exception as e:
        log.exception("api_hedge_execute")
        return jsonify(ok=False, error=str(e)), 500


@app.route("/api/hedge/stats", methods=["GET"])
def api_hedge_stats():
    """Статистика хеджей."""
    try:
        db = _get_hedge_db()
        stats = db.get_stats()
        return jsonify(ok=True, stats=stats)
    except Exception as e:
        log.exception("api_hedge_stats")
        return jsonify(ok=False, error=str(e)), 500


def _run_analyze_worker(task_id, sports, budget):
    """Background worker для анализа hedge-возможностей."""
    from config import Config
    from polymarket_client import PolymarketClient
    from hedge_calculator import analyze_hedge_full
    from collections import defaultdict
    from concurrent.futures import ThreadPoolExecutor, as_completed

    task = _analyze_tasks[task_id]
    try:
        cfg = Config()
        pm = PolymarketClient(cfg.POLYMARKET_PRIVATE_KEY, cfg.POLYMARKET_FUNDER)
        gamma = _get_gamma()

        all_opportunities = []

        for si, sport in enumerate(sports):
            task["current_sport"] = sport
            task["progress_pct"] = int(si / len(sports) * 100)

            # 1. Поиск пар для этого спорта
            pairs = gamma.find_hedge_pairs(
                sport_tags=[sport], cross_tournament=cfg.HEDGE_CROSS_TOURNEY,
                knockout_only=cfg.HEDGE_KNOCKOUT_ONLY,
                min_tourney_price=cfg.HEDGE_MIN_TOURNEY_PRICE)

            if not pairs:
                task["sport_results"][sport] = {"pairs": 0, "opps": 0, "best_roi": 0}
                continue

            # 2. Собираем все уникальные token_id
            token_ids = set()
            for p in pairs:
                if p.match_market and p.match_market.token_id_yes:
                    token_ids.add(p.match_market.token_id_yes)
                if p.tournament_market and p.tournament_market.token_id_yes:
                    token_ids.add(p.tournament_market.token_id_yes)

            # 3. Параллельная загрузка цен (ThreadPoolExecutor)
            to_fetch = _price_cache.missing(token_ids)
            log.info("Analyze %s: %d pairs, %d tokens (%d to fetch)",
                     sport, len(pairs), len(token_ids), len(to_fetch))

            if to_fetch:
                fetched = {}
                with ThreadPoolExecutor(max_workers=20) as executor:
                    futures = {executor.submit(pm.get_midpoint, tid): tid for tid in to_fetch}
                    for f in as_completed(futures):
                        tid = futures[f]
                        try:
                            fetched[tid] = f.result() or 0.0
                        except Exception:
                            fetched[tid] = 0.0
                _price_cache.bulk_set(fetched)

            # Все цены из кэша
            prices = _price_cache.get_all(token_ids)

            def gp(tok):
                return prices.get(tok, 0.0)

            # 4. Группировка для поиска pa_t, pb_t
            by_tourney = defaultdict(list)
            for p in pairs:
                by_tourney[p.event_name].append(p)

            # 4b. Мульти-матч модель: для каждого турнира считаем
            # parallel_eliminated_share для каждого матча
            # = суммарная expected eliminated доля от ДРУГИХ матчей
            parallel_shares = {}  # match_condition_id -> parallel_eliminated_share
            for te_title, te_pairs in by_tourney.items():
                # Собираем уникальные матчи этого турнира
                unique_matches = {}  # condition_id -> (player_a, player_b, match_price)
                for p in te_pairs:
                    cid = p.match_market.condition_id
                    if cid not in unique_matches:
                        mp_val = gp(p.match_market.token_id_yes)
                        unique_matches[cid] = (p.player_a, p.player_b, mp_val)

                if len(unique_matches) <= 1:
                    continue

                # Для каждого матча — expected eliminated share от остальных
                # В каждом другом матче вылетает один из двух:
                # expected_elim = pa_t * (1-mp) + pb_t * mp
                # (если A win prob = mp, то B вылетает с prob mp, A с prob 1-mp)
                for cid in unique_matches:
                    par_share = 0.0
                    for other_cid, (oa, ob, omp) in unique_matches.items():
                        if other_cid == cid:
                            continue
                        if not omp or omp <= 0 or omp >= 1:
                            continue
                        # Найдём турнирные цены обоих игроков другого матча
                        oa_t = 0.0
                        ob_t = 0.0
                        for rp in te_pairs:
                            if not rp.tournament_player:
                                continue
                            if gamma._names_match(rp.tournament_player, oa):
                                oa_t = gp(rp.tournament_market.token_id_yes) or 0.0
                            elif gamma._names_match(rp.tournament_player, ob):
                                ob_t = gp(rp.tournament_market.token_id_yes) or 0.0
                        # Expected eliminated = prob(A wins)*P_B_tourney + prob(B wins)*P_A_tourney
                        par_share += omp * ob_t + (1 - omp) * oa_t
                    parallel_shares[cid] = min(par_share, 0.5)  # cap at 50%

            log.info("Parallel shares computed for %d matches", len(parallel_shares))

            # 5. Анализ пар (чистая математика, без API вызовов)
            sport_opps = []
            for pair in pairs:
                try:
                    mt = pair.match_market
                    tt = pair.tournament_market
                    if not mt or not tt or not mt.token_id_yes or not tt.token_id_yes:
                        continue
                    mp = gp(mt.token_id_yes)
                    tp = gp(tt.token_id_yes)
                    if not mp or not tp or mp <= 0.02 or mp >= 0.98 or tp <= 0.01:
                        continue

                    pa_t = 0.02
                    pb_t = 0.02
                    for rp in by_tourney.get(pair.event_name, []):
                        if not rp.tournament_player:
                            continue
                        if gamma._names_match(rp.tournament_player, pair.player_a):
                            pa_t = gp(rp.tournament_market.token_id_yes) or 0.02
                        elif gamma._names_match(rp.tournament_player, pair.player_b):
                            pb_t = gp(rp.tournament_market.token_id_yes) or 0.02

                    par_share = parallel_shares.get(mt.condition_id, 0.0)

                    result = analyze_hedge_full(
                        match_price_a=mp, tourney_player_price=tp,
                        player_a_tourney_price=pa_t, player_b_tourney_price=pb_t,
                        budget=budget,
                        player_a=pair.player_a, player_b=pair.player_b,
                        tourney_player=pair.tournament_player,
                        is_knockout=pair.is_knockout,
                        parallel_eliminated_share=par_share,
                    )
                    if not result.is_profitable or result.roi_pct < 0.5:
                        continue

                    sport_opps.append({
                        "pair_id": pair.pair_id,
                        "sport": pair.sport,
                        "event": pair.event_name[:40],
                        "match": f"{pair.player_a} vs {pair.player_b}",
                        "tourney_player": pair.tournament_player,
                        "match_price": round(mp * 100, 1),
                        "tourney_price": round(tp * 100, 1),
                        "pa_tourney": round(pa_t * 100, 1),
                        "pb_tourney": round(pb_t * 100, 1),
                        "roi_pct": round(result.roi_pct, 1),
                        "profit": round(result.profit, 2),
                        "size_a": int(result.size_a),
                        "size_b": int(result.size_b),
                        "cost_a": round(result.cost_a, 2),
                        "cost_b": round(result.cost_b, 2),
                        "liq_a": 0, "liq_b": 0,
                        "match_token": mt.token_id_yes,
                        "tourney_token": tt.token_id_yes,
                        "is_knockout": pair.is_knockout,
                        "parallel_share": round(par_share * 100, 1),
                        "scenarios": [{"name": s.name, "exit_b": round(s.exit_b * 100, 1),
                                       "pnl": round(s.total_pnl, 2)} for s in result.scenarios],
                    })
                except Exception as e:
                    log.debug("Analyze pair err: %s", e)

            # 6. Liquidity check — параллельно для top-50 profitable
            sport_opps.sort(key=lambda x: x["roi_pct"], reverse=True)
            top_opps = sport_opps[:50]
            if top_opps:
                liq_tokens = set()
                for o in top_opps:
                    liq_tokens.add(o["match_token"])
                    liq_tokens.add(o["tourney_token"])

                ob_cache = {}
                def _fetch_ob(tid):
                    try:
                        return tid, pm.get_order_book(tid)
                    except Exception:
                        return tid, {}

                with ThreadPoolExecutor(max_workers=10) as executor:
                    for tid, ob in executor.map(lambda t: _fetch_ob(t), liq_tokens):
                        ob_cache[tid] = ob

                def _extract_liq(ob):
                    """Extract liquidity from order book (dict or OrderBookSummary)."""
                    try:
                        asks = getattr(ob, 'asks', None) or (ob.get('asks') if isinstance(ob, dict) else None)
                        if not asks:
                            return 0.0
                        total = 0.0
                        for a in asks[:5]:
                            if isinstance(a, dict):
                                total += float(a.get("size", 0))
                            elif hasattr(a, 'size'):
                                total += float(a.size)
                        return total
                    except Exception:
                        return 0.0

                for opp in top_opps:
                    opp["liq_a"] = round(_extract_liq(ob_cache.get(opp["match_token"], {})), 0)
                    opp["liq_b"] = round(_extract_liq(ob_cache.get(opp["tourney_token"], {})), 0)

            # Фильтр ликвидности
            min_liq = cfg.HEDGE_MIN_LIQUIDITY
            sport_opps = [o for o in sport_opps if o["liq_a"] >= min_liq and o["liq_b"] >= min_liq]

            task["sport_results"][sport] = {
                "pairs": len(pairs), "opps": len(sport_opps),
                "best_roi": round(max((o["roi_pct"] for o in sport_opps), default=0), 1),
            }
            all_opportunities.extend(sport_opps)

        all_opportunities.sort(key=lambda x: x["roi_pct"], reverse=True)
        task["opportunities"] = all_opportunities
        task["total_pairs"] = sum(r["pairs"] for r in task["sport_results"].values())
        task["status"] = "done"
        task["progress_pct"] = 100
        log.info("Analyze done: %d opportunities from %d pairs",
                 len(all_opportunities), task["total_pairs"])

    except Exception as e:
        log.exception("analyze worker error")
        task["status"] = "error"
        task["error"] = str(e)


@app.route("/api/hedge/analyze", methods=["POST"])
def api_hedge_analyze():
    """Запускает фоновый анализ, возвращает task_id мгновенно."""
    try:
        from config import Config
        cfg = Config()
        budget = float((request.json or {}).get("budget", cfg.HEDGE_MAX_BUDGET))
        sports = [s.strip() for s in cfg.HEDGE_SPORTS.split(",") if s.strip()]

        task_id = str(_uuid.uuid4())[:8]
        task = {
            "status": "running",
            "progress_pct": 0,
            "current_sport": "",
            "sport_results": {},
            "opportunities": [],
            "total_pairs": 0,
            "error": None,
            "started": _time.time(),
        }
        with _analyze_lock:
            # Очистка старых задач (>10 мин)
            expired = [k for k, v in _analyze_tasks.items()
                       if _time.time() - v.get("started", 0) > 600]
            for k in expired:
                del _analyze_tasks[k]
            _analyze_tasks[task_id] = task

        t = _threading.Thread(target=_run_analyze_worker,
                              args=(task_id, sports, budget), daemon=True)
        t.start()
        return jsonify(ok=True, task_id=task_id)
    except Exception as e:
        log.exception("api_hedge_analyze")
        return jsonify(ok=False, error=str(e)), 500


@app.route("/api/hedge/analyze-status/<task_id>")
def api_hedge_analyze_status(task_id):
    """Polling endpoint для статуса анализа."""
    with _analyze_lock:
        task = _analyze_tasks.get(task_id)
    if not task:
        return jsonify(ok=False, error="Task not found"), 404
    return jsonify(
        ok=True,
        status=task["status"],
        progress_pct=task["progress_pct"],
        current_sport=task.get("current_sport", ""),
        sport_results=task.get("sport_results", {}),
        opportunities=task.get("opportunities", []) if task["status"] == "done" else [],
        total_pairs=task.get("total_pairs", 0),
        error=task.get("error"),
    )


def _rotate_log(max_mb=10):
    """Обрезать лог до max_mb МБ, оставляя хвост."""
    log_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "valuebet_bot.log")
    try:
        size = os.path.getsize(log_path)
        if size > max_mb * 1024 * 1024:
            keep_bytes = max_mb * 1024 * 1024
            with open(log_path, "rb") as f:
                f.seek(size - keep_bytes)
                tail = f.read()
            # Пропустить обрезанную первую строку
            nl = tail.find(b"\n")
            if nl >= 0:
                tail = tail[nl + 1:]
            with open(log_path, "wb") as f:
                f.write(tail)
            print(f"  [log] Rotated {size/1024/1024:.0f}MB → {len(tail)/1024/1024:.0f}MB")
    except Exception:
        pass

def _kill_old_on_port(port: int):
    """Убить старые процессы на этом порту перед стартом."""
    import subprocess, signal
    try:
        # Windows: netstat → найти PID → убить
        result = subprocess.run(
            ["netstat", "-ano"], capture_output=True, text=True, timeout=5
        )
        my_pid = os.getpid()
        killed = 0
        for line in result.stdout.splitlines():
            if f":{port}" in line and "LISTEN" in line:
                parts = line.split()
                pid = int(parts[-1])
                if pid != my_pid and pid > 0:
                    try:
                        os.kill(pid, signal.SIGTERM)
                        killed += 1
                        print(f"  [cleanup] Killed old process PID={pid} on port {port}")
                    except (ProcessLookupError, PermissionError):
                        pass
        if killed:
            import time; time.sleep(1)
    except Exception:
        pass


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s  %(levelname)-7s  %(message)s")
    port = int(os.getenv("DASHBOARD_PORT", "8080"))
    _rotate_log(10)  # Обрезать лог до 10 МБ
    _kill_old_on_port(port)
    print(f"\n  [*]  Dashboard: http://localhost:{port}\n")
    app.run(host="0.0.0.0", port=port, debug=False, threaded=True)