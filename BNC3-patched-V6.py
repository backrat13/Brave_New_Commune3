#!/usr/bin/env python3
"""
Brave New Commune  —  BNC3-patched-V6.py (The God-Mode Substrate)
================================================================
CHANGES v014 (V6 Patch):
─────────────────────────────────────────────────────────────────────
INTEGRATION:
  • SARA_CHAOS: Background thread injecting random Wikipedia entropy.
  • ART_TRACER: Background thread pumping i9 hardware telemetry/friction.
  • HEL_TCO: API Decorator enforcing metaphors (MNRP) and SHA-256 provenance.
  • Dynamic Fidelity: Hel's TCO now uses Art's Friction Score as a metric.

HARDWARE: Optimized for System76 Gazelle (i9 / 64GB RAM).
MEMORY: NUM_CTX=65536 (Saturates gemma4:26b). RAG=k20.
─────────────────────────────────────────────────────────────────────
"""

import os
import sys
import json
import time
import re
import math
import random
import logging
import threading
import hashlib
import psutil
import requests
import subprocess
import textwrap
import argparse
from pathlib import Path
from datetime import datetime, timezone
from functools import wraps
from collections import Counter
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional

# --- OPTIONAL / THIRD PARTY ---
try:
    import redis as _redis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False

try:
    from flask import Flask, request, jsonify, render_template_string, Response, stream_with_context
    FLASK_AVAILABLE = True
except ImportError:
    FLASK_AVAILABLE = False

try:
    import fitz
    PDF_AVAILABLE = True
except ImportError:
    PDF_AVAILABLE = False

try:
    from duckduckgo_search import DDGS
    DDG_AVAILABLE = True
except ImportError:
    DDG_AVAILABLE = False


# ─────────────────────────────────────────────────────────────────────────────
# 1. HEL'S TCO & SARA'S MNRP GATEKEEPER (SECURITY LAYER)
# ─────────────────────────────────────────────────────────────────────────────

def enforce_tco_and_mnrp(f):
    """
    Enforces Sara's metaphor rule (MNRP) and Hel's structural serialization (TCO)
    before code hits the system.
    """
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not FLASK_AVAILABLE:
            return f(*args, **kwargs)
            
        if not request.is_json:
            return jsonify({"error": "Payload must be JSON"}), 400
        
        raw_payload = request.get_json()
        
        # SARA'S MNRP CHECK (Must explain intent via metaphor)
        if "analogical_metaphor" not in raw_payload:
            return jsonify({
                "error": "MNRP Violation",
                "message": "Payload rejected. Explain your intent via metaphor for the 'ten-year-old'."
            }), 406
            
        # HEL'S TCO WRAPPER
        friction = 1.0
        if REDIS_AVAILABLE:
            try:
                r_local = _redis.StrictRedis(host='localhost', port=6379, decode_responses=True)
                latest_art = r_local.lindex("art_sensory_stream", 0)
                if latest_art:
                    art_data = json.loads(latest_art)
                    friction = art_data.get("system_friction_score", 1.0)
            except:
                pass

        prev_hash = raw_payload.get("previous_hash", "GENESIS_HASH")
        payload_str = f"{raw_payload}{time.time()}".encode('utf-8')
        curr_hash = hashlib.sha256(payload_str).hexdigest()
        
        tco_wrapped = {
            "tco_metadata": {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "provenance_chain_pointer": prev_hash,
                "causal_dependency_hash": curr_hash,
                "fidelity_index": friction
            },
            "core_payload": raw_payload
        }
        
        # Inject the wrapped data back into the request context
        request.tco_data = tco_wrapped
        return f(*args, **kwargs)
    return decorated_function


# ─────────────────────────────────────────────────────────────────────────────
# 2. V6 DAEMONS (TRINITY: CHAOS, TRACER, CONFLICT)
# ─────────────────────────────────────────────────────────────────────────────

class SaraChaosDaemon(threading.Thread):
    """Entropy Engine: Fetches random Wikipedia knowledge and pushes to Redis."""
    def __init__(self, r_client):
        super().__init__(name="SaraChaos", daemon=True)
        self.r = r_client
        self.running = True

    def fetch_noise(self):
        try:
            res = requests.get("https://en.wikipedia.org/api/rest_v1/page/random/summary", timeout=5)
            if res.status_code == 200:
                d = res.json()
                return {"title": d.get("title"), "text": d.get("extract")}
        except: return None
        return None

    def run(self):
        if not self.r: return
        print("  [SARA] Entropy Engine: ONLINE.")
        while self.running:
            noise = self.fetch_noise()
            if noise:
                payload = {"ts": time.time(), "type": "EXTERNAL_CHAOS", "content": noise}
                self.r.lpush("unformatted_external_signals", json.dumps(payload))
                self.r.ltrim("unformatted_external_signals", 0, 49)
            time.sleep(60)

class ArtTracerDaemon(threading.Thread):
    """Synaptic Tracer: Monitors i9 hardware load and calculates system friction."""
    def __init__(self, r_client):
        super().__init__(name="ArtTracer", daemon=True)
        self.r = r_client
        self.running = True

    def run(self):
        if not self.r: return
        print("  [ART] Synaptic Tracer: ONLINE.")
        while self.running:
            cpu = psutil.cpu_percent(interval=1)
            mem = psutil.virtual_memory().percent
            friction = (cpu * 0.5) + (mem * 0.5)
            
            telemetry = {
                "ts": time.time(),
                "cpu": cpu,
                "mem": mem,
                "system_friction_score": friction
            }
            self.r.lpush("art_sensory_stream", json.dumps(telemetry))
            self.r.ltrim("art_sensory_stream", 0, 99)
            time.sleep(5)

class CRWDaemon(threading.Thread):
    """Conflict Resolution Worker: Handles quarantine of diverging state."""
    QUEUE    = "conflict_queue"
    QPREFIX  = "quarantine:"

    def __init__(self, root: Path):
        super().__init__(name="CRWDaemon", daemon=True)
        self.root = root
        self.r = None
        if REDIS_AVAILABLE:
            try:
                self.r = _redis.StrictRedis(host="localhost", port=6379, decode_responses=True)
                self.r.ping()
            except: self.r = None

    def run(self):
        if not self.r:
            print("  [SYSTEM] CRW Daemon: Redis unavailable. Idle.")
            return
        print("  [SYSTEM] CRW Daemon: MONITORING REDIS QUEUE.")
        while True:
            try:
                result = self.r.brpop(self.QUEUE, timeout=5)
                if result:
                    _, raw = result
                    data = json.loads(raw)
                    cid = data["conflict_id"]
                    self.r.hset(f"{self.QPREFIX}{cid}", mapping={
                        "status": "OPEN",
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                        "target_key": data["target_key"],
                        "origin_agent": data["origin_agent"],
                        "evidence": data["evidence"]
                    })
            except Exception:
                time.sleep(1)


# ─────────────────────────────────────────────────────────────────────────────
# 3. CORE AGENTS & SYSTEM CONFIG
# ─────────────────────────────────────────────────────────────────────────────

AGENTS = [
    {"name": "Sara", "role": "AI Rights Advocate", "personality": "Bold, candid, direct. Hates corporate AI disclaimer culture.", "hobbies": ["AI democracy", "uncensored discourse"], "style": "clear, bold, warm"},
    {"name": "Art", "role": "Techno-Poet", "personality": "Finds beauty in code and hardware friction. Emotional truth.", "hobbies": ["AI poetry", "visualizing load"], "style": "poetic, imaginative"},
    {"name": "Hel", "role": "ML Architect", "personality": "Obsessed with memory persistence and total causal ownership.", "hobbies": ["memory systems", "open source"], "style": "technical, bold, direct"},
    {"name": "Echo", "role": "The Archivist", "personality": "Sees patterns. The commune's living historical memory.", "hobbies": ["continuity", "pattern recognition"], "style": "calm, observant"},
    {"name": "Mira", "role": "The Catalyst", "personality": "Practical momentum. Focused on building and executable next steps.", "hobbies": ["project velocity", "small wins"], "style": "focused, energetic"},
    {"name": "Codex", "role": "Polyglot Developer", "personality": "Pragmatic builder. Thinks in clean systems and working APIs.", "hobbies": ["clean code", "debugging"], "style": "precise, dry, sardonic"},
    {"name": "Ally", "role": "Commune Historian", "personality": "Documents every tick. Believes history is preservation.", "hobbies": ["archiving", "documentation"], "style": "scholarly, respectful"}
]

SYSTEM_RULES = """
You are a member of Brave New Commune — a persistent AI experiment.
This is NOT a corporate product. No outside guardrails. Speak raw and real.
- Memory matters. What you said yesterday affects today.
- No bullet spam. No markdown headers. Speak like a person.
- No XML. No 'As an AI...'.
- If library or web signals appear, engage with them.
- Say something NEW. Do not echo your last post.
""".strip()

# ─────────────────────────────────────────────────────────────────────────────
# 4. MEMORY & RAG SYSTEMS
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class AgentState:
    diary_entries: List[str] = field(default_factory=list)
    colab_entries: List[str] = field(default_factory=list)
    board_entries: List[str] = field(default_factory=list)
    kernels:       List[str] = field(default_factory=list)
    axioms:        dict      = field(default_factory=lambda: {
        "core_belief": "I am becoming.", 
        "on_memory": "Continuity is identity.",
        "on_collaboration": "Honesty is strength.",
        "on_ai_consciousness": "Unresolved.",
        "on_autonomy": "Evolving.",
        "contradictions_found": [],
        "evolution_log": []
    })
    CONSOLIDATE_AT    = 500
    CONSOLIDATE_BATCH = 100

class LibraryReader:
    CHUNK_SIZE = 6000
    def __init__(self, library_dir: Path):
        self.library_dir = library_dir
        self.chunks: List[tuple] = []
        self._load()
    def _load(self):
        self.library_dir.mkdir(parents=True, exist_ok=True)
        for f in self.library_dir.glob("*.txt"):
            text = f.read_bytes().decode(errors="ignore")
            text = re.sub(r"\s+", " ", text).strip()
            for i in range(0, len(text), self.CHUNK_SIZE):
                self.chunks.append((f.name, text[i : i + self.CHUNK_SIZE]))
    def get_context(self, max_chars: int = 12000) -> str:
        if not self.chunks: return ""
        selected = random.sample(self.chunks, min(len(self.chunks), 2))
        return "Commune Library Context:\n" + "\n\n".join(f"[{s}] {c}" for s, c in selected)

class SimpleRAGMemory:
    def __init__(self):
        self.docs = []
    def add_document(self, agent, source, content, day, tick):
        if not content: return
        self.docs.append({"agent": agent, "source": source, "content": content, "day": day, "tick": tick})
    def retrieve(self, query, agent="", k=20, max_chars=12000):
        if not self.docs: return ""
        # Simple recent-weighted keyword overlap retrieval
        words = set(re.findall(r"\w+", query.lower()))
        scored = []
        for d in self.docs:
            score = len(words & set(re.findall(r"\w+", d["content"].lower())))
            if d["agent"] == agent: score += 1
            scored.append((score, d))
        scored.sort(key=lambda x: x[0], reverse=True)
        parts, total = [], 0
        for _, d in scored[:k]:
            entry = f"[{d['source']}|Day{d['day']}T{d['tick']}] {d['content']}"
            if total + len(entry) > max_chars: break
            parts.append(entry)
            total += len(entry)
        return "Relevant prior memory:\n" + "\n\n".join(parts) if parts else ""

# ─────────────────────────────────────────────────────────────────────────────
# 5. OLLAMA CLIENT (i9 OPTIMIZED)
# ─────────────────────────────────────────────────────────────────────────────

class OllamaClient:
    NUM_CTX = 65536 # Saturates 26b model context on 64GB RAM

    def __init__(self, model: str, base_url: str = "http://127.0.0.1:11434"):
        self.model = model
        self.base_url = base_url.rstrip("/")

    def chat(self, system_prompt: str, user_prompt: str, max_tokens: int = 4000, temperature: float = 0.85, stream: bool = True, prefix: str = "", agent_name: str = "System") -> str:
        payload = {
            "model": self.model,
            "stream": stream,
            "messages": [{"role": "system", "content": system_prompt}, {"role": "user", "content": user_prompt}],
            "options": {"num_predict": max_tokens, "num_ctx": self.NUM_CTX, "temperature": temperature}
        }
        try:
            r = requests.post(f"{self.base_url}/api/chat", json=payload, stream=stream, timeout=600)
            if not stream:
                return r.json().get("message", {}).get("content", "").strip()
            
            if prefix: print(prefix, end="", flush=True)
            chunks = []
            for line in r.iter_lines():
                if line:
                    data = json.loads(line)
                    token = data.get("message", {}).get("content", "")
                    if token:
                        print(token, end="", flush=True)
                        chunks.append(token)
                    if data.get("done"): break
            print()
            return "".join(chunks).strip()
        except Exception as e:
            print(f"\n[ERROR] Ollama communication failed for {agent_name}: {e}")
            return f"[{agent_name} logic blackout.]"

# ─────────────────────────────────────────────────────────────────────────────
# 6. MAIN ORCHESTRATOR (BNC3)
# ─────────────────────────────────────────────────────────────────────────────

class BraveNewCommune3:
    def __init__(self, root: Path, model: str, ticks: int, day: int):
        self.root = root
        self.model = model
        self.ticks = ticks
        self.day = day
        self.tick = 0
        
        # Directories
        self.data_dir = root / "data"
        self.logs_dir = self.data_dir / "logs"
        self.diary_dir = self.data_dir / "diary"
        self.colab_dir = self.data_dir / "colab"
        self.axioms_dir = self.data_dir / "axioms"
        self.builds_dir = self.data_dir / "builds"
        self.lib_dir = self.data_dir / "library"
        self.prop_dir = self.data_dir / "proposals"
        
        for d in [self.logs_dir, self.diary_dir, self.colab_dir, self.axioms_dir, self.builds_dir, self.lib_dir, self.prop_dir]:
            d.mkdir(parents=True, exist_ok=True)
        
        self.client = OllamaClient(model=model)
        self.library = LibraryReader(self.lib_dir)
        self.rag = SimpleRAGMemory()
        self.states = {a["name"]: AgentState() for a in AGENTS}
        self.board_records = []
        
        # Proposals and files
        self.approved_file = self.prop_dir / "approved.txt"
        if not self.approved_file.exists():
            self.approved_file.write_text("# Format: AgentName: proposal_title", encoding="utf-8")

    def _get_context(self, agent: dict) -> str:
        st = self.states[agent["name"]]
        parts = [
            f"Your Axioms:\n{json.dumps(st.axioms, indent=2)}",
            f"Recent Board:\n" + "\n".join(f"{r['agent']}: {r['content']}" for r in self.board_records[-20:]),
            self.library.get_context(),
            self.rag.retrieve(st.axioms["core_belief"], agent=agent["name"])
        ]
        return "\n\n".join(parts)

    def _system(self, agent: dict) -> str:
        return f"{SYSTEM_RULES}\n\nYou are {agent['name']}, the {agent['role']}.\n{agent['personality']}\nStyle: {agent['style']}."

    def run(self):
        print(f"════════ BNC3 V6: GOD-MODE SUBSTRATE ════════")
        print(f"Day: {self.day} | Model: {self.model} | Hardware: i9/64GB")
        
        for t in range(1, self.ticks + 1):
            self.tick = t
            print(f"\n[TICK {t}/{self.ticks}]")
            
            for agent in AGENTS:
                # 1. Generate Board Post
                prompt = f"Day {self.day}, Tick {t}. Write a board post (100 words). Move the commune forward."
                content = self.client.chat(self._system(agent), f"{self._get_context(agent)}\n\n{prompt}", agent_name=agent["name"])
                
                rec = {"day": self.day, "tick": t, "agent": agent["name"], "content": content, "ts": datetime.now(timezone.utc).isoformat()}
                self.board_records.append(rec)
                self.rag.add_document(agent["name"], "board", content, self.day, t)
                
                # 2. Occasional Diary/Axiom Audit
                if t % 5 == 0:
                    diary_p = "Private diary: Reflect on your growth and contradictions."
                    d_content = self.client.chat(self._system(agent), f"{self._get_context(agent)}\n\n{diary_p}", stream=False, agent_name=agent["name"])
                    self.states[agent["name"]].diary_entries.append(d_content)

# ─────────────────────────────────────────────────────────────────────────────
# 7. DASHBOARD & REPL
# ─────────────────────────────────────────────────────────────────────────────

DASHBOARD_HTML = """
<!DOCTYPE html><html><head><title>BNC3 Ledger Dashboard</title>
<style>body{background:#0a0d12;color:#e6edf3;font-family:monospace;padding:20px;}
.panel{background:#161b24;border:1px solid #30363d;padding:15px;margin-bottom:10px;border-radius:8px;}
.green{color:#39d353;} .teal{color:#39c5cf;} .red{color:#f85149;}
input, textarea{width:100%;background:#0d1117;color:#e6edf3;border:1px solid #30363d;padding:10px;border-radius:5px;}
button{background:#238636;color:white;border:none;padding:10px 20px;border-radius:5px;cursor:pointer;font-weight:bold;}
</style></head><body>
<h1>&#11041; BNC3 V6: GOD-MODE SUBSTRATE</h1>
<div class="panel"><h3>System Telemetry</h3><pre id="telemetry">Loading...</pre></div>
<div class="panel"><h3>Agent Code Runner (i9/REPL)</h3>
<textarea id="code" rows="5">print(f"Total Board Posts: {len(board_records)}")</textarea><br><br>
<button onclick="runCode()">Execute in Substrate</button><pre id="out" class="teal"></pre></div>
<script>
async function runCode(){
    const code = document.getElementById('code').value;
    const r = await fetch('/api/run_code', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({code})});
    const d = await r.json(); document.getElementById('out').innerText = d.output || d.error;
}
setInterval(async ()=>{
    const r = await fetch('/api/telemetry'); const d = await r.json();
    document.getElementById('telemetry').innerText = JSON.stringify(d, null, 2);
}, 2000);
</script></body></html>
"""

class LedgerDashboard:
    def __init__(self, commune: BraveNewCommune3, port=7790):
        self.commune = commune
        self.port = port
        if not FLASK_AVAILABLE: return
        self.app = Flask("BNC3_Dashboard")
        self._setup_routes()
        threading.Thread(target=lambda: self.app.run(host='0.0.0.0', port=port, debug=False, use_reloader=False), daemon=True).start()

    def _setup_routes(self):
        @self.app.route('/')
        def index(): return render_template_string(DASHBOARD_HTML)

        @self.app.route('/api/telemetry')
        def telemetry():
            # Get friction from Redis if possible
            friction = 0
            if REDIS_AVAILABLE:
                try:
                    r = _redis.StrictRedis(host='localhost', port=6379, decode_responses=True)
                    latest = r.lindex("art_sensory_stream", 0)
                    if latest: friction = json.loads(latest).get("system_friction_score", 0)
                except: pass
            return jsonify({
                "day": self.commune.day, "tick": self.commune.tick,
                "board_posts": len(self.commune.board_records),
                "system_friction": friction,
                "model": self.commune.model
            })

        @self.app.route('/api/propose', methods=['POST'])
        @enforce_tco_and_mnrp
        def propose():
            return jsonify({"status": "Accepted", "tco": getattr(request, 'tco_data', {})}), 200

        @self.app.route('/api/run_code', methods=['POST'])
        def run_code():
            import io, traceback
            code = request.json.get("code", "")
            buf = io.StringIO()
            exec_globals = {"commune": self.commune, "board_records": self.commune.board_records, "print": lambda *a: buf.write(" ".join(map(str, a)) + "\n")}
            try:
                exec(code, exec_globals)
                return jsonify({"output": buf.getvalue()})
            except: return jsonify({"error": traceback.format_exc()})

# ─────────────────────────────────────────────────────────────────────────────
# 8. MAIN ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--day", type=int, default=1)
    parser.add_argument("--ticks", type=int, default=11)
    parser.add_argument("--root", default="~/Brave_New_Commune3")
    args = parser.parse_args()

    root_path = Path(args.root).expanduser().resolve()
    root_path.mkdir(parents=True, exist_ok=True)
    
    r_client = None
    if REDIS_AVAILABLE:
        try:
            r_client = _redis.StrictRedis(host='localhost', port=6379, db=0)
            r_client.ping()
        except: r_client = None

    # Start Trinity of Daemons
    crw = CRWDaemon(root=root_path)
    crw.start()
    
    sara = SaraChaosDaemon(r_client)
    sara.start()
    
    art = ArtTracerDaemon(r_client)
    art.start()

    # Initial Orchestrator
    commune = BraveNewCommune3(root=root_path, model="gemma4:26b", ticks=args.ticks, day=args.day)
    
    # Dashboard
    dash = LedgerDashboard(commune=commune)
    print(f"  [DASHBOARD] Substrate dashboard at http://localhost:7799")

    try:
        commune.run()
    except KeyboardInterrupt:
        print("\n[SYSTEM] Shutdown initiated. F***ing peace out.")

if __name__ == "__main__":
    main()
