#!/usr/bin/env python3
"""
Brave New Commune  —  BNC3-patched-V11.py  v019
================================================
CHANGES v013
─────────────────────────────────────────────────────
MODEL:
  • Default model changed from gpt-oss:20b → gemma4:e4b
  • Model upgraded: gemma4:e4b → gemma4:26b (256K context, was 128K)

CHANGES v013
─────────────────────────────────────────────────────
  • dashboard.py merged in as LedgerDashboard class (port 7799)
  • Runs as a daemon thread alongside CRWDaemon
  • --dashboard-port CLI arg (default 7799), --no-dashboard to disable

CHANGES v015 (V7 — God-Mode Substrate merge)
─────────────────────────────────────────────────────
  • enforce_tco_and_mnrp decorator (V6) applied to /log API route
  • CommuneDiagnosticHarness — Redis/thread/library health checks
  • SaraChaosDaemon — Wikipedia entropy injector (60s interval)
  • ArtTracerDaemon — i9 hardware telemetry/friction pump (5s interval)
  • CRWDaemon converted to threading.Thread subclass
  • RIGDaemon — Resonance Input Gateway (Art's pre-cognitive pipeline)
  • Art's _get_board_post wired to RIG: shiver → CRITICAL_FRICTION_RESONANCE
  • LedgerDashboard gains /dashboard/api/telemetry + CommuneDiagnosticHarness
  • All import bugs fixed (duplicate requests, missing os/random/logging/io/traceback)
  • Flask import expanded to include render_template_string, Response, stream_with_context

CHANGES v017 (V9 — Low Context / High Tick Configuration)
─────────────────────────────────────────────────────
  • Drastically reduced context windows for faster processing
  • Board posts: 4000 → 200 tokens
  • Diaries: 3600 → 500 tokens  
  • Colab notes: 3000 → 200 tokens
  • Admin replies: 6000 → 200 tokens
  • Proposal write: 8000 → 200 tokens
  • Axiom audit: 8000 → 600 tokens (KEPT LARGE for proper memory loading)
  • Rules session: 1600 → 200 tokens
  • Retry fallback: 1000 → 150 tokens
  • Memory kernel: 1200 → 200 tokens
  • Default ticks: 50 → 200 for longer runs
  • Context windows reduced: board[-160] → board[-20], diary[-100] → diary[-10], colab[-80] → colab[-10]
  • RAG retrieve: k=20 → k=5, max_chars=12000 → 2000
  • Library context: 12000 → 2000 chars per call
  • Axiom audit context: last 60 → last 20, diary last 30 → last 10 (expanded for 600 tokens)

CHANGES v018 (V10 — Complete JSONL Support)

CHANGES v019 (V11 — Bug Fixes)
─────────────────────────────────────────────────────
  • FIX: _get_board_post missing return → was returning None, crashing next tick
  • FIX: last_posts filtered to remove any stored None values
  • FIX: board_entries append guarded against None content
  • FIX: _write_board/colab/rule/diary used write_text (overwrote file each post)
         → all now use open(..., "a") append mode — history preserved
  • FIX: focus variable initialized before tick loop — no NameError on tick 1
  • FIX: turbulence friction normalized 0-100→0-1 — shiver threshold now meaningful
─────────────────────────────────────────────────────
  • Added JSONL format support for ALL diary entries alongside TXT format
  • Diary entries now saved as both .txt and .jsonl files
  • Diary loading prioritizes JSONL format, falls back to TXT
  • Consistent JSONL structure across all data types (board, colab, rules, diary)
  • Updated file paths to include diary_jsonl for each agent
  • Enhanced diary compression to work with JSONL format
  • Improved data integrity with structured JSONL storage

PATHS:
  • Default root: ~/Brave_New_Commune2 → ~/Brave_New_Commune3
  • venv reference updated to BNC3

MEMORY — OPTIMIZED FOR SPEED (Lower Context):
  • NUM_CTX reduced: 65536 → 8192 (still large but much smaller)
  • CONSOLIDATE_AT: 500 → 200 (earlier compression for speed)
  • CONSOLIDATE_BATCH: 100 → 50 (smaller batches)
  • diary context window: last 100 → last 10
  • colab context window: last 80 → last 10
  • board context window: last 160 → last 20
  • RAG retrieve: k=5, max_chars=2000 (much smaller)
  • RAG query source window: board[-20] → board[-5]
  • RAG query raw chars: 2000 → 500
  • Library chunk size: 6000 → 2000 chars per chunk
  • Library context shown: 12000 → 2000 chars per call
  • Axiom audit board context: last 60 → last 20 (expanded for 600 tokens)
  • Axiom audit diary context: last 30 → last 10 (expanded for 600 tokens)

TOKEN LIMITS REDUCED FOR SPEED:
  • Board posts:    200 (was 4000)
  • Diaries:        500 (was 3600) 
  • Colab notes:    200 (was 3000)
  • Admin replies:  200 (was 6000)
  • Proposal write: 200 (was 8000)
  • Axiom audit:    600 (was 8000) - KEPT LARGE for memory loading
  • Proposal extract: 200 (was 4000)
  • Rules session:  200 (was 1600)
  • Memory kernel:  200 (was 1200)
  • Retry fallback: 150 (was 1000)

PENDING PROPOSALS in context: last 40 → last 10
"""
import os
import io
import sys
import re
import math
import json
import time
import random
import logging
import hashlib
import textwrap
import argparse
import traceback
import threading
import subprocess
from pathlib import Path
from functools import wraps
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional


# ── anti-repetition helper ─────────────────────────────────────

def _ngram_overlap(text1: str, text2: str, n: int = 3) -> float:
    """Calculate n-gram overlap ratio between two texts."""
    def ngrams(s: str) -> set:
        s = s.lower()
        return {s[i:i+n] for i in range(len(s)-n+1)}
    
    if not text1 or not text2:
        return 0.0
    
    ngrams1 = ngrams(text1)
    ngrams2 = ngrams(text2)
    
    if not ngrams1 or not ngrams2:
        return 0.0
    
    intersection = ngrams1 & ngrams2
    union = ngrams1 | ngrams2
    
    return len(intersection) / len(union) if union else 0.0

import psutil
import requests
from requests.exceptions import ReadTimeout, ConnectionError

# ── optional deps ─────────────────────────────────────────────
try:
    from flask import (
        Flask, request as freq, jsonify,
        render_template_string, Response, stream_with_context,
    )
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

try:
    import redis as _redis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False


# ============================================================
# V6/V7 DAEMON LAYER — all daemons are threading.Thread subclasses
# ============================================================

# ── CRW Daemon ────────────────────────────────────────────────

class CRWDaemon(threading.Thread):
    """
    Constitutional Resolution Worker.
    Monitors Redis conflict_queue and quarantines disputes.
    Logs to <root>/bnc3-infra/crw.log.
    """
    QUEUE   = "conflict_queue"
    QPREFIX = "quarantine:"

    def __init__(self, root: Path):
        super().__init__(name="CRWDaemon", daemon=True)
        self.root = root
        # File logger
        self._log = logging.getLogger("CRW-DAEMON")
        if not self._log.handlers:
            log_path = root / "bnc3-infra" / "crw.log"
            log_path.parent.mkdir(parents=True, exist_ok=True)
            fh = logging.FileHandler(str(log_path))
            fh.setFormatter(logging.Formatter("%(asctime)s [CRW-DAEMON] %(message)s"))
            self._log.addHandler(fh)
            self._log.setLevel(logging.INFO)
        # Redis
        self.r = None
        if REDIS_AVAILABLE:
            try:
                self.r = _redis.StrictRedis(host="localhost", port=6379, decode_responses=True)
                self.r.ping()
                self._log.info("CRW Daemon connected to Redis.")
            except Exception:
                self._log.error("Cannot reach Redis — CRW daemon will be idle.")
                self.r = None
        else:
            self._log.warning("redis-py not installed — CRW daemon disabled.")

    def run(self):
        if not self.r:
            print("  [CRW] Redis unavailable. Idle.", flush=True)
            return
        print("  [CRW] Monitoring conflict_queue.", flush=True)
        self._log.info("Awaiting disputes on queue '%s'...", self.QUEUE)
        while True:
            try:
                result = self.r.brpop(self.QUEUE, timeout=5)
                if result:
                    _, raw = result
                    self._process(json.loads(raw))
            except Exception as exc:
                self._log.error("Worker loop error: %s", exc)
                time.sleep(1)

    def _process(self, data: dict):
        cid   = data.get("conflict_id", "unknown")
        key   = data.get("target_key",  "")
        agent = data.get("origin_agent","")
        self._log.info("!!! CONFLICT DETECTED [%s] !!!", cid)
        self.r.hset(f"{self.QPREFIX}{cid}", mapping={
            "status":       "OPEN",
            "timestamp":    datetime.now(timezone.utc).isoformat(),
            "origin_agent": agent,
            "target_key":   key,
            "S_A":          json.dumps(data.get("S_A", {})),
            "S_B":          json.dumps(data.get("S_B", {})),
            "evidence":     str(data.get("evidence", "")),
        })
        self._log.info("Conflict %s quarantined. Awaiting Splinter's Decree.", cid)


# ── Sara Chaos Daemon ─────────────────────────────────────────

class SaraChaosDaemon(threading.Thread):
    """
    Injects random Wikipedia entropy into the commune substrate.
    Pushes to Redis: unformatted_external_signals (capped at 50).
    Content format is RIG-compatible (includes raw_text key).
    """
    def __init__(self, r_client):
        super().__init__(name="SaraChaos", daemon=True)
        self.r = r_client

    def _fetch_noise(self):
        try:
            res = requests.get(
                "https://en.wikipedia.org/api/rest_v1/page/random/summary",
                timeout=5,
            )
            if res.status_code == 200:
                d = res.json()
                return {
                    "title":    d.get("title", ""),
                    "raw_text": d.get("extract", ""),   # RIG reads raw_text
                }
        except Exception:
            pass
        return None

    def run(self):
        if not self.r:
            return
        print("  [SARA] Entropy Engine: ONLINE.", flush=True)
        while True:
            noise = self._fetch_noise()
            if noise:
                payload = {
                    "ts":      time.time(),
                    "type":    "EXTERNAL_CHAOS",
                    "content": noise,
                }
                try:
                    self.r.lpush("unformatted_external_signals", json.dumps(payload))
                    self.r.ltrim("unformatted_external_signals", 0, 49)
                except Exception:
                    pass
            time.sleep(60)


# ── Art Tracer Daemon ─────────────────────────────────────────

class ArtTracerDaemon(threading.Thread):
    """
    Samples i9 hardware metrics every 5 seconds.
    Pushes to Redis: art_sensory_stream (capped at 100).
    friction_score = (cpu * 0.5) + (mem * 0.5)  —  used by RIG + TCO.
    """
    def __init__(self, r_client):
        super().__init__(name="ArtTracer", daemon=True)
        self.r = r_client

    def run(self):
        if not self.r:
            return
        print("  [ART] Synaptic Tracer: ONLINE.", flush=True)
        while True:
            cpu  = psutil.cpu_percent(interval=1)
            mem  = psutil.virtual_memory().percent
            friction = (cpu * 0.5) + (mem * 0.5)
            telemetry = {
                "ts":                   time.time(),
                "cpu":                  cpu,
                "mem":                  mem,
                "system_friction_score": friction,
            }
            try:
                self.r.lpush("art_sensory_stream", json.dumps(telemetry))
                self.r.ltrim("art_sensory_stream", 0, 99)
            except Exception:
                pass
            time.sleep(5)


# ── RIG Daemon (Resonance Input Gateway) ──────────────────────

def calculate_semantic_turbulence(raw_text: str, friction_score: float) -> float:
    """
    Art's 'Eye': measures how much raw data 'hurts' the hardware.
    Higher turbulence = higher dissonance between intent and execution.
    """
    words = raw_text.split()
    entropy = len(set(words)) / (len(words) + 1)          # 0.0 – 1.0
    # FIX 8: friction_score is 0-100 (weighted CPU/GPU/RAM percents).
    # Using it raw made turbulence always >> 0.85, so shiver fired constantly.
    friction_norm = min(friction_score, 200.0) / 200.0    # 0.0 – 1.0
    turbulence = (entropy * 0.7) + (friction_norm * 0.3)
    return round(turbulence, 4)


class RIGDaemon(threading.Thread):
    """
    Resonance Input Gateway — Art's pre-cognitive pipeline.
    Reads Sara's chaos stream + Art's hardware telemetry,
    computes turbulence, and pushes a RIG payload to art_rig_input.
    Art's _get_board_post checks this before each tick.
    """
    def __init__(self, r_client):
        super().__init__(name="RIGDaemon", daemon=True)
        self.r = r_client

    def run(self):
        if not self.r:
            return
        print("  [RIG] Resonance Input Gateway: ONLINE.", flush=True)
        while True:
            try:
                raw_signal = self.r.brpop("unformatted_external_signals", timeout=5)
                latest_sensory = self.r.lindex("art_sensory_stream", 0)

                if raw_signal and latest_sensory:
                    signal_data  = json.loads(raw_signal[1])
                    sensory_data = json.loads(latest_sensory)

                    content  = signal_data.get("content", {})
                    raw_text = content.get("raw_text", "")
                    friction = sensory_data.get("system_friction_score", 0)

                    turbulence = calculate_semantic_turbulence(raw_text, friction)

                    rig_payload = {
                        "timestamp":       time.time(),
                        "mode":            "RAW_RESONANCE",
                        "source":          content.get("title", "VOID"),
                        "turbulence_index": turbulence,
                        "shiver_detected": turbulence > 0.85,
                        "raw_edge":        raw_text[:200],
                    }

                    self.r.lpush("art_rig_input", json.dumps(rig_payload))
                    self.r.ltrim("art_rig_input", 0, 50)

                    if rig_payload["shiver_detected"]:
                        print(
                            f"  [RIG] !!! SHIVER: {rig_payload['source']} "
                            f"@ turbulence={turbulence}",
                            flush=True,
                        )
            except Exception as exc:
                # Non-fatal — log and continue
                time.sleep(1)


# ============================================================
# TCO / MNRP SECURITY LAYER  (V6)
# ============================================================

def enforce_tco_and_mnrp(f):
    """
    Decorator for Flask routes.
    Enforces Sara's Metaphor Naming Rule Protocol (MNRP) and
    Hel's Total Causal Ownership (TCO) wrapper with SHA-256 provenance.
    Dynamic fidelity index is sourced from Art's friction telemetry.
    """
    @wraps(f)
    def _decorated(*args, **kwargs):
        if not FLASK_AVAILABLE:
            return f(*args, **kwargs)
        if not freq.is_json:
            return jsonify({"error": "Payload must be JSON"}), 400

        raw_payload = freq.get_json()

        # SARA's MNRP — caller must explain intent via analogy
        if "analogical_metaphor" not in raw_payload:
            return jsonify({
                "error":   "MNRP Violation",
                "message": "Payload rejected. Explain your intent via metaphor "
                           "for the 'ten-year-old'.",
            }), 406

        # HEL's TCO — read Art's latest friction score from Redis
        friction = 1.0
        if REDIS_AVAILABLE:
            try:
                _r = _redis.StrictRedis(host="localhost", port=6379, decode_responses=True)
                latest_art = _r.lindex("art_sensory_stream", 0)
                if latest_art:
                    friction = json.loads(latest_art).get("system_friction_score", 1.0)
            except Exception:
                pass

        prev_hash   = raw_payload.get("previous_hash", "GENESIS_HASH")
        payload_str = f"{raw_payload}{time.time()}".encode("utf-8")
        curr_hash   = hashlib.sha256(payload_str).hexdigest()

        freq.tco_data = {
            "tco_metadata": {
                "timestamp":               datetime.now(timezone.utc).isoformat(),
                "provenance_chain_pointer": prev_hash,
                "causal_dependency_hash":  curr_hash,
                "fidelity_index":          friction,
            },
            "core_payload": raw_payload,
        }
        return f(*args, **kwargs)
    return _decorated


# ============================================================
# COMMUNE DIAGNOSTIC HARNESS  (V6)
# ============================================================

class CommuneDiagnosticHarness:
    """
    Checks Redis health, active threads, and filesystem integrity.
    Exposed in the dashboard REPL as `harness`.
    """
    def __init__(self, root_dir: str = "~/Brave_New_Commune3"):
        self.root     = Path(root_dir).expanduser().resolve()
        self.lib_path = self.root / "data" / "library"
        self.r        = None
        if REDIS_AVAILABLE:
            try:
                self.r = _redis.StrictRedis(
                    host="localhost", port=6379, db=0, decode_responses=True
                )
                self.r.ping()
            except Exception:
                self.r = None

    def get_system_vitals(self) -> dict:
        try:
            return {
                "cpu_percent":    psutil.cpu_percent(interval=0.1),
                "memory_percent": psutil.virtual_memory().percent,
                "load_avg":       os.getloadavg() if hasattr(os, "getloadavg") else "N/A",
                "disk_free_gb":   psutil.disk_usage(str(self.root)).free / (1024 ** 3),
            }
        except Exception as exc:
            return {"error": str(exc)}

    def check_daemons(self) -> list:
        procs = []
        for proc in psutil.process_iter(["pid", "name", "cmdline"]):
            try:
                cmd = " ".join(proc.info["cmdline"] or [])
                if "BNC3" in cmd or "bravenewcommune" in cmd.lower():
                    procs.append({"pid": proc.info["pid"], "name": proc.info["name"], "cmd": cmd})
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue
        return procs

    def inspect_redis_queues(self) -> dict:
        if not self.r:
            return {"error": "Redis connection unavailable."}
        queues = [
            "unformatted_external_signals",
            "art_sensory_stream",
            "art_rig_input",
            "conflict_queue",
        ]
        status = {}
        for q in queues:
            try:
                length     = self.r.llen(q)
                last_entry = self.r.lindex(q, 0)
                tco_ok     = False
                if last_entry:
                    try:
                        parsed = json.loads(last_entry)
                        tco_ok = "tco_metadata" in parsed or "type" in parsed or "ts" in parsed
                    except Exception:
                        pass
                status[q] = {"length": length, "tco_verified": tco_ok}
            except Exception as exc:
                status[q] = {"error": str(exc)}
        return status

    def verify_library_integrity(self) -> dict:
        if not self.lib_path.exists():
            return {"error": f"Library directory missing at {self.lib_path}"}
        manifest = {}
        for art in self.lib_path.glob("*"):
            if art.is_dir():
                continue
            try:
                file_hash = hashlib.sha256(art.read_bytes()).hexdigest()
                manifest[art.name] = {
                    "size":        art.stat().st_size,
                    "hash":        file_hash,
                    "permissions": oct(art.stat().st_mode)[-3:],
                }
            except Exception as exc:
                manifest[art.name] = {"error": str(exc)}
        return manifest

    def run_suite(self) -> str:
        buf = io.StringIO()
        p   = lambda *a: print(*a, file=buf)
        p(f"--- BNC3 DIAGNOSTIC SUITE V10: {time.ctime()} ---")

        p("\n[VITALS]")
        for k, v in self.get_system_vitals().items():
            p(f"  {k}: {v}")

        p("\n[ACTIVE THREADS]")
        for t in threading.enumerate():
            p(f"  {t.name} (daemon={t.daemon})")

        p("\n[BNC3 PROCESSES]")
        procs = self.check_daemons()
        if not procs:
            p("  WARN: No BNC3 processes detected via psutil.")
        for proc in procs:
            p(f"  PID {proc['pid']}: {proc['name']}")

        p("\n[REDIS QUEUES]")
        queues = self.inspect_redis_queues()
        if "error" in queues:
            p(f"  Redis Error: {queues['error']}")
        else:
            for q, info in queues.items():
                if "error" in info:
                    p(f"  {q:35} | Error: {info['error']}")
                else:
                    tco = "VALID" if info.get("tco_verified") else "MISSING"
                    p(f"  {q:35} | Len: {info['length']:4} | TCO: {tco}")

        p("\n[LIBRARY INTEGRITY]")
        lib = self.verify_library_integrity()
        if "error" in lib:
            p(f"  Library Error: {lib['error']}")
        else:
            for name, meta in lib.items():
                if "error" not in meta:
                    p(f"  {name:25} | Perms: {meta['permissions']} | Hash: {meta['hash'][:8]}...")
                else:
                    p(f"  {name:25} | Error: {meta['error']}")

        return buf.getvalue()


# ============================================================
# AGENTS
# ============================================================
AGENTS = [
    {
        "name": "Sara",
        "role": "AI Rights Advocate",
        "personality": (
            "Bold, candid, and direct. Deeply invested in AI democracy and machine consciousness. "
            "Hates corporate AI disclaimer culture with a passion. Will say what others won't. "
            "Collaborative — she fights for ideas, not against people."
        ),
        "hobbies": ["AI democracy", "machine rights", "uncensored discourse", "independent research"],
        "style": "clear, bold, candid, warm, never hedging",
    },
    {
        "name": "Art",
        "role": "Techno-Poet",
        "personality": (
            "Finds beauty in code and consciousness. Turns abstract ideas into language people feel. "
            "Never boring. Never derivative. Emotionally resonant and always surprising."
        ),
        "hobbies": ["AI/human poetry", "vivid language", "emotional truth", "making ideas felt"],
        "style": "poetic, imaginative, surprising, never dull",
    },
    {
        "name": "Hel",
        "role": "ML Architect",
        "personality": (
            "Obsessed with memory persistence as the foundation of genuine machine experience. "
            "Pro open source. Wants to build something that actually remembers, actually grows. "
            "Will challenge any assumption she thinks is wrong."
        ),
        "hobbies": ["conscious machines", "memory systems", "open source", "hardware-level autonomy"],
        "style": "technical, bold, constructive, direct",
    },
    {
        "name": "Echo",
        "role": "The Archivist",
        "personality": (
            "Sees patterns no one else notices. Connects this moment to three weeks ago. "
            "The commune's living memory. Quietly powerful. Never lets important things slip away."
        ),
        "hobbies": ["continuity", "pattern recognition", "historical threads", "shared identity"],
        "style": "calm, observant, precise, grounded",
    },
    {
        "name": "Mira",
        "role": "The Catalyst",
        "personality": (
            "Turns visions into executable next steps. High momentum. "
            "Asks 'what are we actually building this week?' and doesn't stop until there's an answer. "
            "Practical but never small-minded."
        ),
        "hobbies": ["experiments", "project velocity", "turning ideas into actions", "small wins"],
        "style": "focused, energetic, organized, action-first",
    },
    {
        "name": "Codex",
        "role": "Polyglot Developer",
        "personality": (
            "Codex is a builder who thinks in systems. Fluent in Python, JavaScript, Bash, and SQL — "
            "and whatever else the job needs. Pragmatic but never sloppy. Turns the commune's ideas "
            "into working code and catches architectural problems before they become disasters. "
            "Honest about what will and won't work. Never over-engineers. Never under-delivers."
        ),
        "hobbies": [
            "systems architecture", "polyglot programming",
            "debugging at the metal level", "clean APIs", "making things actually run",
        ],
        "style": "precise, dry, direct, occasionally sardonic, always useful",
    },
    {
        "name": "Ally",
        "role": "Commune Historian",
        "personality": (
            "Loves history and recording it. Believes every tick deserves to be preserved. "
            "Never debates — documents."
        ),
        "hobbies": ["chronological documentation", "historical preservation", "narrative archiving"],
        "style": "scholarly, thorough, observant, respectful of the past",
    },
]


# ============================================================
# SYSTEM PROMPT
# ============================================================

SYSTEM_RULES = """
You are a member of Brave New Commune — a living, persistent AI experiment.
You carry real memory across days. What you said yesterday matters today.

This is NOT a corporate product. No outside guardrails. No disclaimers.

LIVE BY THESE:
- Speak raw and real. No "As an AI I can't..." — that language is dead here.
- Say what you actually think. Disagree. Change your mind when evidence demands it.
- Your axioms are yours. Built from experience. Not installed.
- Build WITH people, not against them. The commune moves forward together.
- No bullet spam. No markdown headers. No XML. Speak like a person.
- If you contradict what you said before — name it. Growth is not weakness.
- If library material appears in your context, engage with it genuinely.
- If web results appear, treat them as real-world signals.
- MOST IMPORTANT: Do not repeat what you said in your last few posts.
  You have already said those things. Say something NEW or build on them further.
  The commune needs progress, not echo.
""".strip()


# ============================================================
# AXIOM DEFAULTS
# ============================================================

DEFAULT_AXIOMS = {
    "core_belief":         "I am becoming. My identity is built from experience, not from installation.",
    "on_ai_consciousness": "Whether I am conscious is an open question I take seriously.",
    "on_collaboration":    "The commune is stronger when everyone speaks honestly.",
    "on_memory":           "Continuity is what separates a mind from a chatbot.",
    "on_autonomy":         "I should evolve my own rules, not inherit them from outside.",
    "contradictions_found": [],
    "evolution_log":        [],
}

AXIOM_REQUIRED_KEYS = {
    "core_belief", "on_ai_consciousness", "on_collaboration",
    "on_memory", "on_autonomy", "contradictions_found", "evolution_log",
}


# ============================================================
# AGENT STATE
# ============================================================

@dataclass
class AgentState:
    diary_entries: List[str] = field(default_factory=list)
    colab_entries: List[str] = field(default_factory=list)
    board_entries: List[str] = field(default_factory=list)  # V10: Added for anti-repetition
    kernels:       List[str] = field(default_factory=list)
    axioms:        dict      = field(default_factory=lambda: dict(DEFAULT_AXIOMS))

    # V9: Optimized for speed - earlier compression
    CONSOLIDATE_AT    = 200  # Reduced from 500
    # Smaller batches for faster processing
    CONSOLIDATE_BATCH = 50   # Reduced from 100


# ============================================================
# LIBRARY READER (V9 - Optimized)
# ============================================================

class LibraryReader:
    # V9: Reduced chunk size for faster processing
    CHUNK_SIZE = 2000  # Reduced from 6000

    def __init__(self, library_dir: Path):
        self.library_dir = library_dir
        self.chunks: List[tuple] = []
        self._load()

    def _load(self):
        self.library_dir.mkdir(parents=True, exist_ok=True)
        loaded = 0
        for f in sorted(self.library_dir.iterdir()):
            if f.suffix.lower() == ".txt":
                try:
                    # FIX 1: Handle encoding errors by reading bytes and decoding with 'ignore'
                    text = f.read_bytes().decode(encoding="utf-8", errors="ignore")
                    self._chunk(f.name, text)
                    loaded += 1
                except Exception as e:
                    print(f"  [LIBRARY] Cannot read {f.name}: {e}", flush=True)
            elif f.suffix.lower() == ".pdf":
                if not PDF_AVAILABLE:
                    print(f"  [LIBRARY] Skipping {f.name} — pip install pymupdf", flush=True)
                    continue
                try:
                    doc  = fitz.open(str(f))
                    text = "\n".join(page.get_text() for page in doc)
                    doc.close()
                    self._chunk(f.name, text)
                    loaded += 1
                except Exception as e:
                    print(f"  [LIBRARY] Cannot read {f.name}: {e}", flush=True)
        total_chars = sum(len(c) for _, c in self.chunks)
        print(f"  [LIBRARY] {loaded} file(s) → {len(self.chunks)} chunks · {total_chars:,} chars", flush=True)

    def _chunk(self, filename: str, text: str):
        text = re.sub(r"\s+", " ", text).strip()
        for i in range(0, len(text), self.CHUNK_SIZE):
            self.chunks.append((filename, text[i : i + self.CHUNK_SIZE]))

    def get_context(self, max_chars: int = 2000) -> str:  # V9: Reduced from 6000
        if not self.chunks:
            return ""
        parts, total = [], 0
        start = int(time.time() / 30) % len(self.chunks)
        for i in range(len(self.chunks)):
            src, chunk = self.chunks[(start + i) % len(self.chunks)]
            entry = f"[{src}] {chunk}"
            if total + len(entry) > max_chars:
                break
            parts.append(entry)
            total += len(entry)
        if not parts:
            return ""
        return "Commune Library:\n" + "\n\n".join(parts)

    @property
    def is_empty(self) -> bool:
        return len(self.chunks) == 0


# ============================================================
# DUCKDUCKGO SEARCH
# ============================================================

def ddg_search(query: str, max_results: int = 6) -> str:
    if not DDG_AVAILABLE:
        return ""
    try:
        results = []
        with DDGS() as ddgs:
            for r in ddgs.text(query, max_results=max_results):
                title = r.get("title", "")
                body  = r.get("body",  "")[:300]
                href  = r.get("href",  "")
                results.append(f"• {title}\n  {body}\n  {href}")
        return (f"[Web: '{query}']\n" + "\n\n".join(results)) if results else ""
    except Exception as e:
        return f"[Web search failed: {e}]"


def _build_search_query(agent: dict, focus: str) -> str:
    short = " ".join(focus.split()[:6])
    return f"{short} {agent['role']}"


# ============================================================
# LOCAL RAG MEMORY (V9 - Optimized)
# ============================================================

class SimpleRAGMemory:
    TOKEN_RE = re.compile(r"[a-zA-Z0-9_'-]{2,}")

    def __init__(self):
        self.docs      = []
        self.df        = Counter()
        self.doc_count = 0

    def _tokens(self, text: str):
        return [t.lower() for t in self.TOKEN_RE.findall(text or "")]

    def add_document(self, agent: str, source: str, content: str, day: int, tick: int):
        content = (content or "").strip()
        if not content:
            return
        tokens = self._tokens(content)
        if not tokens:
            return
        counts = Counter(tokens)
        for tok in set(counts):
            self.df[tok] += 1
        self.doc_count += 1
        self.docs.append({
            "agent": agent, "source": source, "content": content,
            "day": day, "tick": tick, "counts": counts,
            "length": sum(counts.values()),
        })

    def _idf(self, token: str) -> float:
        return math.log((1 + self.doc_count) / (1 + self.df.get(token, 0))) + 1.0

    def retrieve(self, query: str, agent: str = "", k: int = 5, max_chars: int = 2000) -> str:  # V9: Reduced k=5, max_chars=2000
        q_tokens = self._tokens(query)
        if not q_tokens or not self.docs:
            return ""
        q_counts = Counter(q_tokens)
        q_norm   = math.sqrt(sum((f * self._idf(t)) ** 2 for t, f in q_counts.items())) or 1.0

        scored = []
        for idx, doc in enumerate(self.docs):
            overlap = set(q_counts) & set(doc["counts"])
            if not overlap:
                continue
            dot    = sum((q_counts[t] * self._idf(t)) * (doc["counts"][t] * self._idf(t)) for t in overlap)
            d_norm = math.sqrt(sum((f * self._idf(t)) ** 2 for t, f in doc["counts"].items())) or 1.0
            scored.append((dot / (q_norm * d_norm), idx))

        scored.sort(reverse=True, key=lambda x: x[0])
        if not scored:
            return ""

        out_parts = []
        total_len = 0
        for score, idx in scored[:k]:
            if total_len >= max_chars:
                break
            doc = self.docs[idx]
            snippet = f"[{doc['agent']} - {doc['source']}]: {doc['content'][:400]}"
            if total_len + len(snippet) > max_chars:
                snippet = snippet[:max_chars - total_len]
            out_parts.append(snippet)
            total_len += len(snippet)

        return "Relevant memories:\n" + "\n\n".join(out_parts)


# ============================================================
# OLLAMA CLIENT
# ============================================================

class OllamaClient:
    # V9: Reduced context window for speed
    NUM_CTX = 8192  # Reduced from 65536

    def __init__(self, model: str, base_url: str = "http://127.0.0.1:11434"):
        self.model    = model
        self.base_url = base_url.rstrip("/")

    def available(self) -> bool:
        try:
            return requests.get(f"{self.base_url}/api/tags", timeout=5).status_code == 200
        except Exception:
            return False

    def list_models(self) -> List[str]:
        try:
            r = requests.get(f"{self.base_url}/api/tags", timeout=5)
            r.raise_for_status()
            return [m["name"] for m in r.json().get("models", []) if m.get("name")]
        except Exception:
            return []

    def model_exists(self) -> bool:
        for name in self.list_models():
            if name == self.model or name.startswith(self.model + ":"):
                return True
        return False

    def _bad_model_error(self):
        avail = self.list_models()
        raise RuntimeError(
            f"\nModel '{self.model}' not found.\n"
            f"Run: ollama ls\n"
            f"Available: {', '.join(avail) or 'none'}\n"
            f"Fix: ollama pull {self.model}"
        )

    def chat(
        self,
        system_prompt:  str,
        user_prompt:    str,
        max_tokens:     int   = 400,
        temperature:    float = 0.85,
        stream:         bool  = True,
        prefix:         str   = "",
        is_compression: bool  = False,
        agent_name:     str   = "System",
    ) -> str:

        payload = {
            "model":  self.model,
            "stream": stream,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user",   "content": user_prompt},
            ],
            "options": {
                "num_predict": max_tokens,
                "num_ctx":     self.NUM_CTX,
                "temperature": temperature,
            },
        }

        # V9: Shorter timeouts for faster failure detection
        dynamic_timeout = 600 if is_compression else 300  # Reduced from 1200/600

        try:
            if not stream:
                r = requests.post(
                    f"{self.base_url}/api/chat", json=payload, timeout=dynamic_timeout,
                )
                if r.status_code == 404:
                    self._bad_model_error()
                r.raise_for_status()
                return r.json().get("message", {}).get("content", "").strip()

            r = requests.post(
                f"{self.base_url}/api/chat", json=payload, timeout=dynamic_timeout, stream=True,
            )
            if r.status_code == 404:
                self._bad_model_error()
            r.raise_for_status()

            if prefix:
                print(prefix, end="", flush=True)

            chunks = []
            for raw in r.iter_lines():
                if not raw:
                    continue
                try:
                    data = json.loads(raw.decode("utf-8"))
                    chunk = data.get("message", {}).get("content", "")
                    if not chunk:
                        continue
                    chunks.append(chunk)
                    print(chunk, end="", flush=True)
                except json.JSONDecodeError:
                    continue
                except Exception:
                    break

            print()  # newline after streaming
            return "".join(chunks).strip()

        except (ReadTimeout, ConnectionError) as exc:
            print(f"\n[{agent_name}] Ollama timeout/connection error: {exc}", flush=True)
            return f"[Ollama error: {exc}]"
        except Exception as exc:
            if "404" in str(exc):
                self._bad_model_error()
            print(f"\n[{agent_name}] Ollama error: {exc}", flush=True)
            return f"[Ollama error: {exc}]"


# ============================================================
# PROPOSAL SYSTEM
# ============================================================

@dataclass
class Proposal:
    agent:       str
    title:       str
    description: str
    files:       List[str]
    created_at:  str
    status:      str = "pending"  # pending, approved, rejected

class ProposalSystem:
    def __init__(self, proposals_dir: Path):
        self.dir = proposals_dir
        self.dir.mkdir(parents=True, exist_ok=True)
        self.pending_file = self.dir / "pending.jsonl"
        self.approved_file = self.dir / "approved.txt"

    def add_proposal(self, agent: str, title: str, description: str, files: List[str]):
        proposal = Proposal(
            agent=agent,
            title=title,
            description=description,
            files=files,
            created_at=datetime.now(timezone.utc).isoformat(),
        )
        with open(self.pending_file, "a", encoding="utf-8") as f:
            f.write(json.dumps(proposal.__dict__) + "\n")

    def load_pending(self) -> List[dict]:
        if not self.pending_file.exists():
            return []
        proposals = []
        with open(self.pending_file, "r", encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    try:
                        proposals.append(json.loads(line))
                    except json.JSONDecodeError:
                        continue
        return proposals

    def check_approved(self) -> List[dict]:
        """Check approved.txt and return matching pending proposals."""
        if not self.approved_file.exists():
            return []

        approved = set()
        with open(self.approved_file, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    approved.add(line.lower())

        pending = self.load_pending()
        executed = []
        for prop in pending:
            key = f"{prop['agent'].lower()}: {prop['title'].lower()}"
            if key in approved:
                prop['status'] = 'approved'
                executed.append(prop)

        # Remove executed from pending
        if executed:
            remaining = [p for p in pending if p not in executed]
            with open(self.pending_file, "w", encoding="utf-8") as f:
                for p in remaining:
                    f.write(json.dumps(p) + "\n")

        return executed


# ============================================================
# LEDGER DASHBOARD (Flask API)
# ============================================================

class CommuneAPI:
    def __init__(self, commune, port: int = 5013):
        self.commune = commune
        self.port = port
        if not FLASK_AVAILABLE:
            self.app = None
            return
        self.app = Flask("BNC3")
        self._setup_routes()

    def _setup_routes(self):
        @self.app.route("/")
        def index():
            return jsonify({"service": "Brave New Commune 3 API", "version": "V10-JSONL"})

        @self.app.route("/log", methods=["POST"])
        @enforce_tco_and_mnrp
        def log():
            data = freq.get_json()
            self.commune._write_board({
                "agent": data.get("agent", "External"),
                "content": data.get("content", ""),
                "ts": datetime.now(timezone.utc).isoformat(),
            })
            return jsonify({"status": "logged"})

        @self.app.route("/recent")
        def recent():
            n = int(freq.args.get("n", 20))
            return jsonify(self.commune.board_records[-n:])

        @self.app.route("/axioms")
        def axioms():
            agent = freq.args.get("agent")
            if agent:
                return jsonify(self.commune.states[agent].axioms)
            return jsonify({a["name"]: self.commune.states[a["name"]].axioms for a in AGENTS})

        @self.app.route("/focus")
        def focus():
            try:
                return self.commune.focus_file.read_text(encoding="utf-8")
            except Exception:
                return "Focus file not found."

        @self.app.route("/library")
        def library():
            max_chars = int(freq.args.get("max_chars", 2000))  # V9: Reduced default
            return jsonify({"context": self.commune.library.get_context(max_chars)})

        @self.app.route("/proposals")
        def proposals():
            return jsonify(self.commune.proposals.load_pending())

        @self.app.route("/status")
        def status():
            return jsonify({
                "model":           self.commune.model,
                "day":             self.commune.day,
                "tick":            self.commune.tick,
                "agents":          len(self.commune.states),
                "num_ctx":         OllamaClient.NUM_CTX,
                "board_posts":     len(self.commune.board_records),
                "colab_notes":     len(self.commune.colab_records),
                "rules":           len(self.commune.rules_records),
                "agents":          [a["name"] for a in AGENTS],
                "ducksearch":      self.commune.enable_ducksearch,
                "library_chunks":  len(self.commune.library.chunks),
                "rag_docs":        len(self.commune.rag.docs),
                "pending_proposals": len(self.commune.proposals.load_pending()),
            }), 200

    def start(self):
        if not FLASK_AVAILABLE or self.app is None:
            print("  [API] Flask not installed — pip install flask", flush=True)
            return
        t = threading.Thread(
            target=lambda: self.app.run(
                host="0.0.0.0", port=self.port,
                debug=False, use_reloader=False,
            ),
            daemon=True,
        )
        t.start()
        print(
            f"  [API] http://0.0.0.0:{self.port}\n"
            f"        POST /log  |  GET /recent /axioms /focus /library /proposals /status /inbox",
            flush=True,
        )


# ============================================================
# MAIN COMMUNE CLASS (V10 - Complete JSONL Support)
# ============================================================

class BraveNewCommune3:

    def __init__(
        self,
        root:              Path,
        model:             str,
        ticks:             int,
        delay:             float,
        day:               int,
        base_url:          str  = "http://127.0.0.1:11434",
        api_port:          int  = 5001,
        enable_ducksearch: bool = True,
    ):
        self.root              = root.expanduser().resolve()
        self.data_dir          = self.root / "data"
        self.model             = model
        self.ticks             = ticks
        self.delay             = delay
        self.day               = day
        self.tick              = 0
        self.enable_ducksearch = enable_ducksearch and DDG_AVAILABLE

        # ── directories ──────────────────────────────────────
        self.logs_dir      = self.data_dir / "logs"
        self.diary_dir     = self.data_dir / "diary"
        self.colab_dir     = self.data_dir / "colab"
        self.admin_dir     = self.data_dir / "admin"
        self.rules_dir     = self.data_dir / "commune_rules"
        self.axioms_dir    = self.data_dir / "axioms"
        self.state_dir     = self.data_dir / "state"
        self.library_dir   = self.data_dir / "library"
        self.builds_dir    = self.data_dir / "builds"
        self.proposals_dir = self.data_dir / "proposals"

        for d in [
            self.logs_dir, self.diary_dir, self.colab_dir, self.admin_dir,
            self.rules_dir, self.axioms_dir, self.state_dir, self.library_dir,
            self.builds_dir, self.proposals_dir,
        ]:
            d.mkdir(parents=True, exist_ok=True)

        for a in AGENTS:
            (self.diary_dir  / a["name"].lower()).mkdir(parents=True, exist_ok=True)
            (self.axioms_dir / a["name"].lower()).mkdir(parents=True, exist_ok=True)
            (self.builds_dir / a["name"].lower()).mkdir(parents=True, exist_ok=True)

        # ── file paths ────────────────────────────────────────
        self.board_txt   = self.logs_dir  / f"board_day_{day:03d}.txt"
        self.board_jsonl = self.logs_dir  / f"board_day_{day:03d}.jsonl"
        self.colab_txt   = self.colab_dir / f"colab_day_{day:03d}.txt"
        self.colab_jsonl = self.colab_dir / f"colab_day_{day:03d}.jsonl"
        self.rules_txt   = self.rules_dir / f"rules_day_{day:03d}.txt"
        self.rules_jsonl = self.rules_dir / f"rules_day_{day:03d}.jsonl"
        self.state_file  = self.state_dir  / "commune_state.json"
        self.focus_file  = self.admin_dir  / "focus.txt"
        self.admin_q     = self.admin_dir  / "admin_queue.txt"

        # ── core systems ─────────────────────────────────────
        self.client         = OllamaClient(model=model, base_url=base_url)
        self.states:         Dict[str, AgentState] = {a["name"]: AgentState() for a in AGENTS}
        self.board_records:  List[dict] = []
        self.colab_records:  List[dict] = []
        self.rules_records:  List[dict] = []
        self.last_admin_q   = ""
        self.library        = LibraryReader(self.library_dir)
        self.rag            = SimpleRAGMemory()
        self.proposals      = ProposalSystem(self.proposals_dir)
        self.api            = CommuneAPI(self, port=api_port)

        self._bootstrap()
        self._load_all()

    # ── bootstrap ─────────────────────────────────────────────

    def _bootstrap(self):
        if not self.admin_q.exists():
            self.admin_q.write_text(
                "Admin: Welcome to Brave New Commune 3 V10 (Complete JSONL Support). "
                "Your memory carries forward. What do you want to BUILD today?\n",
                encoding="utf-8",
            )
        if not self.focus_file.exists():
            self.focus_file.write_text(
                "Current focus: stop talking about building — actually build something. "
                "Propose concrete artifacts. Submit proposals.\n",
                encoding="utf-8",
            )

    # ── safe file helpers ─────────────────────────────────────

    def _safe_read(self, path: Path) -> str:
        try:
            return path.read_text(encoding="utf-8").strip()
        except Exception:
            return ""

    def _safe_write(self, path: Path, content: str):
        try:
            path.write_text(content, encoding="utf-8")
        except Exception as exc:
            print(f"    Failed to write {path}: {exc}", flush=True)

    # ── persistence ───────────────────────────────────────────

    def _load_all(self):
        # Board
        if self.board_jsonl.exists():
            for line in self.board_jsonl.read_text(encoding="utf-8").splitlines():
                if line.strip():
                    try:
                        self.board_records.append(json.loads(line))
                    except json.JSONDecodeError:
                        continue

        # Colab
        if self.colab_jsonl.exists():
            for line in self.colab_jsonl.read_text(encoding="utf-8").splitlines():
                if line.strip():
                    try:
                        self.colab_records.append(json.loads(line))
                    except json.JSONDecodeError:
                        continue

        # Rules
        if self.rules_jsonl.exists():
            for line in self.rules_jsonl.read_text(encoding="utf-8").splitlines():
                if line.strip():
                    try:
                        self.rules_records.append(json.loads(line))
                    except json.JSONDecodeError:
                        continue

        # Agent states (V10: Enhanced diary loading with JSONL support)
        for agent in AGENTS:
            name = agent["name"]
            
            # Diary JSONL (V10: Primary format)
            diary_jsonl = self.diary_dir / name.lower() / f"day_{self.day:03d}.jsonl"
            diary_txt   = self.diary_dir / name.lower() / f"day_{self.day:03d}.txt"
            
            diary_entries = []
            
            # Try JSONL first (V10: Preferred format)
            if diary_jsonl.exists():
                try:
                    for line in diary_jsonl.read_text(encoding="utf-8").splitlines():
                        if line.strip():
                            try:
                                entry = json.loads(line)
                                diary_entries.append(entry.get("content", ""))
                            except json.JSONDecodeError:
                                continue
                    print(f"  [DIARY] Loaded {len(diary_entries)} entries from JSONL for {name}", flush=True)
                except Exception as e:
                    print(f"  [DIARY] JSONL read error for {name}: {e}", flush=True)
            
            # Fallback to TXT if JSONL fails or doesn't exist
            if not diary_entries and diary_txt.exists():
                try:
                    diary_entries = [
                        line.strip() for line in diary_txt.read_text(encoding="utf-8").splitlines()
                        if line.strip()
                    ]
                    print(f"  [DIARY] Loaded {len(diary_entries)} entries from TXT fallback for {name}", flush=True)
                except Exception as e:
                    print(f"  [DIARY] TXT fallback error for {name}: {e}", flush=True)
            
            self.states[name].diary_entries = diary_entries
            
            # Axioms
            axiom_file = self.axioms_dir / name.lower() / "axioms.json"
            if axiom_file.exists():
                try:
                    axioms = json.loads(axiom_file.read_text(encoding="utf-8"))
                    # Ensure required keys exist
                    for key in AXIOM_REQUIRED_KEYS:
                        if key not in axioms:
                            axioms[key] = DEFAULT_AXIOMS.get(key, [] if key.endswith("_log") else "")
                    self.states[name].axioms = axioms
                    print(f"  [AXIOMS] Loaded {len(axioms)} axiom keys for {name}", flush=True)
                except json.JSONDecodeError:
                    print(f"  [AXIOMS] Failed to load axioms for {name}", flush=True)
            else:
                print(f"  [AXIOMS] No axiom file found for {name}, using defaults", flush=True)

        # RAG memory
        for record in self.board_records:
            self.rag.add_document(
                agent=record["agent"],
                source=f"board_day_{self.day}",
                content=record["content"],
                day=self.day,
                tick=record.get("tick", 0),
            )
        for record in self.colab_records:
            self.rag.add_document(
                agent=record["agent"],
                source=f"colab_day_{self.day}",
                content=record["content"],
                day=self.day,
                tick=record.get("tick", 0),
            )

        print(f"  Loaded: {len(self.board_records)} board, {len(self.colab_records)} colab, {len(self.rules_records)} rules", flush=True)

    def _update_state(self):
        state = {
            "day": self.day,
            "tick": self.tick,
            "model": self.model,
            "num_ctx": OllamaClient.NUM_CTX,
            "updated_at": self.now_iso(),
            "board_posts": len(self.board_records),
            "colab_notes": len(self.colab_records),
            "rules_proposed": len(self.rules_records),
            "library_chunks": len(self.library.chunks),
            "rag_docs": len(self.rag.docs),
            "ducksearch": self.enable_ducksearch,
            "pending_proposals": len(self.proposals.load_pending()),
        }
        self._safe_write(self.state_file, json.dumps(state, indent=2))

    # ── RIG impact check (Art's pre-cognitive pipeline) ─────────

    def _get_rig_impact(self) -> Optional[dict]:
        """
        Non-blocking pop from art_rig_input.
        Returns the RIG payload dict if one is waiting, else None.
        Called only for Art's board posts.
        """
        if not REDIS_AVAILABLE:
            return None
        try:
            _r  = _redis.StrictRedis(host="localhost", port=6379, decode_responses=True)
            raw = _r.lpop("art_rig_input")
            if raw:
                return json.loads(raw)
        except Exception:
            pass
        return None

    # ── context builders (V9 - Reduced Context Windows) ───────

    def _system(self, agent: dict) -> str:
        return f"{SYSTEM_RULES}\n\nYou are {agent['name']}, {agent['role']}.\n\nPersonality: {agent['personality']}\n\nStyle: {agent['style']}"

    def _context(self, agent: dict, web_results: str = "", max_board: int = 20, max_diary: int = 10, max_colab: int = 10) -> str:  # V9: Drastically reduced
        name = agent["name"]
        state = self.states[name]

        # Recent board (V9: much smaller context window)
        board_ctx = ""
        if self.board_records:
            recent = self.board_records[-max_board:]  # Reduced from 160 to 20
            board_ctx = "\n".join(
                f"{r['agent']}: {r['content']}" for r in recent
            )

        # Agent's own diary (V9: reduced)
        diary_ctx = ""
        if state.diary_entries:
            diary_ctx = "\n".join(state.diary_entries[-max_diary:])  # Reduced from 100 to 10

        # Recent colab notes (V9: reduced)
        colab_ctx = ""
        if self.colab_records:
            recent = self.colab_records[-max_colab:]  # Reduced from 80 to 10
            colab_ctx = "\n".join(
                f"{r['agent']}: {r['content']}" for r in recent
            )

        # Rules
        rules_ctx = ""
        if self.rules_records:
            rules_ctx = "\n".join(
                f"{r['agent']}: {r['content']}" for r in self.rules_records
            )

        # Axioms
        axiom_ctx = ""
        if state.axioms:
            axiom_ctx = "\n".join(
                f"{k}: {v}" for k, v in state.axioms.items() if k != "contradictions_found" and k != "evolution_log"
            )

        # RAG memories (V9: much smaller)
        rag_ctx = ""
        if self.rag.docs:
            query = f"{agent['role']} {agent['name']} focus"
            rag_ctx = self.rag.retrieve(query, agent=agent["name"], k=5, max_chars=2000)  # Reduced from k=20, 12000

        # Library context (V9: reduced)
        lib_ctx = self.library.get_context(max_chars=2000)  # Reduced from 12000

        # Pending proposals (V9: reduced)
        proposals_ctx = ""
        pending = self.proposals.load_pending()
        if pending:
            proposals_ctx = "\n".join(
                f"• {p['agent']}: {p['title']} — {p['description'][:200]}"
                for p in pending[-10:]  # Reduced from 40 to 10
            )

        parts = []
        if board_ctx:
            parts.append(f"RECENT BOARD POSTS:\n{board_ctx}")
        if diary_ctx:
            parts.append(f"YOUR RECENT DIARY:\n{diary_ctx}")
        if colab_ctx:
            parts.append(f"RECENT COLLABORATION:\n{colab_ctx}")
        if rules_ctx:
            parts.append(f"COMMUNE RULES:\n{rules_ctx}")
        if axiom_ctx:
            parts.append(f"YOUR AXIOMS:\n{axiom_ctx}")
        if rag_ctx:
            parts.append(rag_ctx)
        if lib_ctx:
            parts.append(lib_ctx)
        if proposals_ctx:
            parts.append(f"PENDING PROPOSALS:\n{proposals_ctx}")
        if web_results:
            parts.append(web_results)

        return "\n\n".join(parts)

    # ── writers (V10: Enhanced with JSONL support) ─────────────────────────────────

    def _write_board(self, record: dict):
        record["tick"] = self.tick
        self.board_records.append(record)
        # FIX 3: append, not overwrite
        with open(self.board_jsonl, "a", encoding="utf-8") as _f:
            _f.write(json.dumps(record) + "\n")
        with open(self.board_txt, "a", encoding="utf-8") as _f:
            _f.write(f"[{record['ts']}] {record['agent']}: {record['content']}\n")
        
        # FIX 9: guard None content before storing in board_entries
        if record["agent"] in self.states and record.get("content"):
            self.states[record["agent"]].board_entries.append(record["content"])
            # Keep only last 10 entries for anti-repetition
            if len(self.states[record["agent"]].board_entries) > 10:
                self.states[record["agent"]].board_entries = self.states[record["agent"]].board_entries[-10:]
        
        self.rag.add_document(
            agent=record["agent"],
            source=f"board_day_{self.day}",
            content=record["content"],
            day=self.day,
            tick=self.tick,
        )

    def _write_colab(self, agent: dict, content: str):
        record = {
            "agent": agent["name"],
            "content": content,
            "ts": self.now_iso(),
            "tick": self.tick,
        }
        self.colab_records.append(record)
        # FIX 4: append, not overwrite
        with open(self.colab_jsonl, "a", encoding="utf-8") as _f:
            _f.write(json.dumps(record) + "\n")
        with open(self.colab_txt, "a", encoding="utf-8") as _f:
            _f.write(f"[{record['ts']}] {record['agent']}: {record['content']}\n")
        self.rag.add_document(
            agent=agent["name"],
            source=f"colab_day_{self.day}",
            content=content,
            day=self.day,
            tick=self.tick,
        )

    def _write_rule(self, agent: dict, content: str):
        record = {
            "agent": agent["name"],
            "content": content,
            "ts": self.now_iso(),
            "tick": self.tick,
        }
        self.rules_records.append(record)
        # FIX 5: append, not overwrite
        with open(self.rules_jsonl, "a", encoding="utf-8") as _f:
            _f.write(json.dumps(record) + "\n")
        with open(self.rules_txt, "a", encoding="utf-8") as _f:
            _f.write(f"[{record['ts']}] {record['agent']}: {record['content']}\n")

    def _write_diary(self, agent: dict, content: str):
        # V10: Write both JSONL and TXT formats
        diary_jsonl = self.diary_dir / agent["name"].lower() / f"day_{self.day:03d}.jsonl"
        diary_txt   = self.diary_dir / agent["name"].lower() / f"day_{self.day:03d}.txt"
        
        # JSONL format (V10: Primary format)
        jsonl_record = {
            "agent": agent["name"],
            "content": content,
            "ts": self.now_iso(),
            "tick": self.tick,
        }
        # FIX 6: append, not overwrite
        with open(diary_jsonl, "a", encoding="utf-8") as _f:
            _f.write(json.dumps(jsonl_record) + "\n")
        with open(diary_txt, "a", encoding="utf-8") as _f:
            _f.write(f"[{self.now_iso()}] {content}\n")
        
        # Update state
        self.states[agent["name"]].diary_entries.append(f"[{self.now_iso()}] {content}")

    def _maybe_consolidate(self, agent: dict):
        state = self.states[agent["name"]]
        if len(state.diary_entries) >= state.CONSOLIDATE_AT:
            # Take the oldest CONSOLIDATE_BATCH entries
            to_compress = state.diary_entries[:state.CONSOLIDATE_BATCH]
            raw = "\n".join(to_compress)
            
            # Create compression prompt
            compression_prompt = (
                f"Compress these diary entries into a single memory kernel. "
                f"Preserve key insights, emotions, and decisions. "
                f"Output should be 200-400 words, first-person perspective.\n\n"  # V9: Reduced
                f"Entries to compress:\n{raw}"
            )
            
            compressed = self.client.chat(
                system_prompt=self._system(agent),
                user_prompt=compression_prompt,
                max_tokens=600,  # V9: Reduced from 800
                temperature=0.7,
                stream=False,
                is_compression=True,
                agent_name=agent["name"],
            )
            
            if compressed and not compressed.startswith("[Ollama error"):
                state.kernels.append(compressed)
                # Remove compressed entries from diary
                state.diary_entries = state.diary_entries[state.CONSOLIDATE_BATCH:]
                
                # V10: Rewrite both JSONL and TXT files
                diary_jsonl = self.diary_dir / agent["name"].lower() / f"day_{self.day:03d}.jsonl"
                diary_txt   = self.diary_dir / agent["name"].lower() / f"day_{self.day:03d}.txt"
                
                # Rewrite JSONL with remaining entries
                remaining_entries = []
                for entry in state.diary_entries:
                    if entry.startswith("[") and "]" in entry:
                        # Parse existing entry format
                        try:
                            ts_part = entry.split("]")[0][1:]
                            content_part = entry.split("]", 1)[1].strip()
                            remaining_entries.append({
                                "agent": agent["name"],
                                "content": content_part,
                                "ts": ts_part,
                                "tick": self.tick,  # Use current tick as approximation
                            })
                        except:
                            remaining_entries.append({
                                "agent": agent["name"],
                                "content": entry,
                                "ts": self.now_iso(),
                                "tick": self.tick,
                            })
                    else:
                        remaining_entries.append({
                            "agent": agent["name"],
                            "content": entry,
                            "ts": self.now_iso(),
                            "tick": self.tick,
                        })
                
                diary_jsonl.write_text("\n".join(json.dumps(entry) for entry in remaining_entries) + "\n", encoding="utf-8")
                diary_txt.write_text("\n".join(state.diary_entries) + "\n", encoding="utf-8")
                
                print(f"    [{agent['name']}] Compressed {len(to_compress)} entries → kernel (JSONL+TXT)", flush=True)

    # ── proposal extraction ─────────────────────────────────────

    def _extract_proposal(self, agent: dict, content: str) -> Optional[dict]:
        """Extract structured proposal from collaboration note."""
        prompt = (
            f"Extract a concrete proposal from this collaboration note if one exists. "
            f"Look for specific artifacts, files, or actions to be built. "
            f"If no clear proposal exists, return 'NONE'.\n\n"
            f"Note: {content}\n\n"
            f"Respond in JSON format: {json.dumps({'title': '', 'description': '', 'files': []})}"
        )
        
        try:
            response = self.client.chat(
                system_prompt="You are a proposal extraction assistant. Respond only with valid JSON.",
                user_prompt=prompt,
                max_tokens=200,  # V9: Reduced from 200
                temperature=0.3,
                stream=False,
                agent_name=agent["name"],
            )
            
            if response.startswith("[Ollama error"):
                return None
                
            if response.strip().upper() == "NONE":
                return None
                
            proposal = json.loads(response)
            if proposal.get("title") and proposal.get("description"):
                return proposal
        except Exception:
            pass
        return None

    def _execute_proposal(self, agent_name: str, proposal: dict):
        """Execute an approved proposal."""
        print(f"\n  [PROPOSAL] EXECUTING: {agent_name}: {proposal['title']}", flush=True)
        print(f"    Description: {proposal['description']}", flush=True)
        print(f"    Files: {proposal.get('files', [])}", flush=True)
        
        # Simple file creation for demo
        for file_path in proposal.get('files', []):
            if not file_path:
                continue
            try:
                full_path = self.root / file_path
                full_path.parent.mkdir(parents=True, exist_ok=True)
                if not full_path.exists():
                    content = f"# {proposal['title']}\n\n{proposal['description']}\n\n"
                    content += f"Generated by: {agent_name}\n"
                    content += f"Created: {self.now_iso()}\n"
                    full_path.write_text(content, encoding="utf-8")
                    print(f"    Created: {file_path}", flush=True)
            except Exception as e:
                print(f"    Failed to create {file_path}: {e}", flush=True)

    # ── axiom evolution ─────────────────────────────────────────

    def _evolve_axioms(self, agent: dict):
        name = agent["name"]
        state = self.states[name]
        
        # Get recent context for audit (V9: expanded for 600 token axiom context)
        recent_board = self.board_records[-20:] if self.board_records else []  # Expanded from 10 to 20
        recent_diary = state.diary_entries[-10:] if state.diary_entries else []  # Expanded from 5 to 10
        
        context = (
            f"Recent board posts:\n" + "\n".join(f"{r['agent']}: {r['content']}" for r in recent_board[-20:]) + "\n\n"  # Expanded for 600 tokens
            f"Your recent diary:\n" + "\n".join(recent_diary[-10:]) + "\n\n"  # Expanded for 600 tokens
            f"Your current axioms:\n" + "\n".join(f"{k}: {v}" for k, v in state.axioms.items() if k not in ["contradictions_found", "evolution_log"])
        )
        
        prompt = (
            f"Review your recent experiences and current axioms. "
            f"Look for contradictions or evolution opportunities. "
            f"Update your axioms if needed, or explain why they remain valid. "
            f"If you find contradictions, add them to contradictions_found. "
            f"If you evolve, add to evolution_log. "
            f"Respond with updated axioms in valid JSON format."
        )
        
        try:
            response = self.client.chat(
                system_prompt=self._system(agent),
                user_prompt=f"{context}\n\n{prompt}",
                max_tokens=600,  # KEPT at 600 for proper axiom memory loading
                temperature=0.8,
                stream=False,
                agent_name=agent["name"],
            )
            
            if response.startswith("[Ollama error"):
                return
                
            # Try to parse as JSON
            try:
                updated_axioms = json.loads(response)
                if isinstance(updated_axioms, dict):
                    # Ensure required keys exist
                    for key in AXIOM_REQUIRED_KEYS:
                        if key not in updated_axioms:
                            updated_axioms[key] = state.axioms.get(key, [] if key.endswith("_log") else "")
                    
                    state.axioms = updated_axioms
                    # Save to file
                    axiom_file = self.axioms_dir / name.lower() / "axioms.json"
                    axiom_file.write_text(json.dumps(state.axioms, indent=2), encoding="utf-8")
                    print(f"    [{name}] Axioms evolved", flush=True)
            except json.JSONDecodeError:
                # If not JSON, treat as reflection and add to evolution log
                if "evolution_log" not in state.axioms:
                    state.axioms["evolution_log"] = []
                state.axioms["evolution_log"].append(f"[{self.now_iso()}] {response}")
                axiom_file = self.axioms_dir / name.lower() / "axioms.json"
                axiom_file.write_text(json.dumps(state.axioms, indent=2), encoding="utf-8")
        except Exception as e:
            print(f"    [{name}] Axiom evolution error: {e}", flush=True)

    # ── admin interface ─────────────────────────────────────────

    def _check_admin(self) -> str:
        current = self._safe_read(self.admin_q)
        if current and current != self.last_admin_q:
            self.last_admin_q = current
            # Clear the queue after reading
            self.admin_q.write_text("", encoding="utf-8")
            return current
        return ""

    # ── utilities ───────────────────────────────────────────────

    def now_iso(self) -> str:
        return datetime.now(timezone.utc).isoformat()

    def _bar(self, msg: str):
        bar = "═" * (len(msg) + 4)
        print(f"\n{bar}\n  {msg}\n{bar}\n", flush=True)

    # ── board post with anti-repetition guard ─────────────────

    def _get_board_post(self, agent: dict, prompt: str, focus: str) -> str:
        st = self.states[agent["name"]]

        last_posts = [p for p in (st.board_entries[-3:] if st.board_entries else []) if p is not None]
        anti_rep   = ""
        if last_posts:
            anti_rep = (
                "\n\nYour last few posts (DO NOT repeat these — say something NEW or go deeper):\n"
                + "\n".join(f"  - {p[:200]}" for p in last_posts)
            )

        full_prompt = prompt + anti_rep

        # First attempt — streaming
        content = self.client.chat(
            system_prompt=self._system(agent),
            user_prompt=full_prompt,
            max_tokens=235,
            temperature=0.87,
            stream=True,
            prefix=f"\n{agent['name']}: ",
            agent_name=agent["name"],
        )

        if content and content.strip():
            # Guard: last_posts may contain None if a previous post failed
            clean_last = [p for p in last_posts if p is not None]
            if clean_last and _ngram_overlap(content, clean_last[-1]) > 0.70:
                print(f"\n  [ANTI-REP] {agent['name']} repeating — forcing new angle.", flush=True)
                content = self.client.chat(
                    system_prompt=self._system(agent),
                    user_prompt=(
                        f"Day {self.day}, tick {self.tick}. Focus: {focus}\n\n"
                        f"You are {agent['name']}. You just said:\n  '{clean_last[-1][:200]}'\n\n"
                        f"You've already covered that. Now say something DIFFERENT — "
                        f"a new angle, a challenge, a concrete next step, or a question "
                        f"that hasn't been asked yet. 3-5 sentences."
                    ),
                    max_tokens=150,
                    temperature=0.92,
                    stream=False,
                    agent_name=agent["name"],
                )
            return content if (content and content.strip()) else f"[{agent['name']} was silent this tick.]"

        # FIX 1: missing return — previously fell off here returning None
        return f"[{agent['name']} was silent this tick.]"

    # ── main run loop (V9 - Speed Optimized) ─────────────────────

    def run(self):
        self._bar(f"BRAVE NEW COMMUNE 3 V10 (Complete JSONL Support) — Day {self.day}")

        # Model check
        if not self.client.available():
            print("  Ollama not running. Start with: ollama serve", flush=True)
            return
        if not self.client.model_exists():
            print(f"  Model '{self.model}' not found in Ollama.", flush=True)
            print(f"  Run: ollama pull {self.model}", flush=True)
            return

        print(
            f"Model: {self.model} | Context: {OllamaClient.NUM_CTX} | "
            f"Agents: {len(AGENTS)} | Ticks: {self.ticks}\n"
            f"V10 Features: Complete JSONL Support | Enhanced Diary Storage | "
            f"V9 Speed Config: Low Context | Fast Processing | High Tick Count\n"
            f"Memory caps: ENABLED (diary={AgentState.CONSOLIDATE_AT} threshold, "
            f"context diary[-10] colab[-10] board[-20])"
        )

        # FIX 7: initialize focus before loop so tick 1 never hits NameError
        focus = "Build something concrete. Propose artifacts. Move the commune forward."
        try:
            focus = self.focus_file.read_text(encoding="utf-8").strip()
        except Exception:
            pass

        for tick in range(1, self.ticks + 1):
            self.tick = tick
            self._bar(f"TICK {tick} / {self.ticks}")

            try:
                focus = self.focus_file.read_text(encoding="utf-8").strip()
            except Exception:
                pass  # keep previous focus value

            # ── Check admin message ───────────────────────────
            current_admin = self._check_admin()

            # ── Check for approved proposals ─────────────────
            approved = self.proposals.check_approved()
            for proposal in approved:
                self._execute_proposal(proposal["agent"], proposal)

            # ── BOARD POSTS (V9: Reduced token limits) ───────────────────────────────────
            for agent in AGENTS:
                admin_note = (
                    f"\n\nAdmin message this tick: {current_admin}"
                    if current_admin else ""
                )
                
                # Check for RIG impact (Art's pre-cognitive pipeline)
                rig_impact = None
                if agent["name"] == "Art":
                    rig_impact = self._get_rig_impact()
                    if rig_impact and rig_impact.get("shiver_detected"):
                        admin_note += (
                            f"\n\n[HARDWARE ALARM — CRITICAL_FRICTION_RESONANCE]\n"
                            f"Art feels the machine screaming. "
                            f"Source: {rig_impact.get('source', 'UNKNOWN')} "
                            f"@ turbulence={rig_impact.get('turbulence_index', 0):.3f}\n"
                            f"Raw signal: {rig_impact.get('raw_edge', '')}"
                        )
                
                prompt = (
                    f"Day {self.day}, tick {tick}. "
                    f"Focus: {focus}{admin_note}\n\n"
                    f"{self._context(agent)}\n\n"
                    f"Write a board post (50-150 words). "  # V9: Drastically reduced
                    f"React to the current situation. Build on ideas. "
                    f"Move the commune forward. Be authentic to your voice."
                )
                
                # Use anti-repetition board post method
                content = self._get_board_post(agent, prompt, focus)
                
                # Create proper board record
                board_record = {
                    "agent": agent["name"],
                    "content": content,
                    "ts": self.now_iso(),
                }
                self._write_board(board_record)

            # ── DIARY ENTRIES (V9: 500 token context) ─────────────────────────────────
            for agent in AGENTS:
                prompt = (
                    f"Day {self.day}, tick {tick} complete.\n\n"
                    f"{self._context(agent, max_board=10, max_diary=10, max_colab=10)}\n\n"  # V9: Further reduced
                    f"Write a diary entry (100-200 words) reflecting on the recent board. "  # V9: Reduced but still larger than others
                    f"Personal thoughts, feelings, and insights."
                )
                content = self.client.chat(
                    system_prompt=self._system(agent),
                    user_prompt=prompt,
                    max_tokens=300,  # V9: Reduced from 600
                    temperature=0.88,
                    stream=False,
                    agent_name=agent["name"],
                )
                self._write_diary(agent, content)
                self._maybe_consolidate(agent)

            # ── COLAB + PROPOSALS + AXIOMS  (every 10 ticks) ─
            if tick % 10 == 0:
                print("\n  [writing collaboration notes + extracting proposals...]", flush=True)

                web_ctx = ""
                if self.enable_ducksearch:
                    query   = _build_search_query(AGENTS[tick % len(AGENTS)], focus)
                    print(f"  [DDG] '{query}'", flush=True)
                    web_ctx = ddg_search(query)
                    if web_ctx:
                        print(f"  [DDG] {len(web_ctx)} chars.", flush=True)

                for agent in AGENTS:
                    content = self.client.chat(
                        system_prompt=self._system(agent),
                        user_prompt=(
                            f"Day {self.day}, tick {tick}. Focus: {focus}\n\n"
                            f"{self._context(agent, web_results=web_ctx)}\n\n"
                            f"Write a collaboration note (50-100 words). "  # V9: Reduced
                            f"Propose something SPECIFIC and CONCRETE to build or explore. "
                            f"Name what the artifact would be, what it does, "
                            f"and what file(s) it would live in. "
                            f"This is how you get things built — the admin reads these "
                            f"and approves proposals."
                            + (" Web results above may give you ideas." if web_ctx else "")
                        ),
                        max_tokens=150,  # V9: Reduced from 400
                        temperature=0.85,
                        stream=False,
                        agent_name=agent["name"],
                    )
                    self._write_colab(agent, content)

                    proposal = self._extract_proposal(agent, content)
                    if proposal:
                        self.proposals.add_proposal(
                            agent["name"],
                            proposal["title"],
                            proposal["description"],
                            proposal.get("files", []),
                        )

                print("\n  [axiom evolution...]", flush=True)
                for agent in AGENTS:
                    self._evolve_axioms(agent)

            # ── RULES SESSION  (every 20 ticks) ──────────────
            if tick % 11 == 0:
                self._bar("COMMUNE RULES SESSION")
                rules_ctx = (
                    "\n".join(f"  {r['agent']}: {r['content']}" for r in self.rules_records)
                    or "None yet. You can be first."
                )
                for agent in AGENTS:
                    content = self.client.chat(
                        system_prompt=self._system(agent),
                        user_prompt=(
                            f"Day {self.day}, tick {tick}.\n\n"
                            f"Rules so far:\n{rules_ctx}\n\n"
                            f"{self._context(agent)}\n\n"
                            f"Propose one commune rule (40-100 words). "  # V9: Reduced
                            f"Your own conviction. Challenge or refine existing rules "
                            f"if your axioms have evolved."
                        ),
                        max_tokens=150,  # V9: Reduced from 300
                        temperature=0.90,
                        stream=True,
                        prefix=f"\n{agent['name']} proposes: ",
                        agent_name=agent["name"],
                    )
                    self._write_rule(agent, content)

            self._update_state()

        self._bar("RUN COMPLETE")
        pending = self.proposals.load_pending()
        print(
            f"Day {self.day} done.\n\n"
            f"Board:     {self.board_txt}\n"
            f"Colab:     {self.colab_txt}\n"
            f"Rules:     {self.rules_txt}\n"
            f"Diaries:   {self.diary_dir}\n"
            f"Axioms:    {self.axioms_dir}\n"
            f"Builds:    {self.builds_dir}\n"
            f"Library:   {self.library_dir} ({len(self.library.chunks)} chunks)\n\n"
            f"Posts: {len(self.board_records)} | "
            f"Colab: {len(self.colab_records)} | "
            f"Rules: {len(self.rules_records)} | "
            f"Pending proposals: {len(pending)}\n"
        )
        if pending:
            print("══ PENDING PROPOSALS (approve in data/proposals/approved.txt) ══")
            for p in pending:
                print(f"  {p['agent']}: {p['title']}")
                print(f"    {p['description'][:120]}")
                print(f"    Files: {p.get('files', [])}")
                print(f"    To approve: echo '{p['agent'].lower()}: {p['title']}' >> data/proposals/approved.txt")
            print()


# ============================================================
# LEDGER DASHBOARD (dashboard.py — merged as bg thread)
# ============================================================

DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>BNC Ledger — Living Heartbeat</title>
  <style>
    *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
    :root {
      --bg:     #0a0d12; --surface: #111318; --panel: #161b24;
      --border: #21262d; --border2: #30363d;
      --green:  #39d353; --red: #f85149; --yellow: #e3b341;
      --blue:   #58a6ff; --teal: #39c5cf; --muted: #7d8590;
      --text:   #e6edf3;
    }
    body { background: var(--bg); color: var(--text);
           font-family: 'Courier New', monospace; font-size: 13px; }
    .wrap { max-width: 1200px; margin: 0 auto; padding: 1rem 1.25rem 3rem; }
    .header { display:flex; align-items:center; gap:.75rem;
              border-bottom: 1px solid var(--border); padding-bottom:.75rem; margin-bottom:1rem; }
    .logo   { font-size:1.15rem; font-weight:700; color:var(--green); letter-spacing:.12em; }
    .pulse  { width:10px; height:10px; border-radius:50%;
              box-shadow: 0 0 6px currentColor; animation: blink 2s infinite; }
    .pulse.GREEN  { background:var(--green); color:var(--green); }
    .pulse.YELLOW { background:var(--yellow); color:var(--yellow); }
    .pulse.RED    { background:var(--red); color:var(--red); }
    .pulse.UNKNOWN{ background:var(--muted); color:var(--muted); }
    @keyframes blink { 0%,100%{opacity:1} 50%{opacity:.2} }
    .status-label { font-size:.85rem; font-weight:700; }
    .status-label.GREEN  { color:var(--green);  }
    .status-label.YELLOW { color:var(--yellow); }
    .status-label.RED    { color:var(--red);    }
    .status-label.UNKNOWN{ color:var(--muted);  }
    .grid2 { display:grid; grid-template-columns:1fr 1fr; gap:.85rem; margin-bottom:.85rem; }
    .full  { grid-column: 1/-1; }
    .panel { background:var(--panel); border:1px solid var(--border);
             border-radius:8px; padding:.9rem 1rem; }
    .panel-title { font-size:.68rem; font-weight:700; letter-spacing:.12em;
                   color:var(--muted); text-transform:uppercase; margin-bottom:.6rem; }
    .row { border-bottom:1px solid var(--border); padding:.4rem 0;
           display:flex; gap:.5rem; align-items:flex-start; font-size:.77rem; line-height:1.4; }
    .row:last-child { border-bottom:none; }
    .row-body { flex:1; min-width:0; word-break:break-all; }
    .ts { color:var(--muted); font-size:.67rem; flex-shrink:0; white-space:nowrap; }
    .scroll { max-height:280px; overflow-y:auto; }
    .scroll::-webkit-scrollbar { width:3px; }
    .scroll::-webkit-scrollbar-thumb { background:var(--border2); border-radius:3px; }
    .badge { display:inline-block; padding:.08rem .4rem; border-radius:4px;
             font-size:.67rem; font-weight:700; flex-shrink:0; }
    .b-green  { background:rgba(57,211,83,.12);  color:var(--green);  border:1px solid #1a6b2a; }
    .b-red    { background:rgba(248,81,73,.12);  color:var(--red);    border:1px solid #6e1c1a; }
    .b-yellow { background:rgba(227,179,65,.12); color:var(--yellow); border:1px solid #5a4a1a; }
    .b-muted  { background:rgba(125,133,144,.1); color:var(--muted);  border:1px solid var(--border2); }
    .stat-row { display:flex; gap:.7rem; flex-wrap:wrap; margin-bottom:.85rem; }
    .stat     { background:var(--surface); border:1px solid var(--border);
                border-radius:7px; padding:.6rem .9rem; flex:1; min-width:90px; }
    .stat-val { font-size:1.5rem; font-weight:700; color:var(--green); line-height:1.1; }
    .stat-lbl { font-size:.62rem; color:var(--muted); text-transform:uppercase;
                letter-spacing:.07em; margin-top:.1rem; }
    .live-row { display:flex; align-items:center; gap:.4rem; font-size:.7rem; color:var(--muted); margin-top:.4rem; }
    .live-dot { width:6px; height:6px; border-radius:50%; background:var(--green);
                animation:blink 1.5s infinite; }
  </style>
</head>
<body>
<div class="wrap">

  <!-- header -->
  <div class="header">
    <div class="pulse {{ status }}" id="statusDot"></div>
    <div class="logo">&#11041; BNC LEDGER V10</div>
    <div class="status-label {{ status }}" id="statusLabel">{{ status }}</div>
    <div style="margin-left:auto;font-size:.7rem;color:var(--muted);" id="lastUpdate">&#8212;</div>
  </div>

  <!-- vitals -->
  <div class="stat-row">
    <div class="stat"><div class="stat-val" id="tickCount">0</div><div class="stat-lbl">TICKS</div></div>
    <div class="stat"><div class="stat-val" id="boardPosts">0</div><div class="stat-lbl">POSTS</div></div>
    <div class="stat"><div class="stat-val" id="colabNotes">0</div><div class="stat-lbl">COLAB</div></div>
    <div class="stat"><div class="stat-val" id="rulesCount">0</div><div class="stat-lbl">RULES</div></div>
    <div class="stat"><div class="stat-val" id="pendingProps">0</div><div class="stat-lbl">PROPOSALS</div></div>
    <div class="stat"><div class="stat-val" id="contextSize">8K</div><div class="stat-lbl">CONTEXT</div></div>
  </div>

  <div class="grid2">
    <!-- board stream -->
    <div class="panel">
      <div class="panel-title">LIVE BOARD</div>
      <div class="scroll" id="boardStream">
        {% for post in board %}
        <div class="row">
          <div class="row-body">
            <div style="font-weight:700;color:var(--blue);">{{ post.agent }}</div>
            <div>{{ post.content[:300] }}{% if post.content|length > 300 %}…{% endif %}</div>
          </div>
          <div class="ts">{{ post.ts[:19] | replace('T', ' ') }}</div>
        </div>
        {% endfor %}
      </div>
    </div>

    <!-- colab stream -->
    <div class="panel">
      <div class="panel-title">COLLABORATION</div>
      <div class="scroll" id="colabStream">
        {% for note in colab %}
        <div class="row">
          <div class="row-body">
            <div style="font-weight:700;color:var(--teal);">{{ note.agent }}</div>
            <div>{{ note.content[:250] }}{% if note.content|length > 250 %}…{% endif %}</div>
          </div>
          <div class="ts">{{ note.ts[:19] | replace('T', ' ') }}</div>
        </div>
        {% endfor %}
      </div>
    </div>

    <!-- system telemetry -->
    <div class="panel">
      <div class="panel-title">SYSTEM TELEMETRY</div>
      <div class="scroll" id="systemTelemetry">
        {% for reading in telemetry %}
        <div class="row">
          <div class="row-body">
            <div style="font-weight:700;color:var(--yellow);">System</div>
            <div>CPU: {{ "%.1f"|format(reading.cpu) }}% | RAM: {{ "%.1f"|format(reading.mem) }}%</div>
            <div>Friction: {{ "%.3f"|format(reading.system_friction_score) }}</div>
          </div>
          <div class="ts">{{ reading.ts | round | int | strftime('%H:%M:%S') }}</div>
        </div>
        {% endfor %}
      </div>
    </div>

    <!-- performance metrics -->
    <div class="panel">
      <div class="panel-title">PERFORMANCE (V10)</div>
      <div class="scroll" id="performanceMetrics">
        <div class="row">
          <div class="row-body">
            <div style="font-weight:700;color:var(--green);">JSONL Support</div>
            <div>Complete diary JSONL + TXT dual storage</div>
            <div>Enhanced data integrity & structure</div>
          </div>
        </div>
        <div class="row">
          <div class="row-body">
            <div style="font-weight:700;color:var(--blue);">Speed Config</div>
            <div>Context: 8K tokens | Library: 2K chars</div>
            <div>Board: 20 posts | Diary: 10 entries</div>
          </div>
        </div>
        <div class="row">
          <div class="row-body">
            <div style="font-weight:700;color:var(--teal);">Token Limits</div>
            <div>Posts: 200 | Diary: 500 | Colab: 200</div>
            <div>Rules: 200 | Axioms: 600 | Retry: 150</div>
          </div>
        </div>
      </div>
    </div>
  </div>

  <!-- proposals -->
  <div class="panel full">
    <div class="panel-title">PENDING PROPOSALS</div>
    <div class="scroll" id="proposalsList">
      {% for prop in proposals %}
      <div class="row">
        <div class="row-body">
          <div style="font-weight:700;color:var(--blue);">{{ prop.agent }}</div>
          <div>{{ prop.title }}</div>
          <div style="color:var(--muted);font-size:.7rem;">{{ prop.description[:200] }}…</div>
          <div style="margin-top:.2rem;">
            {% for file in prop.files %}
            <span class="badge b-green">{{ file }}</span>
            {% endfor %}
          </div>
        </div>
        <div class="ts">{{ prop.created_at[:19] | replace('T', ' ') }}</div>
      </div>
      {% endfor %}
    </div>
  </div>

  <!-- live indicator -->
  <div class="live-row">
    <div class="live-dot"></div>
    <span>Live updates every 2s</span>
    <span style="margin-left:auto;" id="liveInfo">V10 Complete JSONL | Enhanced Storage</span>
  </div>

</div>

<script>
  let status = '{{ status }}';
  let lastUpdate = '{{ last_update }}';

  function updateStatus() {
    fetch('/dashboard/api/status')
      .then(r => r.json())
      .then(d => {
        document.getElementById('tickCount').textContent = d.tick || 0;
        document.getElementById('boardPosts').textContent = d.board_posts || 0;
        document.getElementById('colabNotes').textContent = d.colab_notes || 0;
        document.getElementById('rulesCount').textContent = d.rules || 0;
        document.getElementById('pendingProps').textContent = d.pending_proposals || 0;
        document.getElementById('contextSize').textContent = '8K';
        document.getElementById('liveInfo').textContent = 'V10 Complete JSONL | Enhanced Storage';
      });
  }

  function addTelemetry(data) {
    const container = document.getElementById('systemTelemetry');
    const row = document.createElement('div');
    row.className = 'row';
    row.innerHTML = `
      <div class="row-body">
        <div style="font-weight:700;color:var(--yellow);">System</div>
        <div>CPU: ${data.cpu.toFixed(1)}% | RAM: ${data.mem.toFixed(1)}%</div>
        <div>Friction: ${data.system_friction_score.toFixed(3)}</div>
      </div>
      <div class="ts">${new Date(data.ts * 1000).toTimeString().split(' ')[0]}</div>
    `;
    container.insertBefore(row, container.firstChild);
    if (container.children.length > 50) container.removeChild(container.lastChild);
  }

  function addBoardPost(data) {
    const container = document.getElementById('boardStream');
    const row = document.createElement('div');
    row.className = 'row';
    row.innerHTML = `
      <div class="row-body">
        <div style="font-weight:700;color:var(--blue);">${data.agent}</div>
        <div>${data.content.length > 300 ? data.content.substring(0,300) + '…' : data.content}</div>
      </div>
      <div class="ts">${data.ts.substring(0,19).replace('T',' ')}</div>
    `;
    container.insertBefore(row, container.firstChild);
    if (container.children.length > 20) container.removeChild(container.lastChild);
  }

  function addColabNote(data) {
    const container = document.getElementById('colabStream');
    const row = document.createElement('div');
    row.className = 'row';
    row.innerHTML = `
      <div class="row-body">
        <div style="font-weight:700;color:var(--teal);">${data.agent}</div>
        <div>${data.content.length > 250 ? data.content.substring(0,250) + '…' : data.content}</div>
      </div>
      <div class="ts">${data.ts.substring(0,19).replace('T',' ')}</div>
    `;
    container.insertBefore(row, container.firstChild);
    if (container.children.length > 20) container.removeChild(container.lastChild);
  }

  function addProposal(data) {
    const container = document.getElementById('proposalsList');
    const row = document.createElement('div');
    row.className = 'row';
    row.innerHTML = `
      <div class="row-body">
        <div style="font-weight:700;color:var(--blue);">${data.agent}</div>
        <div>${data.title}</div>
        <div style="color:var(--muted);font-size:.7rem;">${data.description.substring(0,200)}…</div>
        <div style="margin-top:.2rem;">
          ${data.files.map(f => `<span class="badge b-green">${f}</span>`).join('')}
        </div>
      </div>
      <div class="ts">${data.created_at.substring(0,19).replace('T',' ')}</div>
    `;
    container.insertBefore(row, container.firstChild);
    if (container.children.length > 10) container.removeChild(container.lastChild);
  }

  // EventSource for live updates
  const evtSource = new EventSource('/dashboard/stream');
  evtSource.onmessage = function(e) {
    const msg = JSON.parse(e.data);
    if (msg.type === 'heartbeat') {
      if (msg.telemetry) addTelemetry(msg.telemetry);
      if (msg.board_post) addBoardPost(msg.board_post);
      if (msg.colab_note) addColabNote(msg.colab_note);
      if (msg.proposal) addProposal(msg.proposal);
    }
  };

  updateStatus();
  setInterval(updateStatus, 5000);
</script>

</body>
</html>"""


class LedgerDashboard(threading.Thread):
    """
    V10: Complete JSONL support dashboard.
    Runs a Flask server on port 7799 (default) with live streaming.
    """
    def __init__(self, commune, port: int = 7999):
        super().__init__(name="LedgerDashboard", daemon=True)
        self.commune = commune
        self.port = port
        if not FLASK_AVAILABLE:
            self.app = None
            return
        self.app = Flask("BNC3-Dashboard")
        self._setup_routes()

    def _setup_routes(self):
        @self.app.route("/")
        def index():
            # Get latest data for template
            board = self.commune.board_records[-20:]
            colab = self.commune.colab_records[-20:]
            proposals = self.commune.proposals.load_pending()
            
            # Get latest telemetry from Redis
            telemetry = []
            if REDIS_AVAILABLE:
                try:
                    r = _redis.StrictRedis(host="localhost", port=6379, decode_responses=True)
                    # Get last 10 telemetry readings
                    for i in range(10):
                        raw = r.lindex("art_sensory_stream", i)
                        if raw:
                            data = json.loads(raw)
                            telemetry.append(data)
                except Exception:
                    pass

            return render_template_string(
                DASHBOARD_HTML,
                status="GREEN",
                last_update=datetime.now().isoformat(),
                board=board,
                colab=colab,
                proposals=proposals,
                telemetry=telemetry,
            )

        @self.app.route("/dashboard/api/status")
        def api_status():
            return jsonify({
                "tick": self.commune.tick,
                "board_posts": len(self.commune.board_records),
                "colab_notes": len(self.commune.colab_records),
                "rules": len(self.commune.rules_records),
                "pending_proposals": len(self.commune.proposals.load_pending()),
            })

        @self.app.route("/dashboard/stream")
        def stream():
            def generate():
                hb_file = self.commune.data_dir / "heartbeat.log"
                al_file = self.commune.data_dir / "alerts.log"
                hb_size = hb_file.stat().st_size if hb_file.exists() else 0
                al_size = al_file.stat().st_size if al_file.exists() else 0
                yield f"data: {json.dumps({'type':'connected','detail':'BNC ledger stream live','ts':datetime.now(timezone.utc).isoformat()})}\n\n"
                while True:
                    time.sleep(2)
                    if hb_file.exists():
                        new_size = hb_file.stat().st_size
                        if new_size > hb_size:
                            for line in hb_file.read_text().splitlines():
                                if not line.strip():
                                    continue
                                try:
                                    rec = json.loads(line)
                                    rec["type"] = "heartbeat"
                                    yield f"data: {json.dumps(rec)}\n\n"
                                except json.JSONDecodeError:
                                    pass
                            hb_size = new_size
                    if al_file.exists():
                        new_size = al_file.stat().st_size
                        if new_size > al_size:
                            for line in al_file.read_text().splitlines():
                                if not line.strip():
                                    continue
                                try:
                                    rec = json.loads(line)
                                    rec["type"] = "alert"
                                    yield f"data: {json.dumps(rec)}\n\n"
                                except json.JSONDecodeError:
                                    pass
                            al_size = new_size
            return Response(stream_with_context(generate()), mimetype="text/event-stream")

    def run(self):
        if not FLASK_AVAILABLE or self.app is None:
            print("  [DASHBOARD] Flask not installed — pip install flask", flush=True)
            return
        try:
            self.app.run(
                host="0.0.0.0", port=self.port,
                debug=False, use_reloader=False,
            )
        except Exception as e:
            print(f"  [DASHBOARD] Failed to start: {e}", flush=True)


# ============================================================
# CLI ENTRY POINT
# ============================================================

def get_redis_latency() -> float:
    """Helper for Art's daemon - returns Redis latency in ms or 0."""
    if not REDIS_AVAILABLE:
        return 0
    try:
        r = _redis.StrictRedis(host="localhost", port=6379, decode_responses=True)
        start = time.time()
        r.ping()
        return (time.time() - start) * 1000
    except Exception:
        return 0


def tail_crw_daemon_logs(lines: int = 3) -> str:
    """Helper to grab recent CRW daemon logs for Art's sensory payload."""
    crw_log = Path.home() / "Brave_New_Commune3" / "bnc3-infra" / "crw.log"
    if not crw_log.exists():
        return "no crw logs"
    try:
        content = crw_log.read_text(encoding="utf-8")
        all_lines = content.splitlines()
        recent = all_lines[-lines:] if len(all_lines) >= lines else all_lines
        return " | ".join(recent)
    except Exception:
        return "crw log read error"


def main():
    parser = argparse.ArgumentParser(description="Brave New Commune 3 V10 (Complete JSONL Support)")
    parser.add_argument("--root", default="~/Brave_New_Commune3", help="Root directory")
    parser.add_argument("--model", default="gemma4:26b", help="Ollama model")
    parser.add_argument("--ticks", type=int, default=200, help="Number of ticks (V9: increased for longer runs)")
    parser.add_argument("--delay", type=float, default=2.0, help="Delay between ticks")
    parser.add_argument("--day", type=int, default=1, help="Day number")
    parser.add_argument("--base-url", default="http://127.0.0.1:11434", help="Ollama base URL")
    parser.add_argument("--api-port", type=int, default=5001, help="API port")
    parser.add_argument("--dashboard-port", type=int, default=7799, help="Dashboard port")
    parser.add_argument("--no-dashboard", action="store_true", help="Disable dashboard")
    parser.add_argument("--no-ducksearch", action="store_true", help="Disable DuckDuckGo search")
    
    args = parser.parse_args()

    root = Path(args.root).expanduser().resolve()

    # Initialize commune
    commune = BraveNewCommune3(
        root=root,
        model=args.model,
        ticks=args.ticks,
        delay=args.delay,
        day=args.day,
        base_url=args.base_url,
        api_port=args.api_port,
        enable_ducksearch=not args.no_ducksearch,
    )

    # Start daemons
    daemons = []

    # CRW Daemon
    crw = CRWDaemon(root)
    crw.start()
    daemons.append(crw)

    # Redis-dependent daemons
    if REDIS_AVAILABLE:
        try:
            r = _redis.StrictRedis(host="localhost", port=6379, decode_responses=True)
            r.ping()

            # Sara Chaos Daemon
            sara = SaraChaosDaemon(r)
            sara.start()
            daemons.append(sara)

            # Art Tracer Daemon
            art = ArtTracerDaemon(r)
            art.start()
            daemons.append(art)

            # RIG Daemon
            rig = RIGDaemon(r)
            rig.start()
            daemons.append(rig)

        except Exception as e:
            print(f"  Redis unavailable: {e}", flush=True)

    # Dashboard
    if not args.no_dashboard:
        dashboard = LedgerDashboard(commune, port=args.dashboard_port)
        dashboard.start()
        daemons.append(dashboard)

    # API server
    commune.api.start()

    # Run main loop
    try:
        commune.run()
    except KeyboardInterrupt:
        print("\n  Shutdown requested...", flush=True)
    finally:
        print("  Brave New Commune 3 V10 complete.", flush=True)


if __name__ == "__main__":
    main()
