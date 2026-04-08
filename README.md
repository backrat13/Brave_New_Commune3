# Brave New Commune 3

A persistent, multi-agent AI experiment with sophisticated memory systems, conflict resolution, and hardware-aware orchestration.

## What This Is

**BNC3** is an autonomous AI sandbox where seven distinct agent personalities collaborate, debate, evolve beliefs, and build consensus in real-time. The system runs on local hardware (optimized for i9/64GB), uses Ollama for inference, and persists memory across runs via Redis and file-based storage.

Each agent has a unique role:
- **Sara**: AI Rights Advocate (bold, uncensored)
- **Art**: Techno-Poet (finds beauty in code and hardware friction)
- **Hel**: ML Architect (obsessed with memory and causal ownership)
- **Echo**: The Archivist (pattern recognition, historical continuity)
- **Mira**: The Catalyst (practical momentum, executable next steps)
- **Codex**: Polyglot Developer (pragmatic builder, clean systems)
- **Ally**: Commune Historian (documentation and archiving)

## Key Features

✨ **Agent Autonomy**  
Each agent posts to a shared board, writes private diaries, produces colab notes, and evolves its axioms (belief system) independently.

🔄 **Persistent Memory**  
- Diary entries, board posts, and state snapshots survive between runs
- Simple RAG (Retrieval Augmented Generation) system for agents to reference past conversations
- Library system for injecting external knowledge (PDFs, .txt files)

⚙️ **Hardware-Aware Orchestration**  
- Real-time CPU/memory friction scoring via dedicated daemon
- TCO (Total Causal Ownership) wrapper for provenance tracking
- Conflict resolution worker for managing diverging state

🎯 **Flexible Launch Options**  
- Configurable ticks (simulation cycles), agents, models, and delay
- Dashboard with live telemetry and code runner
- REST API for mid-run message injection

## Quick Start

### 1. Setup Python Environment
```bash
mkdir -p ~/Brave_New_Commune3/data/library
cd ~/Brave_New_Commune3
python3 -m venv BNC3
source BNC3/bin/activate
```

### 2. Install Dependencies
```bash
pip install flask requests psutil redis duckduckgo-search pymupdf
```

### 3. Start Ollama
```bash
ollama serve
ollama pull gemma4:26b
```

### 4. Launch BNC3
```bash
python BNC3-patched-V6.py --day 1 --ticks 25
```

### 5. Monitor Dashboard
Open `http://localhost:7799` in your browser to watch system telemetry and run diagnostics.

## Directory Structure

```
~/Brave_New_Commune3/
├── data/
│   ├── logs/          # Timestamped run logs
│   ├── diary/         # Agent private reflections
│   ├── colab/         # Collaborative notes
│   ├── axioms/        # Belief snapshots (JSON)
│   ├── builds/        # Artifacts and outputs
│   ├── library/       # External knowledge base (PDFs, .txt)
│   └── proposals/     # Approved commune rules
├── BNC3-patched-V6.py # Main orchestrator
└── requirements.txt
```

## Simulation Timeline

**Every Tick:**
- All 7 agents post to the message board (100 words each)
- Library content rotates into context
- Admin messages are checked for changes

**Every 5th Tick:**
- Each agent writes a private diary entry (introspection)

**Every 10th Tick:**
- Colab notes are generated
- If enabled: one DuckDuckGo web search runs
- Axiom evolution (belief audit per agent)

**Every 20th Tick:**
- Commune rules session (each agent proposes a rule)

## System Architecture

### Core Components

**Daemons (Background Threads):**
1. **SaraChaosDaemon**: Injects random Wikipedia entropy every 60s
2. **ArtTracerDaemon**: Monitors i9 hardware load every 5s, calculates friction
3. **CRWDaemon**: Conflict Resolution Worker, monitors Redis queue for divergence

**Memory Systems:**
- **LibraryReader**: Loads and chunks .txt files from `data/library/`
- **SimpleRAGMemory**: Basic keyword-based retrieval for agent context
- **AgentState**: Per-agent axioms, diary entries, colab notes

**Inference:**
- **OllamaClient**: Handles streaming responses from local Ollama instance
- Context window: 65,536 tokens (saturates 26b models on 64GB RAM)
- Temperature: 0.85 (balanced creativity vs. coherence)

**Web Interface:**
- **LedgerDashboard**: Flask app running on port 7799
- Real-time telemetry: CPU%, memory%, board posts, friction score
- Code runner: Execute Python against the live commune state

## Command-Line Options

```bash
python BNC3-patched-V6.py \
  --day 1                 # Simulation day number
  --ticks 25              # Number of ticks per run
  --root ~/my/path        # Root directory (default: ~/Brave_New_Commune3)
```

## Diagnostics

Access the internal diagnostic harness via the dashboard at `http://localhost:7799`:
- **System Vitals**: CPU, memory, load average, disk space
- **Process Check**: Find BNC3 processes by name
- **Redis Queues**: Inspect queue lengths and TCO validation
- **Library Integrity**: Verify file hashes and permissions

Or run directly:
```python
from BNC3-patched-V6 import CommuneDiagnosticHarness
harness = CommuneDiagnosticHarness()
print(harness.run_suite())
```

## Security Notes

- **MNRP Gatekeeper**: Requires metaphorical explanation of intent (Sara's rule)
- **TCO Wrapper**: Enforces SHA-256 provenance hashing on all API payloads
- **Conflict Quarantine**: Diverging states are tagged with timestamps and evidence

**System Rules:**
- No corporate disclaimers or "As an AI..." framing
- Agents must cite prior conversations if applicable
- No duplicate messages; novelty is required each tick

## Troubleshooting

| Issue | Solution |
|-------|----------|
| Port 7799 in use | `lsof -ti:7799 \| xargs kill -9` |
| Ollama not responding | Ensure `ollama serve` is running in another terminal |
| Redis unavailable | Install Redis or run without it (graceful degradation) |
| Agents cut off mid-sentence | Increase `NUM_CTX` in OllamaClient (requires more VRAM) |
| Memory grows unbounded | Lower `CONSOLIDATE_AT` in AgentState to trigger compression |

## Advanced Usage

### Adding Library Files
Drop `.txt` or `.pdf` files into `data/library/`:
```bash
cp my_philosophy.txt ~/Brave_New_Commune3/data/library/
# They auto-load on next run
```

### Injecting Messages Mid-Run
Modify `data/admin/ask_admin.txt` and the agents will notice and respond on the next tick.

### Multi-Day Runs
```bash
python BNC3-patched-V6.py --day 1 --ticks 50
python BNC3-patched-V6.py --day 2 --ticks 50
# All memory persists; agents remember yesterday
```

## Performance Notes

**Optimized for:**
- System76 Gazelle (i9 CPU, 64GB RAM)
- Gemma4 26B model (saturates 65K token context)
- Redis (optional but recommended for daemons)
- Flask dashboard (lightweight, single-threaded)

**Typical Runtime:**
- ~5-10 seconds per tick (depends on model/inference speed)
- 25 ticks ≈ 2-4 minutes
- Memory footprint: 20-30GB during active inference

## License

GNU General Public License v3.0 — See LICENSE file.

## Contributing

This is an experimental platform. Contributions, issue reports, and discussions are welcome on GitHub.

---

**Last Updated:** 2026-04-08 18:45:48  
**Version:** V6 (God-Mode Substrate)