#!/usr/bin/env python3
"""
Flask YouTube downloader — v4.2  (2025-08-03)
─────────────────────────────────────────────
* Exactly **one** active download per IP (FIFO).
* **100** concurrent downloads server-wide.
* Unlimited global queue depth.
* Per-IP sliding-window limiter: ≤ 6 URLs / min.
* 24-hour finished-video cache prevents duplicate spam.
"""

from __future__ import annotations

import os, re, unicodedata, uuid, shutil, time, threading, concurrent.futures
from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone

from flask import Flask, render_template, request, jsonify, send_file, Response
from yt_dlp import YoutubeDL, utils as ytdl_utils
from apscheduler.schedulers.background import BackgroundScheduler

# ───────── configurable limits ──────────────────────────────────
BASE_DIR                  = os.path.dirname(__file__)

# ─── persistent download directory detection ───────────────────────────
from pathlib import Path
BASE_DIR = Path(__file__).resolve().parent

def _detect_dl_dir() -> Path:
    """Return a persistent directory for video files.

    Priority:
      1. Explicit DL_DIR env var (always wins)
      2. Railway-attached volume mount path
      3. Hard-coded /data fallback when running on Railway
      4. Local dev: <project>/downloads
    """
    # 1. Let the operator override everything
    if os.getenv("DL_DIR"):
        return Path(os.getenv("DL_DIR"))

    # 2. Auto-detect Railway volume mount (if you added one)
    if os.getenv("RAILWAY_VOLUME_MOUNT_PATH"):
        return Path(os.getenv("RAILWAY_VOLUME_MOUNT_PATH"))

    # 3. Any other Railway container (no volume attached yet)
    if os.getenv("RAILWAY_PROJECT_ID"):
        return Path("/data")           # matches Railway’s default UI suggestion

    # 4. Fall back to a local folder next to the code
    return BASE_DIR / "downloads"

DL_DIR = _detect_dl_dir()
DL_DIR.mkdir(parents=True, exist_ok=True)

MAX_ACTIVE_PER_IP         = 1      # sequential per IP
MAX_DOWNLOADS_GLOBAL      = 100    # concurrent workers
MAX_REQUESTS_PER_IP_MIN   = 6      # sliding-window URLs / minute
MAX_URLS_PER_REQUEST      = 20
MAX_QUEUE_PER_IP          = 50
REQUEST_WINDOW            = timedelta(minutes=1)
JOB_TTL                   = timedelta(hours=24)
COMPLETED_CACHE_TTL       = timedelta(hours=24)   # block duplicate IDs this long

TRUST_PROXY = [p.strip() for p in os.getenv("TRUST_PROXY", "").split(",") if p.strip()]

# ───────── globals ──────────────────────────────────────────────
app = Flask(__name__)

jobs: dict[str, dict]                               = {}            # job_id → data
url_map: dict[str, str | None]                      = {}            # video_id → job_id / "cached"
completed_cache: dict[str, datetime]                = {}            # video_id → finished_ts
ip_queues: defaultdict[str, deque[str]]             = defaultdict(deque)
ip_active: dict[str, str]                           = {}            # ip → job_id
ip_recent: defaultdict[str, deque[datetime]]        = defaultdict(deque)
active_dl_set: set[str]                             = set()

LOCK = threading.Lock()
EXECUTOR = concurrent.futures.ThreadPoolExecutor(max_workers=MAX_DOWNLOADS_GLOBAL)

# ───────── helpers ──────────────────────────────────────────────
def ffmpeg_ok() -> bool:
    return shutil.which("ffmpeg") is not None

def clean_filename(title: str, vid: str, max_len: int = 80) -> str:
    s = unicodedata.normalize("NFC", title)
    s = re.sub(r'[\\/*?:"<>|]', "", s).strip() or vid
    return f"{s[:max_len].rstrip() or vid}.mp4"

def client_ip() -> str:
    if request.remote_addr in TRUST_PROXY and "X-Forwarded-For" in request.headers:
        return request.headers["X-Forwarded-For"].split(",")[0].strip()
    return request.remote_addr or "unknown"

# ── YouTube validation & canonicalisation ───────────────────────
YOUTUBE_RE = re.compile(
    r"^(https?://)?(www\.)?(youtube\.com/watch\?v=|youtu\.be/)([\w-]{11})(?:[&#?].*)?$",
    re.IGNORECASE,
)

def is_youtube_url(u: str) -> bool:
    return bool(YOUTUBE_RE.match(u.strip()))

def canonical(u: str) -> str:
    m = YOUTUBE_RE.match(u.strip())
    return m.group(4).lower() if m else ""

# ── sliding-window limiter (counts URLs) ────────────────────────
def can_queue(ip: str, num_urls: int) -> bool:
    now = datetime.now(timezone.utc)
    dq = ip_recent[ip]
    while dq and now - dq[0] > REQUEST_WINDOW:
        dq.popleft()
    if len(dq) + num_urls > MAX_REQUESTS_PER_IP_MIN:
        return False
    dq.extend([now] * num_urls)
    return True

# ───────── queue scheduling ────────────────────────────────────
def launch_if_possible(ip: str) -> None:
    with LOCK:
        if ip in ip_active or not ip_queues[ip] or len(active_dl_set) >= MAX_DOWNLOADS_GLOBAL:
            return
        job_id = ip_queues[ip].popleft()
        ip_active[ip] = job_id
        active_dl_set.add(job_id)
        jobs[job_id]["status"] = "downloading"
    EXECUTOR.submit(download_job, job_id, jobs[job_id]["url"], ip)

# ───────── download worker ─────────────────────────────────────
def download_job(job_id: str, url: str, ip: str) -> None:
    outtmpl = os.path.join(DL_DIR, f"{job_id}.%(ext)s")
    ydl_opts = {
        "format": "bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best",
        "merge_output_format": "mp4",
        "outtmpl": outtmpl,
        "quiet": True,
        "no_warnings": True,
    }
    j = jobs[job_id]
    if not ffmpeg_ok():
        j.update(status="error", error="ffmpeg not found")
        _cleanup_job_state(ip, job_id)
        return
    try:
        with YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=True)
        if j.get("cancelled"):
            raise ytdl_utils.DownloadError("cancelled")
        title = info.get("title") or ""
        vid = info.get("id") or job_id
        j.update(status="finished", file=f"{job_id}.mp4", name=clean_filename(title, vid))
    except ytdl_utils.DownloadError as e:
        j.update(status="cancelled" if "cancelled" in str(e) else "error", error=str(e))
        for ext in (".mp4", ".m4a", ".part"):
            try:
                os.remove(outtmpl % {"ext": ext.lstrip(".")})
            except FileNotFoundError:
                pass
    except Exception as e:
        j.update(status="error", error=str(e))
    finally:
        _cleanup_job_state(ip, job_id)
        launch_if_possible(ip)

def _cleanup_job_state(ip: str, job_id: str) -> None:
    with LOCK:
        ip_active.pop(ip, None)
        active_dl_set.discard(job_id)
        status = jobs[job_id]["status"]
        for vid, jid in list(url_map.items()):
            if jid == job_id:
                if status == "finished":
                    completed_cache[vid] = datetime.now(timezone.utc)
                    url_map[vid] = "cached"
                else:
                    url_map.pop(vid, None)

# ───────── periodic maintenance ────────────────────────────────
def clean_files() -> None:
    cutoff = datetime.now(timezone.utc) - JOB_TTL
    for fn in os.listdir(DL_DIR):
        fp = os.path.join(DL_DIR, fn)
        try:
            if os.path.isfile(fp) and datetime.fromtimestamp(os.path.getmtime(fp), timezone.utc) < cutoff:
                os.remove(fp)
        except OSError:
            pass

def purge_job_table() -> None:
    cutoff = datetime.now(timezone.utc) - JOB_TTL
    with LOCK:
        for jid, j in list(jobs.items()):
            if j.get("created") and j["created"] < cutoff and j["status"] in {"finished", "error", "cancelled", "invalid"}:
                jobs.pop(jid, None)
        valid_ids = set(jobs.keys())
        for vid, jid in list(url_map.items()):
            if jid not in valid_ids and jid != "cached":
                url_map.pop(vid, None)

def purge_completed_cache() -> None:
    cutoff = datetime.now(timezone.utc) - COMPLETED_CACHE_TTL
    with LOCK:
        for vid, ts in list(completed_cache.items()):
            if ts < cutoff:
                completed_cache.pop(vid, None)
                url_map.pop(vid, None)

scheduler = BackgroundScheduler(daemon=True)
scheduler.add_job(clean_files,          "cron", hour=0, minute=0)   # nightly
scheduler.add_job(purge_job_table,      "cron", hour="*")           # hourly
scheduler.add_job(purge_completed_cache,"cron", minute="*/30")      # every 30 min
scheduler.start()

# ───────── routes ──────────────────────────────────────────────
@app.route("/")
def index():
    return render_template("index.html")

@app.route("/start", methods=["POST"])
def start():
    raw  = (request.json.get("urls") or "").strip()
    urls = [u for u in re.split(r"[,\s]+", raw) if u]

    if not urls:
        return jsonify({"error": "No URL provided"}), 400
    if len(urls) > MAX_URLS_PER_REQUEST:
        return jsonify({"error": f"Max {MAX_URLS_PER_REQUEST} URLs per request"}), 400

    ip = client_ip()
    if not can_queue(ip, len(urls)):
        return jsonify({"error": f"Rate limit: {MAX_REQUESTS_PER_IP_MIN} URLs/min"}), 429
    if len(ip_queues[ip]) + len(urls) > MAX_QUEUE_PER_IP:
        return jsonify({"error": f"Per-IP queue limit {MAX_QUEUE_PER_IP} exceeded"}), 429

    resp: dict[str, str | dict] = {}
    now = datetime.now(timezone.utc)

    with LOCK:
        for url in urls:
            if not is_youtube_url(url):
                resp[url] = {"error": "Not a valid YouTube link"}
                continue
            vid = canonical(url)

            # duplicate in-flight or cached?
            if vid in url_map:
                resp[url] = url_map[vid]      # job_id or "cached"
                continue
            # duplicate recently finished?
            if vid in completed_cache and now - completed_cache[vid] < COMPLETED_CACHE_TTL:
                url_map[vid] = "cached"
                resp[url] = "cached"
                continue

            # create new job
            job_id = str(uuid.uuid4())
            url_map[vid] = job_id
            jobs[job_id] = {
                "status": "queued",
                "url": url,
                "ip": ip,
                "created": now,
            }
            ip_queues[ip].append(job_id)
            resp[url] = job_id

    launch_if_possible(ip)
    return jsonify(resp), 202

@app.route("/status/<job_id>")
def status(job_id):
    return jsonify(jobs.get(job_id, {"status": "invalid"}))

@app.route("/file/<job_id>")
def file(job_id):
    job = jobs.get(job_id)
    if not job or job.get("status") != "finished":
        return "Not ready", 404
    path = os.path.join(DL_DIR, job["file"])
    if not os.path.exists(path):
        return "Missing file", 404

    resp: Response = send_file(
        path,
        as_attachment=True,
        download_name=job.get("name", "video.mp4"),
        mimetype="video/mp4",
    )

    @resp.call_on_close
    def _auto_delete():
        threading.Thread(target=_delete_with_retries, args=(path, job_id), daemon=True).start()
    return resp

def _delete_with_retries(p: str, job_id: str, retries: int = 6, delay: float = 1.5) -> None:
    for _ in range(retries):
        try:
            os.remove(p)
            break
        except PermissionError:
            time.sleep(delay)
        except FileNotFoundError:
            break
    jobs.pop(job_id, None)

@app.route("/cancel_all", methods=["POST"])
def cancel_all():
    ip = client_ip()
    with LOCK:
        for jid in list(ip_queues[ip]):          # queued
            ip_queues[ip].remove(jid)
            jobs[jid]["status"] = "cancelled"
            url_map.pop(canonical(jobs[jid]["url"]), None)
        jid = ip_active.get(ip)                  # active
        if jid:
            jobs[jid]["cancelled"] = True
    return "", 204

@app.route("/info")
def info():
    ip = client_ip()
    with LOCK:
        return jsonify({
            "your_active"  : 1 if ip in ip_active else 0,
            "your_queue"   : len(ip_queues[ip]),
            "global_active": len(active_dl_set),
            "global_queue" : sum(len(q) for q in ip_queues.values()),
        })

if __name__ == "__main__":
    app.run(port=int(os.getenv("PORT", 8989)), debug=False, use_reloader=False)
