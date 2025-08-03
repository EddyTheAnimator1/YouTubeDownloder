#!/usr/bin/env python3
"""
Flask YouTube downloader — v4.3  (2025-08-03)

* One active download per IP (FIFO)
* 100 concurrent downloads server-wide
* 6 URLs / minute sliding-window IP limiter
* 24-hour finished cache prevents duplicate spam
* Optional “audio only” mode (m4a)
* Files are removed immediately after they finish streaming
"""

from __future__ import annotations
import os, re, unicodedata, uuid, shutil, time, threading, concurrent.futures
from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone
from pathlib import Path

from flask import (
    Flask, render_template, request, jsonify, Response,
    send_file, stream_with_context,
)
from yt_dlp import YoutubeDL, utils as ytdl_utils, DownloadError
from apscheduler.schedulers.background import BackgroundScheduler

# ───────────────────────────────────────────────────────────────
# configurable paths & limits
# ───────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent

def _detect_dl_dir() -> Path:
    if os.getenv("DL_DIR"):
        return Path(os.getenv("DL_DIR"))
    if os.getenv("RAILWAY_VOLUME_MOUNT_PATH"):
        return Path(os.getenv("RAILWAY_VOLUME_MOUNT_PATH"))
    if os.getenv("RAILWAY_PROJECT_ID"):
        return Path("/data")
    return BASE_DIR / "downloads"

DL_DIR                      = _detect_dl_dir(); DL_DIR.mkdir(parents=True, exist_ok=True)
MAX_ACTIVE_PER_IP           = 1
MAX_DOWNLOADS_GLOBAL        = 100
MAX_REQUESTS_PER_IP_MIN     = 6
MAX_URLS_PER_REQUEST        = 20
MAX_QUEUE_PER_IP            = 50
REQUEST_WINDOW              = timedelta(minutes=1)
JOB_TTL                     = timedelta(hours=24)
COMPLETED_CACHE_TTL         = timedelta(hours=24)
TRUST_PROXY                 = [p.strip() for p in os.getenv("TRUST_PROXY", "").split(",") if p.strip()]

# ───────────────────────────────────────────────────────────────
# globals
# ───────────────────────────────────────────────────────────────
app         = Flask(__name__)
jobs        : dict[str, dict]        = {}            # job_id -> job data
url_map     : dict[str, str|None]    = {}            # <vid>|a/v -> job_id / "cached"
completed_cache: dict[str, datetime] = {}            # same key -> ts
ip_queues   : defaultdict[str, deque[str]] = defaultdict(deque)
ip_active   : dict[str, str]         = {}            # ip -> job_id
ip_recent   : defaultdict[str, deque[datetime]] = defaultdict(deque)
active_dl_set: set[str]              = set()

LOCK = threading.Lock()
EXECUTOR = concurrent.futures.ThreadPoolExecutor(max_workers=MAX_DOWNLOADS_GLOBAL)

# ───────────────────────────────────────────────────────────────
# helpers
# ───────────────────────────────────────────────────────────────
YOUTUBE_RE = re.compile(
    r"^(https?://)?(www\.)?(youtube\.com/watch\?v=|youtu\.be/)([\w-]{11})(?:[&#?].*)?$",
    re.IGNORECASE,
)

def ffmpeg_ok() -> bool:
    return shutil.which("ffmpeg") is not None

def clean_filename(title: str, vid: str, ext: str, max_len: int = 80) -> str:
    s = unicodedata.normalize("NFC", title)
    s = re.sub(r'[\\/*?:"<>|]', "", s).strip() or vid
    base = s[:max_len].rstrip() or vid
    return f"{base}.{ext}"

def client_ip() -> str:
    if request.remote_addr in TRUST_PROXY and "X-Forwarded-For" in request.headers:
        return request.headers["X-Forwarded-For"].split(",")[0].strip()
    return request.remote_addr or "unknown"

def canonical(url: str) -> str:
    m = YOUTUBE_RE.match(url.strip())
    return m.group(4).lower() if m else ""

def vid_key(vid: str, audio: bool) -> str:
    return f"{vid}|{'a' if audio else 'v'}"

def can_queue(ip: str, num_urls: int) -> bool:
    now = datetime.now(timezone.utc)
    dq = ip_recent[ip]
    while dq and now - dq[0] > REQUEST_WINDOW:
        dq.popleft()
    if len(dq) + num_urls > MAX_REQUESTS_PER_IP_MIN:
        return False
    dq.extend([now] * num_urls)
    return True

# ───────────────────────────────────────────────────────────────
# queue handling
# ───────────────────────────────────────────────────────────────
def launch_if_possible(ip: str) -> None:
    with LOCK:
        if ip in ip_active or not ip_queues[ip] or len(active_dl_set) >= MAX_DOWNLOADS_GLOBAL:
            return
        job_id = ip_queues[ip].popleft()
        ip_active[ip] = job_id
        active_dl_set.add(job_id)
        jobs[job_id]["status"] = "downloading"
    EXECUTOR.submit(download_job, job_id, jobs[job_id]["url"], ip)

# ───────────────────────────────────────────────────────────────
# download worker
# ───────────────────────────────────────────────────────────────
def download_job(job_id: str, url: str, ip: str) -> None:
    def abort_hook(_):                      # fires ~0.5 s
        if jobs[job_id].get("cancelled"):
            raise DownloadError("cancelled")

    j          = jobs[job_id]
    # ── inside download_job ──
    audio_only = j.get("audio")
    ext        = "mp3" if audio_only else "mp4"          # ← was "m4a"

    outtmpl    = os.path.join(DL_DIR, f"{job_id}.%(ext)s")

    if audio_only:
        ydl_opts = {
            "format": "bestaudio[ext=m4a]/bestaudio",
            "outtmpl": outtmpl,
            "quiet": True, "no_warnings": True,
            "progress_hooks": [abort_hook],
            "postprocessors": [{
                "key": "FFmpegExtractAudio",
                "preferredcodec": "mp3",
                "preferredquality": "0",
            }],
        }
    else:
        ydl_opts = {
            "format": "bestvideo[ext=mp4]+bestaudio[ext=mp3]/best[ext=mp4]/best",
            "merge_output_format": "mp4",
            "outtmpl": outtmpl,
            "quiet": True, "no_warnings": True,
            "progress_hooks": [abort_hook],
        }

    if not ffmpeg_ok():
        j.update(status="error", error="ffmpeg not found")
        _cleanup_job_state(ip, job_id)
        return

    try:
        with YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=True)
        if j.get("cancelled"):
            raise DownloadError("cancelled")

        title = info.get("title") or ""
        vid   = info.get("id") or job_id
        j.update(
            status="finished",
            file=f"{job_id}.{ext}",
            name=clean_filename(title, vid, ext),
        )
    except DownloadError as e:
        j.update(status="cancelled" if "cancelled" in str(e) else "error",
                 error=str(e))
        for ex in (".mp4", ".mp3", ".part"):
            try: os.remove(outtmpl % {"ext": ex.lstrip(".")})
            except FileNotFoundError: pass
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
        for key, jid in list(url_map.items()):
            if jid == job_id:
                if status == "finished":
                    completed_cache[key] = datetime.now(timezone.utc)
                    url_map[key] = "cached"
                else:
                    url_map.pop(key, None)

# ───────────────────────────────────────────────────────────────
# housekeeping
# ───────────────────────────────────────────────────────────────
def clean_files() -> None:
    cutoff = datetime.now(timezone.utc) - JOB_TTL
    for fn in os.listdir(DL_DIR):
        fp = DL_DIR / fn
        try:
            if fp.is_file() and datetime.fromtimestamp(fp.stat().st_mtime, timezone.utc) < cutoff:
                fp.unlink()
        except OSError:
            pass

def purge_job_table() -> None:
    cutoff = datetime.now(timezone.utc) - JOB_TTL
    with LOCK:
        for jid, j in list(jobs.items()):
            if j.get("created") and j["created"] < cutoff \
               and j["status"] in {"finished", "error", "cancelled", "invalid"}:
                jobs.pop(jid, None)
        valid = set(jobs)
        for key, jid in list(url_map.items()):
            if jid not in valid and jid != "cached":
                url_map.pop(key, None)

def purge_completed_cache() -> None:
    cutoff = datetime.now(timezone.utc) - COMPLETED_CACHE_TTL
    with LOCK:
        for key, ts in list(completed_cache.items()):
            if ts < cutoff:
                completed_cache.pop(key, None)
                url_map.pop(key, None)

scheduler = BackgroundScheduler(daemon=True)
scheduler.add_job(clean_files,          "cron", hour=0,   minute=0)
scheduler.add_job(purge_job_table,      "cron", hour="*")
scheduler.add_job(purge_completed_cache,"cron", minute="*/30")
scheduler.start()

# ───────────────────────────────────────────────────────────────
# routes
# ───────────────────────────────────────────────────────────────
@app.route("/")
def index():
    return render_template("index.html")

@app.route("/start", methods=["POST"])
def start():
    raw    = (request.json.get("urls") or "").strip()
    audio  = bool(request.json.get("audio"))
    urls   = [u for u in re.split(r"[,\s]+", raw) if u]

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
    now  = datetime.now(timezone.utc)

    with LOCK:
        for url in urls:
            if not YOUTUBE_RE.match(url):
                resp[url] = {"error": "Not a valid YouTube link"}
                continue

            vid    = canonical(url)
            key    = vid_key(vid, audio)

            # duplicate?
            if key in url_map:
                resp[url] = url_map[key]
                continue
            if key in completed_cache and now - completed_cache[key] < COMPLETED_CACHE_TTL:
                url_map[key] = "cached"
                resp[url]    = "cached"
                continue

            job_id = str(uuid.uuid4())
            url_map[key] = job_id
            jobs[job_id] = {
                "status":  "queued",
                "url":     url,
                "ip":      ip,
                "created": now,
                "audio":   audio,
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

    path = DL_DIR / job["file"]
    if not path.exists():
        return "Missing file", 404

    def stream_and_delete():
        try:
            with open(path, "rb") as f:
                for chunk in iter(lambda: f.read(65536), b""):
                    yield chunk
        finally:
            try: path.unlink()
            except FileNotFoundError: pass
            jobs.pop(job_id, None)

    mimetype = "audio/mp4" if path.suffix == ".mp3" else "video/mp4"
    resp = Response(stream_with_context(stream_and_delete()), mimetype=mimetype)
    resp.headers["Content-Disposition"] = f'attachment; filename="{job.get("name","file")}"'
    resp.headers["Cache-Control"] = "no-store"
    return resp

@app.route("/cancel_all", methods=["POST"])
def cancel_all():
    ip = client_ip()
    with LOCK:
        for jid in list(ip_queues[ip]):                          # queued
            ip_queues[ip].remove(jid)
            jobs[jid]["status"] = "cancelled"
            # remove corresponding key from url_map
            for k, v in list(url_map.items()):
                if v == jid:
                    url_map.pop(k, None)
        jid = ip_active.get(ip)                                  # active
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

# ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    app.run(port=int(os.getenv("PORT", 8989)), debug=False, use_reloader=False)
