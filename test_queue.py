#!/usr/bin/env python3
"""
stress_queue.py – hammer the downloader hard ⚒️  (v4 – 10 identical links/IP)

Exactly the **same IP spoofing and monitor logic** as the original, but instead
of inventing random (usually invalid) IDs we queue a fixed set of **10 well-
known, public YouTube videos** for every client.  That pounds your dedup +
per-IP FIFO code without the 404 spam.

Everything else is untouched: constants at the top, the /info polling loop, and
error handling remain identical.
"""

import asyncio, aiohttp, sys, time, random, string  # random & string kept for full parity
from itertools import islice

# ── CONFIG ─────────────────────────────────────────────
BASE_URL      = "http://localhost:8989"    # change if server lives elsewhere
IPS           = 10                         # how many fake clients / IPs
LINKS_PER_IP  = 10                         # now a strict 10 – see VIDEO_URLS below
POLL_INTERVAL = 3                          # seconds between /info polls
# ───────────────────────────────────────────────────────

# Ten stable, public videos (official uploads that have been online for years)
VIDEO_URLS = [
    "https://www.youtube.com/watch?v=dQw4w9WgXcQ",  # Rick Astley – Never Gonna Give You Up
    "https://www.youtube.com/watch?v=9bZkp7q19f0",  # PSY – Gangnam Style
    "https://www.youtube.com/watch?v=L_jWHffIx5E",  # Nirvana – Smells Like Teen Spirit
    "https://www.youtube.com/watch?v=RgKAFK5djSk",  # Wiz Khalifa – See You Again ft. Charlie Puth
    "https://www.youtube.com/watch?v=OnyPSlV0dic",  # Adele – Rolling in the Deep (Live at the Royal Albert Hall)
    "https://www.youtube.com/watch?v=fLexgOxsZu0",  # Maroon 5 – Sugar
    "https://www.youtube.com/watch?v=tVj0ZTS4WF4",  # LMFAO – Party Rock Anthem
    "https://www.youtube.com/watch?v=CevxZvSJLk8",  # Katy Perry – Roar
    "https://www.youtube.com/watch?v=kffacxfA7G4",  # Justin Bieber – Baby ft. Ludacris
    "https://www.youtube.com/watch?v=3tmd-ClpJxA",  # Ed Sheeran – Shape of You
]

URLS_STRING = " ".join(VIDEO_URLS)

# ── (Unused) random helpers kept so the structure barely changes ──
ALPHABET = string.ascii_letters + string.digits + "_-"

def random_video_id():
    return "".join(random.choice(ALPHABET) for _ in range(11))

def random_youtube_url():
    vid = random_video_id()
    return f"https://www.youtube.com/watch?v={vid}"

# ── queue + monitor logic (unchanged except for URL_STRING) ───────
async def queue_links(session, ip):
    headers = {"X-Forwarded-For": ip}
    async with session.post(
        f"{BASE_URL}/start",
        json={"urls": URLS_STRING},
        headers=headers,
    ) as r:
        if r.status != 200:
            print(f"[{ip}] /start HTTP {r.status}")
            text = await r.text()
            print(text[:500])
            return {}
        data = await r.json()
        errs = {
            k: v
            for k, v in data.items()
            if isinstance(v, dict) and v.get("error")
        }
        if errs:
            print(f"[{ip}] {len(errs)} link-level errors (showing 3):", list(islice(errs.items(), 3)))
        return {k: v for k, v in data.items() if isinstance(v, str)}  # {url: job_id}


async def poll_info(session):
    async with session.get(f"{BASE_URL}/info") as r:
        return await r.json()


async def main():
    async with aiohttp.ClientSession() as session:
        fake_ips = [f"203.0.113.{i}" for i in range(IPS)]
        print(f"Queuing {LINKS_PER_IP} links each for {IPS} fake IPs …")

        # fire off /start for every fake IP
        all_jobs = {}
        for ip in fake_ips:
            jobs = await queue_links(session, ip)
            all_jobs[ip] = jobs
            print(f"  ↳ {ip}: queued {len(jobs)} URLs (errors: {LINKS_PER_IP - len(jobs)})")

        print("\n== Hammer sent. Monitoring queue size ==")
        while True:
            try:
                inf = await poll_info(session)
                print(
                    time.strftime("%H:%M:%S"),
                    f"active={inf['global_active']}/100",
                    f"queue={inf['global_queue']}",
                    f"(you_active={inf['your_active']} you_queue={inf['your_queue']})",
                )
            except Exception as e:
                print("info poll error:", e, file=sys.stderr)

            if inf["global_active"] == 0 and inf["global_queue"] == 0:
                print("All jobs completed or errored out. Done.")
                break
            await asyncio.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nStopped by user.")