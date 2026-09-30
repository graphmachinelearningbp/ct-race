# /// script
# requires-python = ">=3.9"
# ///
"""CT-REF-01..04 contra github.com con UN solo token de grano fino (Contents: write).

Uso:   uv run race_refs.py DUENO/REPO        (o: py race_refs.py DUENO/REPO)
Vars:  RACES=100  WORKERS=10  PAUSE=80  MODE=create|patch   (v2: hilos robustos y registro JSONL incremental)
Repo:  desechable, NO vacio (con un README), sin workflows. Borralo al terminar.
"""
import getpass, http.client, json, os, sys, threading, time, uuid
from collections import Counter

REPO = sys.argv[1]
TOKEN = os.environ.get("GH_TOKEN") or getpass.getpass("Token de grano fino: ")
RACES = int(os.environ.get("RACES", 100))
W = int(os.environ.get("WORKERS", 10))
PAUSE = float(os.environ.get("PAUSE", 80))  # ~45 carreras/h: < 500 escrituras/h por usuario
MODE = os.environ.get("MODE", "create")      # create = POST git/refs; patch = candado alternativo
RUN = time.strftime("%Y%m%d%H%M%S")
HDR = {"Authorization": f"Bearer {TOKEN}", "Accept": "application/vnd.github+json",
       "X-GitHub-Api-Version": "2026-03-10", "User-Agent": "ct-ref-race",
       "Content-Type": "application/json"}


def conn():
    c = http.client.HTTPSConnection("api.github.com", timeout=60)
    c.connect()  # TLS abierto ANTES de la barrera: la carrera es real
    return c


def call(method, path, body=None, c=None):
    c = c or conn()
    url = f"/repos/{REPO}" + (f"/{path}" if path else "")
    c.request(method, url, json.dumps(body).encode() if body is not None else None, HDR)
    r = c.getresponse()
    raw = r.read()
    try:
        data = json.loads(raw) if raw else {}
    except ValueError:
        data = {"message": raw[:200].decode(errors="replace")}
    if not isinstance(data, dict):
        data = {"message": str(data)[:200]}
    return r.status, data, r.getheader("retry-after")


def brief(res):
    return res[0], res[1].get("message", "")


def must(res, what):
    if res[0] >= 300:
        sys.exit(f"{what}: {brief(res)}")
    return res[1]


default = must(call("GET", ""), "repo")["default_branch"]
base = must(call("GET", f"git/ref/heads/{default}"), "base")["object"]["sha"]
tree = must(call("GET", f"git/commits/{base}"), "tree")["tree"]["sha"]


def commit(parent, msg):
    return must(call("POST", "git/commits",
                     {"message": msg, "tree": tree, "parents": [parent]}), "commit")["sha"]


# Un commit de reclamo distinto por hilo (hijos de la base), creados UNA vez y reutilizados.
shas = [commit(base, f"ct-ref-03 claim {i}\n\nclaim-nonce {uuid.uuid4()}") for i in range(W)]
log = {"run": RUN, "mode": MODE, "base": base, "shas": shas, "single": {}, "races": []}

if MODE == "create":  # CT-REF-01, CT-REF-02 y CT-REF-04, una sola vez
    n = f"refs/heads/ct/ref02/{RUN}"
    rp = f"git/refs/heads/ct/ref02/{RUN}"
    s = {}
    s["01 crear nueva"] = brief(call("POST", "git/refs", {"ref": n, "sha": shas[0]}))
    s["02 existe, mismo sha"] = brief(call("POST", "git/refs", {"ref": n, "sha": shas[0]}))
    s["02 existe, otro sha"] = brief(call("POST", "git/refs", {"ref": n, "sha": shas[1]}))
    s["04 PATCH no ff"] = brief(call("PATCH", rp,
                                     {"sha": shas[1], "force": False}))
    ff = commit(shas[0], "ct-ref-04 ff")
    s["04 PATCH ff"] = brief(call("PATCH", rp,
                                  {"sha": ff, "force": False}))
    s["04 DELETE 1"] = brief(call("DELETE", rp))
    s["04 DELETE 2"] = brief(call("DELETE", rp))
    log["single"] = s
    for k, v in s.items():
        print(f"CT-REF-{k}: {v}")


def race(k):
    name = f"ct/race/{RUN}/{k}"
    if MODE == "patch":  # candado alternativo: ref creada en la base, todos hacen PATCH ff
        try:
            lk = call("POST", "git/refs", {"ref": f"refs/heads/{name}", "sha": base})
        except Exception as e:
            lk = (0, {"message": f"EXC {e}"[:200]}, None)
        if lk[0] != 201:  # sin candado no hay carrera: se invalida y se repite
            return name, [{"i": -1, "st": lk[0] if lk[0] >= 300 else 0, "msg": "lock " + lk[1].get("message", ""),
                           "t0": 0, "t1": 0, "ra": lk[2]}], [None, None]
    try:
        conns = [conn() for _ in range(W)]
    except Exception as e:
        return name, [{"i": -1, "st": 0, "msg": f"EXC conn {e}"[:200], "t0": 0, "t1": 0, "ra": None}], [None, None]
    bar, out = threading.Barrier(W), [None] * W

    def run(i):
        bar.wait()
        t0 = time.perf_counter()
        try:
            if MODE == "create":
                st, d, ra = call("POST", "git/refs", {"ref": f"refs/heads/{name}", "sha": shas[i]}, conns[i])
            else:
                st, d, ra = call("PATCH", f"git/refs/heads/{name}", {"sha": shas[i], "force": False}, conns[i])
        except Exception as e:  # red caida, timeout: la carrera se invalida, no se pierde el registro
            st, d, ra = 0, {"message": f"EXC {type(e).__name__}: {e}"[:200]}, None
        out[i] = {"i": i, "st": st, "msg": d.get("message", ""), "t0": t0,
                  "t1": time.perf_counter(), "ra": ra}

    ts = [threading.Thread(target=run, args=(i,)) for i in range(W)]
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    finals = []
    for _ in range(2):  # dos lecturas separadas 3 s: la ref no debe cambiar
        try:
            st, d, _ = call("GET", f"git/ref/heads/{name}")
        except Exception:
            finals.append("EXC")
            time.sleep(3)
            continue
        finals.append(d.get("object", {}).get("sha") if st == 200 else None)
        time.sleep(3)
    return name, out, finals


OUT = f"ct_ref_{MODE}_{RUN}.jsonl"
def save(rec):
    with open(OUT, "a", encoding="utf-8") as f:
        f.write(json.dumps(rec) + chr(10))
save({k: v for k, v in log.items() if k != "races"})
win_code = 201 if MODE == "create" else 200
stats, bad, valid, overl = Counter(), [], 0, 0
k = 0
while valid < RACES:
    name, out, finals = race(k)
    k += 1
    codes = Counter(f"{o['st']} {o['msg']}" for o in out)
    stats.update(codes)
    if any(o["st"] in (0, 403, 429) or o["st"] >= 500 for o in out) or "EXC" in finals:
        wait = max([int(float(o["ra"])) for o in out if o["ra"]] or [120])
        print(f"{name}: INVALIDA (limite o 5xx) {dict(codes)}; espero {wait}s")
        save({"name": name, "invalid": True, "out": out, "finals": finals})
        time.sleep(wait)
        continue
    valid += 1
    winners = [o for o in out if o["st"] == win_code]
    overlap = max(o["t0"] for o in out) < min(o["t1"] for o in out)
    overl += overlap
    good = (len(winners) == 1
            and all(o["st"] in (409, 422) for o in out if o is not winners[0])
            and finals[0] == finals[1] == shas[winners[0]["i"]])
    if not good:
        bad.append(name)
    save({"name": name, "ok": good, "overlap": overlap, "out": out, "finals": finals})
    print(f"{valid:3}/{RACES} {name}: {'OK' if good else 'FALLA'} solapadas={overlap} {dict(codes)}")
    time.sleep(PAUSE)

save({"summary": {"valid": valid, "bad": bad, "overlap": overl, "codes": dict(stats)}})
print(f"\nValidas: {valid}  Fallas: {len(bad)}  Con solapamiento real: {overl}")
print("Respuestas vistas:", dict(stats))
print("Resultado:", "PASA" if not bad else f"NO PASA: {bad}")
sys.exit(1 if bad else 0)
