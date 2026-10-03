"""S3-bis: atomicidad del reclamo por git push con --force-with-lease=<ref>: (la ref no debe existir).

Contexto: en las sesiones web de Claude Code el proxy bloquea POST git/refs (S1, 2026-10-02), pero
acepta git push. Esta prueba comprueba que, con W pushes simultáneos que intentan crear la misma
rama con commits distintos, GitHub acepta exactamente uno y la rama queda en el commit del ganador.

Uso (en GitHub Actions, ver ct-push-race.yml):  python3 race_push.py DUENO/REPO
Vars: RACES=100  WORKERS=10  PAUSE=15  GH_TOKEN (token con contents: write)
Repo: desechable, NO vacío. Deja ~RACES ramas ct/prace/<run>/<k>: borra el repo al terminar.
"""
import base64, json, os, subprocess, sys, tempfile, threading, time, uuid
from collections import Counter

REPO = sys.argv[1]
TOKEN = os.environ["GH_TOKEN"]
RACES = int(os.environ.get("RACES", 100))
W = int(os.environ.get("WORKERS", 10))
PAUSE = float(os.environ.get("PAUSE", 15))
RUN = time.strftime("%Y%m%d%H%M%S")
URL = os.environ.get("PRACE_URL") or f"https://github.com/{REPO}.git"  # PRACE_URL: solo para probar en local
AUTH = base64.b64encode(f"x-access-token:{TOKEN}".encode()).decode()
G = ["git", "-c", f"http.extraheader=AUTHORIZATION: basic {AUTH}",
     "-c", "user.name=race", "-c", "user.email=race@example.invalid"]
WORK = tempfile.mkdtemp(prefix="prace-")


def git(*args, check=True):
    return subprocess.run([*G, *args], cwd=WORK, capture_output=True, text=True, check=check, timeout=120)


git("init", "-q")
default = os.environ.get("PRACE_DEFAULT") or json.loads(subprocess.run(
    ["gh", "api", f"repos/{REPO}"], capture_output=True, text=True, check=True).stdout)["default_branch"]
git("fetch", "-q", "--depth=1", URL, f"refs/heads/{default}")
base = git("rev-parse", "FETCH_HEAD").stdout.strip()
tree = git("rev-parse", "FETCH_HEAD^{tree}").stdout.strip()
# Un commit de reclamo distinto por hilo (hijos de la base), creados una vez y reutilizados.
shas = [git("commit-tree", tree, "-p", base, "-m", f"prace claim {i}\n\nclaim-nonce {uuid.uuid4()}").stdout.strip()
        for i in range(W)]
OUT = f"ct_push_{RUN}.jsonl"


def save(rec):
    with open(OUT, "a", encoding="utf-8") as f:
        f.write(json.dumps(rec) + "\n")


save({"run": RUN, "base": base, "shas": shas, "workers": W, "races": RACES})
TRANSPORT = ("could not resolve", "rpc failed", "http 5", "unable to access", "timed out",
             "connection", "the requested url returned error: 403", "returned error: 429")


def race(k):
    ref = f"refs/heads/ct/prace/{RUN}/{k}"
    bar, out = threading.Barrier(W), [None] * W

    def run(i):
        bar.wait()
        t0 = time.perf_counter()
        try:
            p = subprocess.run([*G, "push", "--porcelain", f"--force-with-lease={ref}:", URL, f"{shas[i]}:{ref}"],
                               cwd=WORK, capture_output=True, text=True, timeout=120)
            rc, txt = p.returncode, (p.stdout + p.stderr)
        except Exception as e:  # noqa: BLE001
            rc, txt = -1, f"EXC {type(e).__name__}: {e}"
        out[i] = {"i": i, "rc": rc, "t0": t0, "t1": time.perf_counter(), "txt": txt[-400:]}

    ts = [threading.Thread(target=run, args=(i,)) for i in range(W)]
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    finals = []
    for _ in range(2):
        r = git("ls-remote", URL, ref, check=False)
        finals.append(r.stdout.split()[0] if r.returncode == 0 and r.stdout.strip() else None)
        time.sleep(2)
    return ref, out, finals


def kind(o):
    t = o["txt"].lower()
    if o["rc"] == 0:
        return "ok"
    if "stale info" in t:
        return "cliente:stale-info"
    if "remote rejected" in t or "failed to update ref" in t or "cannot lock ref" in t or "already exists" in t:
        return "servidor:rechazo"
    if "fetch first" in t or "non-fast-forward" in t:
        return "servidor:no-ff"
    if any(x in t for x in TRANSPORT) or o["rc"] < 0:
        return "transporte"
    return "otro"


stats, bad, valid, k, srv = Counter(), [], 0, 0, 0
while valid < RACES:
    ref, out, finals = race(k)
    k += 1
    kinds = [kind(o) for o in out]
    stats.update(kinds)
    if "transporte" in kinds:  # un error de red invalida la carrera; "otro" cuenta como falla (cierre fallido)
        save({"ref": ref, "invalid": True, "out": out, "finals": finals})
        print(f"{ref}: INVALIDA {Counter(kinds)}; espero 60 s")
        time.sleep(60)
        continue
    valid += 1
    winners = [o for o in out if o["rc"] == 0]
    good = (len(winners) == 1 and finals[0] == finals[1] == shas[winners[0]["i"]]
            and all(x in ("ok", "cliente:stale-info", "servidor:rechazo", "servidor:no-ff") for x in kinds))
    srv += any(x.startswith("servidor") for x in kinds)
    if not good:
        bad.append(ref)
    save({"ref": ref, "ok": good, "kinds": kinds, "out": out, "finals": finals})
    print(f"{valid:3}/{RACES} {ref}: {'OK' if good else 'FALLA'} {dict(Counter(kinds))}")
    time.sleep(PAUSE)

save({"summary": {"valid": valid, "bad": bad, "kinds": dict(stats), "races_with_server_rejection": srv}})
print(f"\nVálidas: {valid}  Fallas: {len(bad)}  Carreras donde el servidor rechazó al menos a uno: {srv}")
print("Tipos de respuesta:", dict(stats))
print("Resultado:", "PASA" if not bad else f"NO PASA: {bad}")
sys.exit(1 if bad else 0)
