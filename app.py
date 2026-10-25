"""
app.py - Dashboard Simulasi Gempa (Flask + MQTT + MySQL)
=======================================================
ESP32 --(MQTT)--> app.py --(MySQL)--> riwayat & analisis
                  app.py --(SSE)-----> browser (tampilan live)

Jalankan:
    pip install -r requirements.txt
    mysql -u root -p < schema.sql
    python app.py            -> buka http://localhost:5000

Konfigurasi lewat environment variable (atau ubah nilai default di bawah):
    DB_HOST, DB_USER, DB_PASS, DB_NAME, MQTT_HOST, MQTT_PORT, TOPIC_PREFIX
"""

import csv
import io
import json
import os
import queue
import threading
import time
from contextlib import contextmanager
from datetime import datetime, timedelta

import paho.mqtt.client as mqtt
import pymysql
from flask import Flask, Response, jsonify, render_template, request

# ======================= KONFIGURASI =======================
MQTT_HOST    = os.environ.get("MQTT_HOST", "broker.hivemq.com")
MQTT_PORT    = int(os.environ.get("MQTT_PORT", 1883))
MQTT_CLIENT  = "flask-gempa-kelompok3a"                      # harus unik di broker
TOPIC_PREFIX = os.environ.get("TOPIC_PREFIX", "gempa3a/kelompok3a/")   # samakan dengan ESP32

T_DATA   = TOPIC_PREFIX + "data"
T_SPEC   = TOPIC_PREFIX + "spektrum"
T_STATUS = TOPIC_PREFIX + "status"
T_EVENT  = TOPIC_PREFIX + "event"
T_HASIL  = TOPIC_PREFIX + "hasil"      # dari gempa_monitor.py (opsional)
T_CMD    = TOPIC_PREFIX + "cmd"

DB = dict(
    host=os.environ.get("DB_HOST", "localhost"),
    user=os.environ.get("DB_USER", "root"),
    password=os.environ.get("DB_PASS", ""),
    database=os.environ.get("DB_NAME", "gempa_db"),
    charset="utf8mb4",
    autocommit=True,
    cursorclass=pymysql.cursors.DictCursor,
)

SENSOR_ID = {"tanah": 1, "kecil": 2, "sedang": 3, "tinggi": 4}   # sama dengan tabel sensor
SIMPAN_SPEKTRUM_TIAP_S = 10     # spektrum disimpan ke DB tiap sekian detik
AMP_TANAH_MIN_G = 0.02          # getaran tanah minimum untuk analisis sapuan
BIN_HZ = 0.5                    # lebar kelompok frekuensi pada analisis sapuan
MIN_TITIK = 3                   # minimal titik data per kelompok

app = Flask(__name__)

# ======================= STATE & SSE =======================
state = {"data": None, "spektrum": None, "status": None, "hasil": None}
sesi_aktif = {"id": None, "nama": None}
subscribers = []
sub_lock = threading.Lock()
_last_spec_save = 0.0
mqtt_client = None


def sse(event, payload):
    return f"event: {event}\ndata: {json.dumps(payload)}\n\n"


def broadcast(event, payload):
    msg = sse(event, payload)
    with sub_lock:
        for q in list(subscribers):
            try:
                q.put_nowait(msg)
            except queue.Full:
                pass          # browser terlalu lambat: lewati saja


def iso(v):
    return v.isoformat() if isinstance(v, datetime) else v


@contextmanager
def db():
    conn = pymysql.connect(**DB)
    try:
        with conn.cursor() as cur:
            yield cur
    finally:
        conn.close()


def muat_sesi_aktif():
    try:
        with db() as cur:
            cur.execute("SELECT id, nama FROM sesi WHERE selesai IS NULL ORDER BY id DESC LIMIT 1")
            r = cur.fetchone()
            if r:
                sesi_aktif.update(id=r["id"], nama=r["nama"])
    except Exception as e:
        print(f"[DB] Gagal membaca sesi aktif: {e}")


# ======================= MQTT -> DB + SSE =======================
def simpan_data(d, waktu):
    res = d.get("resonansi", {})
    rows = []
    for kode, sid in SENSOR_ID.items():
        s = d.get(kode)
        if not s:
            continue
        rows.append((waktu, sesi_aktif["id"], sid, int(s.get("on", 0)),
                     s.get("rms"), s.get("x"), s.get("y"), s.get("z"),
                     s.get("f"), s.get("amp"), s.get("rasio"),
                     res.get(kode) if kode != "tanah" else None))
    with db() as cur:
        cur.executemany(
            "INSERT INTO pembacaan (waktu, sesi_id, sensor_id, online, rms, x, y, z, f, amp, rasio, resonansi) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", rows)


def simpan_spektrum(d, waktu):
    rows = []
    for kode, sid in SENSOR_ID.items():
        arr = d.get(kode)
        if arr:
            rows.append((waktu, sesi_aktif["id"], sid, d.get("df"), d.get("fs"), json.dumps(arr)))
    if rows:
        with db() as cur:
            cur.executemany(
                "INSERT INTO spektrum (waktu, sesi_id, sensor_id, df, fs, amplitudo) VALUES (%s,%s,%s,%s,%s,%s)", rows)


def on_connect(client, userdata, flags, reason_code, properties):
    if reason_code.is_failure:
        print(f"[MQTT] Gagal konek: {reason_code}")
        return
    print("[MQTT] Terhubung ke broker")
    client.subscribe([(T_DATA, 0), (T_SPEC, 0), (T_STATUS, 0), (T_EVENT, 0), (T_HASIL, 0)])


def on_message(client, userdata, msg):
    global _last_spec_save
    payload = msg.payload.decode("utf-8", errors="replace")
    waktu = datetime.now()
    try:
        if msg.topic == T_DATA:
            d = json.loads(payload)
            d["waktu"] = iso(waktu)
            d["sesi"] = sesi_aktif["nama"]
            state["data"] = d
            broadcast("data", d)
            simpan_data(d, waktu)

        elif msg.topic == T_SPEC:
            d = json.loads(payload)
            state["spektrum"] = d
            broadcast("spektrum", d)
            if time.time() - _last_spec_save >= SIMPAN_SPEKTRUM_TIAP_S:
                _last_spec_save = time.time()
                simpan_spektrum(d, waktu)

        elif msg.topic == T_STATUS:
            status = payload.strip().lower()
            if status in ("online", "offline"):
                state["status"] = status
                broadcast("status", {"status": status})
                with db() as cur:
                    cur.execute("INSERT INTO status_perangkat (waktu, status) VALUES (%s,%s)", (waktu, status))

        elif msg.topic == T_EVENT:
            d = json.loads(payload)
            d["waktu"] = iso(waktu)
            broadcast("event", d)
            with db() as cur:
                cur.execute(
                    "INSERT INTO kejadian (waktu, sesi_id, sensor_id, tipe, uptime_esp) VALUES (%s,%s,%s,%s,%s)",
                    (waktu, sesi_aktif["id"], SENSOR_ID.get(d.get("sumber")), d.get("tipe"), d.get("t")))

        elif msg.topic == T_HASIL:
            d = json.loads(payload)
            state["hasil"] = d
            broadcast("hasil", d)

    except json.JSONDecodeError:
        print(f"[WARN] Bukan JSON valid di {msg.topic}: {payload[:60]}")
    except Exception as e:
        # Kegagalan DB tidak boleh menghentikan tampilan live
        print(f"[ERROR] {msg.topic}: {e}")


def mulai_mqtt():
    global mqtt_client
    mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=MQTT_CLIENT)
    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message
    mqtt_client.reconnect_delay_set(min_delay=1, max_delay=30)
    mqtt_client.connect_async(MQTT_HOST, MQTT_PORT, keepalive=60)
    mqtt_client.loop_start()


# ======================= HALAMAN & STREAM =======================
@app.get("/")
def index():
    return render_template("dashboard.html")


@app.get("/stream")
def stream():
    q = queue.Queue(maxsize=100)
    with sub_lock:
        subscribers.append(q)

    def gen():
        try:
            for ev in ("status", "data", "spektrum", "hasil"):     # kirim kondisi terakhir
                if state[ev] is not None:
                    yield sse(ev, state[ev] if ev != "status" else {"status": state["status"]})
            while True:
                try:
                    yield q.get(timeout=15)
                except queue.Empty:
                    yield ": ping\n\n"                              # jaga koneksi tetap hidup
        finally:
            with sub_lock:
                if q in subscribers:
                    subscribers.remove(q)

    return Response(gen(), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


# ======================= API =======================
@app.post("/api/cmd")
def api_cmd():
    cmd = (request.get_json(silent=True) or {}).get("cmd")
    if cmd not in ("mute", "unmute"):
        return jsonify(ok=False, error="Perintah tidak dikenal"), 400
    mqtt_client.publish(T_CMD, cmd)
    return jsonify(ok=True)


@app.get("/api/riwayat")
def api_riwayat():
    """Data RMS beberapa menit terakhir untuk mengisi grafik saat halaman dibuka."""
    menit = max(1, min(request.args.get("menit", 5, type=int), 180))
    batas = datetime.now() - timedelta(minutes=menit)
    with db() as cur:
        cur.execute(
            "SELECT p.waktu, s.kode, p.online, p.rms FROM pembacaan p "
            "JOIN sensor s ON s.id = p.sensor_id WHERE p.waktu >= %s ORDER BY p.waktu, p.sensor_id", (batas,))
        rows = cur.fetchall()
    out = {}
    for r in rows:
        out.setdefault(iso(r["waktu"]), {})[r["kode"]] = {"on": r["online"], "rms": r["rms"]}
    return jsonify([{"waktu": k, **v} for k, v in out.items()])


@app.get("/api/sweep")
def api_sweep():
    """
    Rasio amplitudo bangunan/tanah per frekuensi getaran tanah.
    Puncak kurva = frekuensi natural bangunan (resonansi).
    """
    sesi_id = request.args.get("sesi_id", type=int)
    sql = (
        "SELECT s.kode, ROUND(t.f / %s) * %s AS f_bin, AVG(b.rasio) AS rasio_rata, "
        "MAX(b.rasio) AS rasio_maks, COUNT(*) AS n "
        "FROM pembacaan b "
        "JOIN pembacaan t ON t.waktu = b.waktu AND t.sensor_id = 1 "
        "JOIN sensor s ON s.id = b.sensor_id "
        "WHERE b.sensor_id <> 1 AND b.online = 1 AND t.f IS NOT NULL AND t.amp >= %s AND b.rasio IS NOT NULL "
    )
    params = [BIN_HZ, BIN_HZ, AMP_TANAH_MIN_G]
    if sesi_id:
        sql += "AND b.sesi_id = %s "
        params.append(sesi_id)
    sql += "GROUP BY s.kode, f_bin HAVING n >= %s ORDER BY s.kode, f_bin"
    params.append(MIN_TITIK)
    with db() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()
    return jsonify([{"kode": r["kode"], "f_bin": float(r["f_bin"]), "rasio_rata": float(r["rasio_rata"]),
                     "rasio_maks": float(r["rasio_maks"]), "n": r["n"]} for r in rows])


@app.get("/api/kejadian")
def api_kejadian():
    limit = max(1, min(request.args.get("limit", 30, type=int), 200))
    with db() as cur:
        cur.execute(
            "SELECT k.waktu, s.kode AS sumber, k.tipe, k.uptime_esp AS t FROM kejadian k "
            "LEFT JOIN sensor s ON s.id = k.sensor_id ORDER BY k.id DESC LIMIT %s", (limit,))
        rows = cur.fetchall()
    return jsonify([{**r, "waktu": iso(r["waktu"])} for r in rows])


@app.get("/api/sesi")
def api_sesi():
    with db() as cur:
        cur.execute("SELECT id, nama, catatan, mulai, selesai FROM sesi ORDER BY id DESC LIMIT 50")
        rows = cur.fetchall()
    return jsonify([{**r, "mulai": iso(r["mulai"]), "selesai": iso(r["selesai"])} for r in rows])


@app.post("/api/sesi/mulai")
def api_sesi_mulai():
    j = request.get_json(silent=True) or {}
    sekarang = datetime.now()
    nama = (j.get("nama") or "").strip()[:100] or f"Sesi {sekarang:%d/%m %H:%M}"
    with db() as cur:
        cur.execute("UPDATE sesi SET selesai = %s WHERE selesai IS NULL", (sekarang,))
        cur.execute("INSERT INTO sesi (nama, catatan, mulai) VALUES (%s,%s,%s)", (nama, j.get("catatan"), sekarang))
        sesi_aktif.update(id=cur.lastrowid, nama=nama)
    return jsonify(ok=True, id=sesi_aktif["id"], nama=nama)


@app.post("/api/sesi/selesai")
def api_sesi_selesai():
    with db() as cur:
        cur.execute("UPDATE sesi SET selesai = %s WHERE selesai IS NULL", (datetime.now(),))
    sesi_aktif.update(id=None, nama=None)
    return jsonify(ok=True)


@app.get("/api/export.csv")
def api_export():
    """Unduh data pembacaan (semua, atau satu sesi) sebagai CSV untuk laporan."""
    sesi_id = request.args.get("sesi_id", type=int)
    sql = ("SELECT p.waktu, s.kode AS sensor, p.online, p.rms, p.x, p.y, p.z, p.f, p.amp, p.rasio, p.resonansi "
           "FROM pembacaan p JOIN sensor s ON s.id = p.sensor_id ")
    params = []
    if sesi_id:
        sql += "WHERE p.sesi_id = %s "
        params.append(sesi_id)
    sql += "ORDER BY p.waktu, p.sensor_id LIMIT 500000"
    with db() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["waktu", "sensor", "online", "rms", "x", "y", "z", "f", "amp", "rasio", "resonansi"])
    for r in rows:
        w.writerow([iso(v) if k == "waktu" else ("" if v is None else v) for k, v in r.items()])
    nama = f"gempa_sesi_{sesi_id}.csv" if sesi_id else "gempa_semua.csv"
    return Response(buf.getvalue(), mimetype="text/csv",
                    headers={"Content-Disposition": f"attachment; filename={nama}"})


if __name__ == "__main__":
    muat_sesi_aktif()
    mulai_mqtt()
    # use_reloader=False: supaya klien MQTT tidak berjalan dua kali
    app.run(host="0.0.0.0", port=5000, threaded=True, use_reloader=False)
