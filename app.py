from flask import Flask, jsonify, request, render_template
import os
import socket
import psycopg2
import psycopg2.extras

app = Flask(__name__)

DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "postgres")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASS = os.getenv("DB_PASS", "")
NODE_NAME = os.getenv("NODE_NAME", socket.gethostname())

def get_conn():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        connect_timeout=3,
    )

def ensure_table():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS dr_events (
                    id BIGSERIAL PRIMARY KEY,
                    ts TIMESTAMPTZ NOT NULL DEFAULT now(),
                    node TEXT NOT NULL,
                    note TEXT
                );
            """)
        conn.commit()

@app.get("/health")
def health():
    return jsonify(status="ok", node=NODE_NAME), 200

@app.get("/status")
def status():
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT pg_is_in_recovery();")
                in_recovery = cur.fetchone()[0]
        return jsonify(
            node=NODE_NAME,
            db_host=DB_HOST,
            role=("standby" if in_recovery else "primary"),
        ), 200
    except Exception as e:
        return jsonify(node=NODE_NAME, error=str(e)), 500

@app.post("/write")
def write_event():
    note = (request.json or {}).get("note", "")
    try:
        ensure_table()
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO dr_events(node, note) VALUES (%s, %s) RETURNING id, ts;",
                    (NODE_NAME, note),
                )
                row = cur.fetchone()
            conn.commit()
        return jsonify(node=NODE_NAME, inserted_id=row[0], ts=str(row[1])), 200
    except Exception as e:
        #will fail on standby with "cannot execute INSERT in a read-only transaction"
        return jsonify(node=NODE_NAME, error=str(e)), 500

@app.get("/last")
def last_events():
    n = int(request.args.get("n", "5"))
    n = max(1, min(n, 50))
    try:
        ensure_table()
        with get_conn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    "SELECT id, ts, node, note FROM dr_events ORDER BY id DESC LIMIT %s;",
                    (n,),
                )
                rows = cur.fetchall()
        return jsonify(node=NODE_NAME, rows=rows), 200
    except Exception as e:
        return jsonify(node=NODE_NAME, error=str(e)), 500

@app.get("/")
def index():
    return render_template("index.html")


if __name__ == "__main__":
    #listen on all interfaces (not just VIP!!!)
    app.run(host="0.0.0.0", port=8080)
