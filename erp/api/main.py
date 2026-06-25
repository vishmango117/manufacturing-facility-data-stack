"""MES/ERP production-scheduling API.

Thin FastAPI service over the ERP source database. Writes here (create/update
work orders, post production runs, change machine state) land in Postgres and are
captured by Debezium as CDC events -> Kafka -> warehouse.
"""

from __future__ import annotations

import os
from contextlib import contextmanager
from datetime import date

import psycopg2
import psycopg2.extras
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

DSN = os.environ.get(
    "ERP_DSN",
    "host=erp-db port=5432 dbname=erp user=erp password=erp",
)

app = FastAPI(title="MES/ERP Scheduling API", version="1.0.0")


@contextmanager
def cursor():
    conn = psycopg2.connect(DSN)
    conn.autocommit = True
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            yield cur
    finally:
        conn.close()


# ----------------------------------------------------------------- schemas
class WorkOrderIn(BaseModel):
    order_no: str
    product_id: int
    machine_id: str
    planned_qty: int
    due_date: date
    status: str = "PLANNED"


class StatusIn(BaseModel):
    status: str


class ProductionRunIn(BaseModel):
    work_order_id: int
    machine_id: str
    good_qty: int = 0
    scrap_qty: int = 0


class MachineStateIn(BaseModel):
    machine_id: str
    state: str  # RUN | IDLE | DOWN


# ----------------------------------------------------------------- endpoints
@app.get("/health")
def health():
    with cursor() as cur:
        cur.execute("SELECT 1 AS ok")
        return {"status": "ok", "db": cur.fetchone()["ok"]}


@app.get("/machines")
def machines():
    with cursor() as cur:
        cur.execute("SELECT * FROM machines ORDER BY machine_id")
        return cur.fetchall()


@app.get("/schedule")
def schedule():
    with cursor() as cur:
        cur.execute("""
            SELECT w.*, p.sku, p.name AS product_name, m.machine_type, m.building
            FROM work_orders w
            JOIN products p ON p.product_id = w.product_id
            JOIN machines m ON m.machine_id = w.machine_id
            ORDER BY w.due_date, w.work_order_id
        """)
        return cur.fetchall()


@app.post("/work_orders", status_code=201)
def create_work_order(wo: WorkOrderIn):
    with cursor() as cur:
        try:
            cur.execute("""
                INSERT INTO work_orders (order_no, product_id, machine_id,
                                         planned_qty, due_date, status)
                VALUES (%s,%s,%s,%s,%s,%s)
                RETURNING *
            """, (wo.order_no, wo.product_id, wo.machine_id,
                  wo.planned_qty, wo.due_date, wo.status))
            return cur.fetchone()
        except psycopg2.Error as exc:
            raise HTTPException(status_code=400, detail=str(exc))


@app.patch("/work_orders/{work_order_id}/status")
def update_status(work_order_id: int, body: StatusIn):
    with cursor() as cur:
        cur.execute("""
            UPDATE work_orders SET status=%s, updated_at=now()
            WHERE work_order_id=%s RETURNING *
        """, (body.status, work_order_id))
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="work order not found")
        return row


@app.post("/production_runs", status_code=201)
def post_run(run: ProductionRunIn):
    with cursor() as cur:
        cur.execute("""
            INSERT INTO production_runs (work_order_id, machine_id, good_qty,
                                         scrap_qty, started_at, ended_at)
            VALUES (%s,%s,%s,%s, now(), now())
            RETURNING *
        """, (run.work_order_id, run.machine_id, run.good_qty, run.scrap_qty))
        return cur.fetchone()


@app.post("/machine_states", status_code=201)
def post_state(st: MachineStateIn):
    with cursor() as cur:
        cur.execute("""
            INSERT INTO machine_states (machine_id, state, started_at)
            VALUES (%s,%s, now()) RETURNING *
        """, (st.machine_id, st.state))
        return cur.fetchone()
