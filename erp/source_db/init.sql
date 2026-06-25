-- ERP/MES source database schema + seed.
-- Logical replication is enabled via command-line args in docker-compose
-- (wal_level=logical) so Debezium can capture changes.

-- ---------------------------------------------------------------- master data
CREATE TABLE products (
    product_id   SERIAL PRIMARY KEY,
    sku          TEXT NOT NULL UNIQUE,
    name         TEXT NOT NULL,
    family       TEXT NOT NULL,
    uom          TEXT NOT NULL DEFAULT 'EA',
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Manufacturing machines — conformed with EMS energy streams via energy_tag.
CREATE TABLE machines (
    machine_id    TEXT PRIMARY KEY,           -- e.g. BA-IMM-01 (== EMS energy_tag)
    machine_type  TEXT NOT NULL,              -- INJECTION_MOULDING | CNC | HEATING
    building      TEXT NOT NULL,
    energy_tag    TEXT NOT NULL,
    rated_power_kw NUMERIC(8,2),
    is_active     BOOLEAN NOT NULL DEFAULT TRUE,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE shifts (
    shift_id   SERIAL PRIMARY KEY,
    shift_code TEXT NOT NULL,                 -- A | B | C
    start_hour INT NOT NULL,
    end_hour   INT NOT NULL,
    crew       TEXT
);

-- --------------------------------------------------------------- transactional
CREATE TABLE work_orders (
    work_order_id SERIAL PRIMARY KEY,
    order_no      TEXT NOT NULL UNIQUE,
    product_id    INT  NOT NULL REFERENCES products(product_id),
    machine_id    TEXT NOT NULL REFERENCES machines(machine_id),
    planned_qty   INT  NOT NULL,
    due_date      DATE NOT NULL,
    status        TEXT NOT NULL DEFAULT 'PLANNED',  -- PLANNED|RELEASED|RUNNING|DONE
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE production_runs (
    run_id        SERIAL PRIMARY KEY,
    work_order_id INT  NOT NULL REFERENCES work_orders(work_order_id),
    machine_id    TEXT NOT NULL REFERENCES machines(machine_id),
    good_qty      INT  NOT NULL DEFAULT 0,
    scrap_qty     INT  NOT NULL DEFAULT 0,
    started_at    TIMESTAMPTZ,
    ended_at      TIMESTAMPTZ,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE machine_states (
    state_id    SERIAL PRIMARY KEY,
    machine_id  TEXT NOT NULL REFERENCES machines(machine_id),
    state       TEXT NOT NULL,               -- RUN | IDLE | DOWN
    started_at  TIMESTAMPTZ NOT NULL,
    ended_at    TIMESTAMPTZ
);

-- ---------------------------------------------------------------------- seeds
INSERT INTO products (sku, name, family) VALUES
    ('SKU-1001', 'Housing Cover',    'MOULDED'),
    ('SKU-1002', 'Gear Bracket',     'MACHINED'),
    ('SKU-1003', 'Heat Sink',        'MACHINED'),
    ('SKU-1004', 'Connector Body',   'MOULDED'),
    ('SKU-1005', 'Tempered Plate',   'HEAT_TREATED');

INSERT INTO machines (machine_id, machine_type, building, energy_tag, rated_power_kw) VALUES
    ('BA-IMM-01','INJECTION_MOULDING','Building-Alpha','BA-IMM-01',140.0),
    ('BA-IMM-02','INJECTION_MOULDING','Building-Alpha','BA-IMM-02',140.0),
    ('BA-IMM-03','INJECTION_MOULDING','Building-Alpha','BA-IMM-03',140.0),
    ('BA-CNC-01','CNC','Building-Alpha','BA-CNC-01',35.0),
    ('BA-CNC-02','CNC','Building-Alpha','BA-CNC-02',35.0),
    ('BA-CNC-03','CNC','Building-Alpha','BA-CNC-03',35.0),
    ('BA-HEAT-01','HEATING','Building-Alpha','BA-HEAT-01',220.0),
    ('BA-HEAT-02','HEATING','Building-Alpha','BA-HEAT-02',220.0),
    ('BB-IMM-01','INJECTION_MOULDING','Building-Beta','BB-IMM-01',140.0),
    ('BB-IMM-02','INJECTION_MOULDING','Building-Beta','BB-IMM-02',140.0),
    ('BB-CNC-01','CNC','Building-Beta','BB-CNC-01',35.0),
    ('BB-CNC-02','CNC','Building-Beta','BB-CNC-02',35.0),
    ('BB-HEAT-01','HEATING','Building-Beta','BB-HEAT-01',220.0),
    ('BG-IMM-01','INJECTION_MOULDING','Building-Gamma','BG-IMM-01',140.0),
    ('BG-IMM-02','INJECTION_MOULDING','Building-Gamma','BG-IMM-02',140.0),
    ('BG-CNC-01','CNC','Building-Gamma','BG-CNC-01',35.0),
    ('BG-CNC-02','CNC','Building-Gamma','BG-CNC-02',35.0),
    ('BG-HEAT-01','HEATING','Building-Gamma','BG-HEAT-01',220.0);

INSERT INTO shifts (shift_code, start_hour, end_hour, crew) VALUES
    ('A', 6, 14, 'Crew-1'),
    ('B', 14, 22, 'Crew-2'),
    ('C', 22, 6, 'Crew-3');

-- A few seed work orders so MES has schedule data immediately.
INSERT INTO work_orders (order_no, product_id, machine_id, planned_qty, due_date, status) VALUES
    ('WO-0001', 1, 'BA-IMM-01', 500, CURRENT_DATE + 1, 'RELEASED'),
    ('WO-0002', 2, 'BA-CNC-01', 200, CURRENT_DATE + 1, 'RUNNING'),
    ('WO-0003', 5, 'BA-HEAT-01', 80,  CURRENT_DATE + 2, 'PLANNED'),
    ('WO-0004', 4, 'BB-IMM-01', 350, CURRENT_DATE + 2, 'PLANNED'),
    ('WO-0005', 3, 'BG-CNC-01', 150, CURRENT_DATE + 3, 'PLANNED');

-- Mark tables for full-row CDC images.
ALTER TABLE products        REPLICA IDENTITY FULL;
ALTER TABLE machines        REPLICA IDENTITY FULL;
ALTER TABLE work_orders     REPLICA IDENTITY FULL;
ALTER TABLE production_runs REPLICA IDENTITY FULL;
ALTER TABLE machine_states  REPLICA IDENTITY FULL;
