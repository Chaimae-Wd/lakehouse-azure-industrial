SET search_path TO industrial;

-- =========================================================
-- 1. TABLE DES ATELIERS
-- =========================================================

CREATE TABLE IF NOT EXISTS workshops (
    workshop_id SERIAL PRIMARY KEY,
    workshop_name VARCHAR(100) NOT NULL UNIQUE,
    location VARCHAR(100) NOT NULL,
    description TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- =========================================================
-- 2. TABLE DES MACHINES
-- =========================================================

CREATE TABLE IF NOT EXISTS machines (
    machine_id SERIAL PRIMARY KEY,
    machine_name VARCHAR(100) NOT NULL UNIQUE,
    machine_type VARCHAR(100) NOT NULL,
    workshop_id INTEGER NOT NULL,
    installation_date DATE,
    status VARCHAR(30) NOT NULL DEFAULT 'ACTIVE',
    manufacturer VARCHAR(100),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_machine_workshop
        FOREIGN KEY (workshop_id)
        REFERENCES workshops(workshop_id),

    CONSTRAINT chk_machine_status
        CHECK (status IN ('ACTIVE', 'INACTIVE', 'MAINTENANCE'))
);

-- =========================================================
-- 3. TABLE DES TECHNICIENS
-- =========================================================

CREATE TABLE IF NOT EXISTS technicians (
    technician_id SERIAL PRIMARY KEY,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    specialty VARCHAR(100) NOT NULL,
    email VARCHAR(150) UNIQUE,
    phone VARCHAR(30),
    hire_date DATE,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- =========================================================
-- 4. TABLE DES CAPTEURS
-- =========================================================

CREATE TABLE IF NOT EXISTS sensors (
    sensor_id SERIAL PRIMARY KEY,
    sensor_code VARCHAR(50) NOT NULL UNIQUE,
    machine_id INTEGER NOT NULL,
    sensor_type VARCHAR(50) NOT NULL,
    measurement_unit VARCHAR(30) NOT NULL,
    minimum_threshold NUMERIC(12, 3),
    maximum_threshold NUMERIC(12, 3),
    status VARCHAR(30) NOT NULL DEFAULT 'ACTIVE',
    installation_date DATE,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_sensor_machine
        FOREIGN KEY (machine_id)
        REFERENCES machines(machine_id),

    CONSTRAINT chk_sensor_type
        CHECK (
            sensor_type IN (
                'TEMPERATURE',
                'VIBRATION',
                'PRESSURE',
                'ENERGY'
            )
        ),

    CONSTRAINT chk_sensor_status
        CHECK (status IN ('ACTIVE', 'INACTIVE', 'FAULTY')),

    CONSTRAINT chk_sensor_thresholds
        CHECK (
            minimum_threshold IS NULL
            OR maximum_threshold IS NULL
            OR minimum_threshold < maximum_threshold
        )
);

-- =========================================================
-- 5. TABLE DE PRODUCTION
-- =========================================================

CREATE TABLE IF NOT EXISTS production (
    production_id BIGSERIAL PRIMARY KEY,
    machine_id INTEGER NOT NULL,
    production_date DATE NOT NULL,
    shift VARCHAR(20) NOT NULL,
    quantity_produced NUMERIC(14, 3) NOT NULL,
    energy_consumption NUMERIC(14, 3) NOT NULL,
    operating_hours NUMERIC(8, 2) NOT NULL,
    rejected_quantity NUMERIC(14, 3) NOT NULL DEFAULT 0,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_production_machine
        FOREIGN KEY (machine_id)
        REFERENCES machines(machine_id),

    CONSTRAINT chk_production_shift
        CHECK (shift IN ('MORNING', 'AFTERNOON', 'NIGHT')),

    CONSTRAINT chk_quantity_produced
        CHECK (quantity_produced >= 0),

    CONSTRAINT chk_energy_consumption
        CHECK (energy_consumption >= 0),

    CONSTRAINT chk_operating_hours
        CHECK (operating_hours BETWEEN 0 AND 24),

    CONSTRAINT chk_rejected_quantity
        CHECK (
            rejected_quantity >= 0
            AND rejected_quantity <= quantity_produced
        )
);

-- =========================================================
-- 6. TABLE DE MAINTENANCE
-- =========================================================

CREATE TABLE IF NOT EXISTS maintenance (
    maintenance_id BIGSERIAL PRIMARY KEY,
    machine_id INTEGER NOT NULL,
    technician_id INTEGER NOT NULL,
    maintenance_type VARCHAR(30) NOT NULL,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    description TEXT,
    cost NUMERIC(14, 2) NOT NULL DEFAULT 0,
    status VARCHAR(30) NOT NULL DEFAULT 'PLANNED',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_maintenance_machine
        FOREIGN KEY (machine_id)
        REFERENCES machines(machine_id),

    CONSTRAINT fk_maintenance_technician
        FOREIGN KEY (technician_id)
        REFERENCES technicians(technician_id),

    CONSTRAINT chk_maintenance_type
        CHECK (
            maintenance_type IN (
                'PREVENTIVE',
                'CORRECTIVE',
                'EMERGENCY'
            )
        ),

    CONSTRAINT chk_maintenance_status
        CHECK (
            status IN (
                'PLANNED',
                'IN_PROGRESS',
                'COMPLETED',
                'CANCELLED'
            )
        ),

    CONSTRAINT chk_maintenance_dates
        CHECK (end_time IS NULL OR end_time >= start_time),

    CONSTRAINT chk_maintenance_cost
        CHECK (cost >= 0)
);

-- =========================================================
-- 7. TABLE DES ALERTES
-- =========================================================

CREATE TABLE IF NOT EXISTS alerts (
    alert_id BIGSERIAL PRIMARY KEY,
    sensor_id INTEGER NOT NULL,
    machine_id INTEGER NOT NULL,
    alert_timestamp TIMESTAMP NOT NULL,
    alert_type VARCHAR(50) NOT NULL,
    severity VARCHAR(20) NOT NULL,
    measured_value NUMERIC(14, 3) NOT NULL,
    threshold_value NUMERIC(14, 3),
    message TEXT,
    status VARCHAR(30) NOT NULL DEFAULT 'OPEN',
    resolved_at TIMESTAMP,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_alert_sensor
        FOREIGN KEY (sensor_id)
        REFERENCES sensors(sensor_id),

    CONSTRAINT fk_alert_machine
        FOREIGN KEY (machine_id)
        REFERENCES machines(machine_id),

    CONSTRAINT chk_alert_severity
        CHECK (severity IN ('LOW', 'MEDIUM', 'HIGH', 'CRITICAL')),

    CONSTRAINT chk_alert_status
        CHECK (status IN ('OPEN', 'ACKNOWLEDGED', 'RESOLVED')),

    CONSTRAINT chk_alert_resolution
        CHECK (
            resolved_at IS NULL
            OR resolved_at >= alert_timestamp
        )
);

-- =========================================================
-- INDEX UTILISÉS PAR LES FUTURS PIPELINES
-- =========================================================

CREATE INDEX IF NOT EXISTS idx_machines_workshop
    ON machines(workshop_id);

CREATE INDEX IF NOT EXISTS idx_sensors_machine
    ON sensors(machine_id);

CREATE INDEX IF NOT EXISTS idx_production_machine
    ON production(machine_id);

CREATE INDEX IF NOT EXISTS idx_production_date
    ON production(production_date);

CREATE INDEX IF NOT EXISTS idx_maintenance_machine
    ON maintenance(machine_id);

CREATE INDEX IF NOT EXISTS idx_maintenance_technician
    ON maintenance(technician_id);

CREATE INDEX IF NOT EXISTS idx_maintenance_start_time
    ON maintenance(start_time);

CREATE INDEX IF NOT EXISTS idx_alerts_sensor
    ON alerts(sensor_id);

CREATE INDEX IF NOT EXISTS idx_alerts_machine
    ON alerts(machine_id);

CREATE INDEX IF NOT EXISTS idx_alerts_timestamp
    ON alerts(alert_timestamp);