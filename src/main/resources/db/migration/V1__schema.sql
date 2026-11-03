CREATE EXTENSION IF NOT EXISTS btree_gist;

CREATE TABLE dealerships (
    id UUID PRIMARY KEY,
    name VARCHAR(160) NOT NULL,
    timezone VARCHAR(80) NOT NULL,
    open_time TIME NOT NULL,
    close_time TIME NOT NULL,
    CONSTRAINT dealerships_open_before_close CHECK (open_time < close_time)
);

CREATE TABLE service_types (
    id UUID PRIMARY KEY,
    code VARCHAR(64) NOT NULL UNIQUE,
    name VARCHAR(160) NOT NULL,
    duration_minutes INTEGER NOT NULL CHECK (duration_minutes > 0),
    required_skill VARCHAR(64) NOT NULL
);

CREATE TABLE service_bays (
    id UUID PRIMARY KEY,
    dealership_id UUID NOT NULL REFERENCES dealerships(id),
    bay_code VARCHAR(32) NOT NULL,
    status VARCHAR(16) NOT NULL CHECK (status IN ('ACTIVE', 'MAINTENANCE')),
    CONSTRAINT service_bays_dealership_code_unique UNIQUE (dealership_id, bay_code),
    CONSTRAINT service_bays_id_dealership_unique UNIQUE (id, dealership_id)
);

CREATE TABLE technicians (
    id UUID PRIMARY KEY,
    dealership_id UUID NOT NULL REFERENCES dealerships(id),
    full_name VARCHAR(160) NOT NULL,
    status VARCHAR(16) NOT NULL CHECK (status IN ('ACTIVE', 'INACTIVE')),
    CONSTRAINT technicians_id_dealership_unique UNIQUE (id, dealership_id)
);

CREATE TABLE technician_skills (
    technician_id UUID NOT NULL REFERENCES technicians(id) ON DELETE CASCADE,
    skill_code VARCHAR(64) NOT NULL,
    PRIMARY KEY (technician_id, skill_code)
);

CREATE TABLE appointments (
    id UUID PRIMARY KEY,
    dealership_id UUID NOT NULL REFERENCES dealerships(id),
    service_type_id UUID NOT NULL REFERENCES service_types(id),
    service_bay_id UUID NOT NULL,
    technician_id UUID NOT NULL,
    customer_id VARCHAR(80) NOT NULL,
    vehicle_vin VARCHAR(17) NOT NULL,
    start_time TIMESTAMPTZ NOT NULL,
    end_time TIMESTAMPTZ NOT NULL,
    status VARCHAR(16) NOT NULL CHECK (status = 'CONFIRMED'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT appointments_positive_interval CHECK (end_time > start_time),
    CONSTRAINT appointments_valid_vin CHECK (vehicle_vin ~ '^[A-HJ-NPR-Z0-9]{17}$'),
    CONSTRAINT appointments_bay_same_dealership FOREIGN KEY (service_bay_id, dealership_id)
        REFERENCES service_bays(id, dealership_id),
    CONSTRAINT appointments_technician_same_dealership FOREIGN KEY (technician_id, dealership_id)
        REFERENCES technicians(id, dealership_id),
    CONSTRAINT appointments_bay_no_overlap EXCLUDE USING gist
        (service_bay_id WITH =, tstzrange(start_time, end_time, '[)') WITH &&),
    CONSTRAINT appointments_technician_no_overlap EXCLUDE USING gist
        (technician_id WITH =, tstzrange(start_time, end_time, '[)') WITH &&),
    CONSTRAINT appointments_vin_no_overlap EXCLUDE USING gist
        (vehicle_vin WITH =, tstzrange(start_time, end_time, '[)') WITH &&)
);

CREATE INDEX appointments_dealership_start_idx ON appointments (dealership_id, start_time);
