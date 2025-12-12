-- MetroPulse DWH - STAGING LAYER
-- PostgreSQL
-- Описание: Staging таблицы для временного хранения сырых данных из источников

-- Создание схемы staging
CREATE SCHEMA IF NOT EXISTS staging;

-- Staging: Пользователи
CREATE TABLE staging.stg_users (
    user_id BIGINT,
    username VARCHAR(50),
    email VARCHAR(100),
    phone VARCHAR(20),
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    date_of_birth DATE,
    registration_date TIMESTAMP,
    status VARCHAR(20),
    balance DECIMAL(10, 2),
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    load_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_system VARCHAR(50) DEFAULT 'OLTP'
);

CREATE INDEX idx_stg_users_load_date ON staging.stg_users(load_date);

COMMENT ON TABLE staging.stg_users IS 'Staging таблица для пользователей из OLTP';

-- Staging: Маршруты
CREATE TABLE staging.stg_routes (
    route_id VARCHAR(20),
    route_number VARCHAR(10),
    route_name VARCHAR(200),
    transport_type VARCHAR(20),
    base_fare DECIMAL(10, 2),
    is_active BOOLEAN,
    description TEXT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    load_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_system VARCHAR(50) DEFAULT 'OLTP'
);

CREATE INDEX idx_stg_routes_load_date ON staging.stg_routes(load_date);

COMMENT ON TABLE staging.stg_routes IS 'Staging таблица для маршрутов из OLTP';

-- Staging: Станции
CREATE TABLE staging.stg_stations (
    station_id VARCHAR(20),
    station_name VARCHAR(200),
    latitude DECIMAL(10, 8),
    longitude DECIMAL(11, 8),
    station_type VARCHAR(20),
    district VARCHAR(100),
    zone VARCHAR(10),
    is_active BOOLEAN,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    load_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_system VARCHAR(50) DEFAULT 'OLTP'
);

CREATE INDEX idx_stg_stations_load_date ON staging.stg_stations(load_date);

COMMENT ON TABLE staging.stg_stations IS 'Staging таблица для станций из OLTP';

-- Staging: Транспортные средства
CREATE TABLE staging.stg_vehicles (
    vehicle_id VARCHAR(20),
    vehicle_type VARCHAR(20),
    route_id VARCHAR(20),
    capacity INTEGER,
    manufacture_year INTEGER,
    model VARCHAR(100),
    license_plate VARCHAR(20),
    last_maintenance_date DATE,
    status VARCHAR(20),
    created_at TIMESTAMP,
    load_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_system VARCHAR(50) DEFAULT 'OLTP'
);

CREATE INDEX idx_stg_vehicles_load_date ON staging.stg_vehicles(load_date);

COMMENT ON TABLE staging.stg_vehicles IS 'Staging таблица для транспортных средств из OLTP';

-- Staging: Поездки
CREATE TABLE staging.stg_trips (
    trip_id BIGINT,
    user_id BIGINT,
    route_id VARCHAR(20),
    vehicle_id VARCHAR(20),
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    start_station_id VARCHAR(20),
    end_station_id VARCHAR(20),
    distance_km DECIMAL(6, 2),
    status VARCHAR(20),
    created_at TIMESTAMP,
    load_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_system VARCHAR(50) DEFAULT 'OLTP'
);

CREATE INDEX idx_stg_trips_load_date ON staging.stg_trips(load_date);
CREATE INDEX idx_stg_trips_start_time ON staging.stg_trips(start_time);

COMMENT ON TABLE staging.stg_trips IS 'Staging таблица для поездок из OLTP (incremental)';

-- Staging: Транзакции
CREATE TABLE staging.stg_transactions (
    transaction_id BIGINT,
    user_id BIGINT,
    trip_id BIGINT,
    amount DECIMAL(10, 2),
    payment_method VARCHAR(20),
    transaction_type VARCHAR(20),
    transaction_date TIMESTAMP,
    status VARCHAR(20),
    description TEXT,
    created_at TIMESTAMP,
    load_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_system VARCHAR(50) DEFAULT 'OLTP'
);

CREATE INDEX idx_stg_transactions_load_date ON staging.stg_transactions(load_date);
CREATE INDEX idx_stg_transactions_date ON staging.stg_transactions(transaction_date);

COMMENT ON TABLE staging.stg_transactions IS 'Staging таблица для транзакций из OLTP (incremental)';

-- ============================================================================
-- STAGING TABLES - Из Kafka Stream
-- ============================================================================

-- Staging: Позиции транспорта (из Kafka)
CREATE TABLE staging.stg_vehicle_positions (
    position_id BIGSERIAL PRIMARY KEY,
    vehicle_id VARCHAR(20),
    route_id VARCHAR(20),
    latitude DECIMAL(10, 8),
    longitude DECIMAL(11, 8),
    speed DECIMAL(5, 2),
    bearing INTEGER,
    occupancy VARCHAR(50),
    event_timestamp TIMESTAMP,
    load_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_system VARCHAR(50) DEFAULT 'KAFKA',
    kafka_topic VARCHAR(100),
    kafka_partition INTEGER,
    kafka_offset BIGINT
);

-- Партиционирование по дням для эффективного хранения больших объемов
-- CREATE TABLE staging.stg_vehicle_positions_2025_12_12 PARTITION OF staging.stg_vehicle_positions
-- FOR VALUES FROM ('2025-12-12 00:00:00') TO ('2025-12-13 00:00:00');

CREATE INDEX idx_stg_positions_vehicle ON staging.stg_vehicle_positions(vehicle_id, event_timestamp);
CREATE INDEX idx_stg_positions_route ON staging.stg_vehicle_positions(route_id, event_timestamp);
CREATE INDEX idx_stg_positions_event_time ON staging.stg_vehicle_positions(event_timestamp);
CREATE INDEX idx_stg_positions_load_date ON staging.stg_vehicle_positions(load_date);

COMMENT ON TABLE staging.stg_vehicle_positions IS 'Staging таблица для позиций транспорта из Kafka stream';

-- ============================================================================
-- METADATA и CONTROL TABLES
-- ============================================================================

-- Таблица: Контроль загрузок
CREATE TABLE staging.load_control (
    load_id BIGSERIAL PRIMARY KEY,
    source_system VARCHAR(50) NOT NULL,
    table_name VARCHAR(100) NOT NULL,
    load_type VARCHAR(20) NOT NULL CHECK (load_type IN ('full', 'incremental', 'stream')),
    load_start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    load_end_time TIMESTAMP,
    rows_inserted INTEGER,
    rows_updated INTEGER,
    rows_deleted INTEGER,
    status VARCHAR(20) DEFAULT 'running' CHECK (status IN ('running', 'success', 'failed')),
    error_message TEXT,
    last_extracted_timestamp TIMESTAMP,
    watermark_value TIMESTAMP,
    comments TEXT
);

CREATE INDEX idx_load_control_status ON staging.load_control(status, load_start_time DESC);
CREATE INDEX idx_load_control_table ON staging.load_control(table_name, load_start_time DESC);

COMMENT ON TABLE staging.load_control IS 'Метаданные и контроль ETL процессов';

-- Таблица: Data Quality проверки
CREATE TABLE staging.data_quality_checks (
    check_id BIGSERIAL PRIMARY KEY,
    load_id BIGINT,
    table_name VARCHAR(100) NOT NULL,
    check_type VARCHAR(50) NOT NULL,
    check_name VARCHAR(200) NOT NULL,
    check_query TEXT,
    expected_value VARCHAR(100),
    actual_value VARCHAR(100),
    check_result VARCHAR(20) CHECK (check_result IN ('passed', 'failed', 'warning')),
    check_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    details TEXT,
    FOREIGN KEY (load_id) REFERENCES staging.load_control(load_id) ON DELETE CASCADE
);

CREATE INDEX idx_dq_checks_load ON staging.data_quality_checks(load_id);
CREATE INDEX idx_dq_checks_result ON staging.data_quality_checks(check_result, check_timestamp DESC);

COMMENT ON TABLE staging.data_quality_checks IS 'Результаты проверок качества данных';

-- ============================================================================
-- ПРОЦЕДУРЫ для очистки Staging
-- ============================================================================

-- Функция: Очистка старых staging данных
CREATE OR REPLACE FUNCTION staging.cleanup_staging_data(retention_days INTEGER DEFAULT 7)
RETURNS TABLE (
    table_name TEXT,
    rows_deleted BIGINT
) AS $$
BEGIN
    -- Очистка stg_users
    DELETE FROM staging.stg_users WHERE load_date < CURRENT_DATE - retention_days;
    table_name := 'stg_users';
    rows_deleted := (SELECT COUNT(*) FROM staging.stg_users WHERE load_date < CURRENT_DATE - retention_days);
    RETURN NEXT;

    -- Очистка stg_routes
    DELETE FROM staging.stg_routes WHERE load_date < CURRENT_DATE - retention_days;
    table_name := 'stg_routes';
    rows_deleted := (SELECT COUNT(*) FROM staging.stg_routes WHERE load_date < CURRENT_DATE - retention_days);
    RETURN NEXT;

    -- Очистка stg_stations
    DELETE FROM staging.stg_stations WHERE load_date < CURRENT_DATE - retention_days;
    table_name := 'stg_stations';
    rows_deleted := (SELECT COUNT(*) FROM staging.stg_stations WHERE load_date < CURRENT_DATE - retention_days);
    RETURN NEXT;

    -- Очистка stg_vehicles
    DELETE FROM staging.stg_vehicles WHERE load_date < CURRENT_DATE - retention_days;
    table_name := 'stg_vehicles';
    rows_deleted := (SELECT COUNT(*) FROM staging.stg_vehicles WHERE load_date < CURRENT_DATE - retention_days);
    RETURN NEXT;

    -- Очистка stg_trips
    DELETE FROM staging.stg_trips WHERE load_date < CURRENT_DATE - retention_days;
    table_name := 'stg_trips';
    rows_deleted := (SELECT COUNT(*) FROM staging.stg_trips WHERE load_date < CURRENT_DATE - retention_days);
    RETURN NEXT;

    -- Очистка stg_transactions
    DELETE FROM staging.stg_transactions WHERE load_date < CURRENT_DATE - retention_days;
    table_name := 'stg_transactions';
    rows_deleted := (SELECT COUNT(*) FROM staging.stg_transactions WHERE load_date < CURRENT_DATE - retention_days);
    RETURN NEXT;

    -- Очистка stg_vehicle_positions (сохраняем только 3 дня для stream данных)
    DELETE FROM staging.stg_vehicle_positions WHERE load_date < CURRENT_DATE - 3;
    table_name := 'stg_vehicle_positions';
    rows_deleted := (SELECT COUNT(*) FROM staging.stg_vehicle_positions WHERE load_date < CURRENT_DATE - 3);
    RETURN NEXT;

    RETURN;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION staging.cleanup_staging_data IS 'Очистка старых данных из staging таблиц';

-- Функция: Получение последнего watermark для incremental load
CREATE OR REPLACE FUNCTION staging.get_last_watermark(
    p_source_system VARCHAR,
    p_table_name VARCHAR
) RETURNS TIMESTAMP AS $$
DECLARE
    v_watermark TIMESTAMP;
BEGIN
    SELECT watermark_value
    INTO v_watermark
    FROM staging.load_control
    WHERE source_system = p_source_system
      AND table_name = p_table_name
      AND status = 'success'
    ORDER BY load_end_time DESC
    LIMIT 1;

    RETURN COALESCE(v_watermark, '1900-01-01'::TIMESTAMP);
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION staging.get_last_watermark IS 'Получение последнего watermark для инкрементальной загрузки';

-- Функция: Запись начала загрузки
CREATE OR REPLACE FUNCTION staging.start_load(
    p_source_system VARCHAR,
    p_table_name VARCHAR,
    p_load_type VARCHAR
) RETURNS BIGINT AS $$
DECLARE
    v_load_id BIGINT;
BEGIN
    INSERT INTO staging.load_control (
        source_system,
        table_name,
        load_type,
        load_start_time,
        status
    ) VALUES (
        p_source_system,
        p_table_name,
        p_load_type,
        CURRENT_TIMESTAMP,
        'running'
    ) RETURNING load_id INTO v_load_id;

    RETURN v_load_id;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION staging.start_load IS 'Регистрация начала процесса загрузки';

-- Функция: Запись завершения загрузки
CREATE OR REPLACE FUNCTION staging.end_load(
    p_load_id BIGINT,
    p_status VARCHAR,
    p_rows_inserted INTEGER DEFAULT 0,
    p_rows_updated INTEGER DEFAULT 0,
    p_rows_deleted INTEGER DEFAULT 0,
    p_watermark TIMESTAMP DEFAULT NULL,
    p_error_message TEXT DEFAULT NULL
) RETURNS VOID AS $$
BEGIN
    UPDATE staging.load_control
    SET load_end_time = CURRENT_TIMESTAMP,
        status = p_status,
        rows_inserted = p_rows_inserted,
        rows_updated = p_rows_updated,
        rows_deleted = p_rows_deleted,
        watermark_value = p_watermark,
        error_message = p_error_message
    WHERE load_id = p_load_id;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION staging.end_load IS 'Регистрация завершения процесса загрузки';

-- ============================================================================
-- ПРЕДСТАВЛЕНИЯ для мониторинга
-- ============================================================================

-- Представление: Статус последних загрузок
CREATE OR REPLACE VIEW staging.recent_loads AS
SELECT
    load_id,
    source_system,
    table_name,
    load_type,
    load_start_time,
    load_end_time,
    EXTRACT(EPOCH FROM (load_end_time - load_start_time))/60 as duration_minutes,
    rows_inserted,
    rows_updated,
    rows_deleted,
    status,
    error_message
FROM staging.load_control
WHERE load_start_time > CURRENT_TIMESTAMP - INTERVAL '7 days'
ORDER BY load_start_time DESC;

COMMENT ON VIEW staging.recent_loads IS 'Последние загрузки за 7 дней';

-- Представление: Статистика по staging таблицам
CREATE OR REPLACE VIEW staging.staging_statistics AS
SELECT
    'stg_users' as table_name,
    COUNT(*) as row_count,
    MIN(load_date) as oldest_load,
    MAX(load_date) as latest_load
FROM staging.stg_users
UNION ALL
SELECT
    'stg_routes',
    COUNT(*),
    MIN(load_date),
    MAX(load_date)
FROM staging.stg_routes
UNION ALL
SELECT
    'stg_stations',
    COUNT(*),
    MIN(load_date),
    MAX(load_date)
FROM staging.stg_stations
UNION ALL
SELECT
    'stg_vehicles',
    COUNT(*),
    MIN(load_date),
    MAX(load_date)
FROM staging.stg_vehicles
UNION ALL
SELECT
    'stg_trips',
    COUNT(*),
    MIN(load_date),
    MAX(load_date)
FROM staging.stg_trips
UNION ALL
SELECT
    'stg_transactions',
    COUNT(*),
    MIN(load_date),
    MAX(load_date)
FROM staging.stg_transactions
UNION ALL
SELECT
    'stg_vehicle_positions',
    COUNT(*),
    MIN(load_date),
    MAX(load_date)
FROM staging.stg_vehicle_positions;

COMMENT ON VIEW staging.staging_statistics IS 'Статистика по staging таблицам';

-- ============================================================================
-- GRANTS
-- ============================================================================

-- CREATE ROLE staging_loader WITH LOGIN PASSWORD 'loader_password';
-- GRANT USAGE ON SCHEMA staging TO staging_loader;
-- GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA staging TO staging_loader;
-- GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA staging TO staging_loader;
-- GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA staging TO staging_loader;

