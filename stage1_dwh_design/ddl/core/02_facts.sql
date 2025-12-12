-- MetroPulse DWH - CORE LAYER: FACTS
-- PostgreSQL
-- Описание: Таблицы фактов (Kimball Star Schema)

-- FACT TABLES (Таблицы фактов)

-- Факт: Позиции транспорта (из Kafka stream)
CREATE TABLE dwh.fact_vehicle_positions (
    position_key BIGSERIAL PRIMARY KEY,  -- Surrogate Key

    -- Foreign Keys (измерения)
    vehicle_key BIGINT NOT NULL,
    route_key BIGINT NOT NULL,
    date_key INTEGER NOT NULL,
    time_key INTEGER NOT NULL,
    occupancy_key INTEGER,

    -- Measures (метрики)
    latitude DECIMAL(10, 8) NOT NULL,
    longitude DECIMAL(11, 8) NOT NULL,
    speed DECIMAL(5, 2),  -- км/ч
    bearing INTEGER,  -- градусы 0-360

    -- Degenerate Dimensions (вырожденные измерения)
    event_timestamp TIMESTAMP NOT NULL,

    -- Metadata
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_system VARCHAR(50) DEFAULT 'KAFKA',

    -- Foreign Key Constraints
    FOREIGN KEY (vehicle_key) REFERENCES dwh.dim_vehicle(vehicle_key),
    FOREIGN KEY (route_key) REFERENCES dwh.dim_route(route_key),
    FOREIGN KEY (date_key) REFERENCES dwh.dim_date(date_key),
    FOREIGN KEY (time_key) REFERENCES dwh.dim_time(time_key),
    FOREIGN KEY (occupancy_key) REFERENCES dwh.dim_occupancy_level(occupancy_key)
) PARTITION BY RANGE (date_key);

-- Создание партиций по дням (пример для декабря 2025)
CREATE TABLE dwh.fact_vehicle_positions_202512 PARTITION OF dwh.fact_vehicle_positions
    FOR VALUES FROM (20251201) TO (20260101);

-- Создание индексов на партициях
CREATE INDEX idx_fact_positions_vehicle ON dwh.fact_vehicle_positions(vehicle_key, date_key, time_key);
CREATE INDEX idx_fact_positions_route ON dwh.fact_vehicle_positions(route_key, date_key, time_key);
CREATE INDEX idx_fact_positions_date_time ON dwh.fact_vehicle_positions(date_key, time_key);
CREATE INDEX idx_fact_positions_timestamp ON dwh.fact_vehicle_positions(event_timestamp);

COMMENT ON TABLE dwh.fact_vehicle_positions IS 'Факты позиций транспорта в реальном времени';
COMMENT ON COLUMN dwh.fact_vehicle_positions.speed IS 'Скорость в км/ч';
COMMENT ON COLUMN dwh.fact_vehicle_positions.bearing IS 'Направление движения в градусах (0-360)';

-- Факт: Поездки пользователей
CREATE TABLE dwh.fact_trips (
    trip_key BIGSERIAL PRIMARY KEY,  -- Surrogate Key

    -- Foreign Keys (измерения)
    user_key BIGINT NOT NULL,
    route_key BIGINT NOT NULL,
    vehicle_key BIGINT,
    start_date_key INTEGER NOT NULL,
    start_time_key INTEGER NOT NULL,
    end_date_key INTEGER,
    end_time_key INTEGER,
    start_station_key BIGINT,
    end_station_key BIGINT,

    -- Measures (метрики)
    distance_km DECIMAL(6, 2),
    duration_minutes INTEGER,  -- Вычисляемое: разница между start и end
    actual_fare DECIMAL(10, 2),  -- Фактическая стоимость (может отличаться от base_fare)

    -- Degenerate Dimensions (вырожденные измерения)
    trip_id BIGINT NOT NULL,  -- Business Key из OLTP
    trip_status VARCHAR(20),  -- completed, cancelled
    start_timestamp TIMESTAMP NOT NULL,
    end_timestamp TIMESTAMP,

    -- Metadata
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_system VARCHAR(50) DEFAULT 'OLTP',

    -- Foreign Key Constraints
    FOREIGN KEY (user_key) REFERENCES dwh.dim_user(user_key),
    FOREIGN KEY (route_key) REFERENCES dwh.dim_route(route_key),
    FOREIGN KEY (vehicle_key) REFERENCES dwh.dim_vehicle(vehicle_key),
    FOREIGN KEY (start_date_key) REFERENCES dwh.dim_date(date_key),
    FOREIGN KEY (start_time_key) REFERENCES dwh.dim_time(time_key),
    FOREIGN KEY (end_date_key) REFERENCES dwh.dim_date(date_key),
    FOREIGN KEY (end_time_key) REFERENCES dwh.dim_time(time_key),
    FOREIGN KEY (start_station_key) REFERENCES dwh.dim_station(station_key),
    FOREIGN KEY (end_station_key) REFERENCES dwh.dim_station(station_key)
) PARTITION BY RANGE (start_date_key);

-- Создание партиций по месяцам
CREATE TABLE dwh.fact_trips_202512 PARTITION OF dwh.fact_trips
    FOR VALUES FROM (20251201) TO (20260101);

CREATE TABLE dwh.fact_trips_202601 PARTITION OF dwh.fact_trips
    FOR VALUES FROM (20260101) TO (20260201);

-- Создание индексов
CREATE INDEX idx_fact_trips_user ON dwh.fact_trips(user_key, start_date_key);
CREATE INDEX idx_fact_trips_route ON dwh.fact_trips(route_key, start_date_key);
CREATE INDEX idx_fact_trips_vehicle ON dwh.fact_trips(vehicle_key, start_date_key);
CREATE INDEX idx_fact_trips_start_date ON dwh.fact_trips(start_date_key, start_time_key);
CREATE INDEX idx_fact_trips_status ON dwh.fact_trips(trip_status);
CREATE INDEX idx_fact_trips_trip_id ON dwh.fact_trips(trip_id);
CREATE INDEX idx_fact_trips_stations ON dwh.fact_trips(start_station_key, end_station_key);

COMMENT ON TABLE dwh.fact_trips IS 'Факты поездок пользователей';
COMMENT ON COLUMN dwh.fact_trips.duration_minutes IS 'Длительность поездки в минутах';
COMMENT ON COLUMN dwh.fact_trips.actual_fare IS 'Фактическая стоимость поездки (может отличаться от базового тарифа)';

-- Факт: Транзакции (платежи)
CREATE TABLE dwh.fact_transactions (
    transaction_key BIGSERIAL PRIMARY KEY,  -- Surrogate Key

    -- Foreign Keys (измерения)
    user_key BIGINT NOT NULL,
    trip_key BIGINT,  -- NULL для пополнений баланса
    payment_method_key INTEGER NOT NULL,
    date_key INTEGER NOT NULL,
    time_key INTEGER NOT NULL,

    -- Measures (метрики)
    amount DECIMAL(10, 2) NOT NULL,

    -- Degenerate Dimensions (вырожденные измерения)
    transaction_id BIGINT NOT NULL,  -- Business Key из OLTP
    transaction_type VARCHAR(20) NOT NULL,  -- payment, refund, topup
    transaction_status VARCHAR(20) NOT NULL,  -- success, failed, pending, refunded
    transaction_timestamp TIMESTAMP NOT NULL,
    description TEXT,

    -- Metadata
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_system VARCHAR(50) DEFAULT 'OLTP',

    -- Foreign Key Constraints
    FOREIGN KEY (user_key) REFERENCES dwh.dim_user(user_key),
    FOREIGN KEY (trip_key) REFERENCES dwh.fact_trips(trip_key),
    FOREIGN KEY (payment_method_key) REFERENCES dwh.dim_payment_method(payment_method_key),
    FOREIGN KEY (date_key) REFERENCES dwh.dim_date(date_key),
    FOREIGN KEY (time_key) REFERENCES dwh.dim_time(time_key)
) PARTITION BY RANGE (date_key);

-- Создание партиций по месяцам
CREATE TABLE dwh.fact_transactions_202512 PARTITION OF dwh.fact_transactions
    FOR VALUES FROM (20251201) TO (20260101);

CREATE TABLE dwh.fact_transactions_202601 PARTITION OF dwh.fact_transactions
    FOR VALUES FROM (20260101) TO (20260201);

-- Создание индексов
CREATE INDEX idx_fact_trans_user ON dwh.fact_transactions(user_key, date_key);
CREATE INDEX idx_fact_trans_trip ON dwh.fact_transactions(trip_key);
CREATE INDEX idx_fact_trans_payment_method ON dwh.fact_transactions(payment_method_key);
CREATE INDEX idx_fact_trans_date ON dwh.fact_transactions(date_key, time_key);
CREATE INDEX idx_fact_trans_type ON dwh.fact_transactions(transaction_type);
CREATE INDEX idx_fact_trans_status ON dwh.fact_transactions(transaction_status);
CREATE INDEX idx_fact_trans_trans_id ON dwh.fact_transactions(transaction_id);
CREATE INDEX idx_fact_trans_amount ON dwh.fact_transactions(amount);

COMMENT ON TABLE dwh.fact_transactions IS 'Факты финансовых транзакций';

-- ============================================================================
-- АГРЕГИРОВАННЫЕ ФАКТЫ (для ускорения типовых запросов)
-- ============================================================================

-- Агрегат: Ежечасная статистика по маршрутам
CREATE TABLE dwh.fact_route_hourly_stats (
    stats_key BIGSERIAL PRIMARY KEY,

    -- Dimensions
    route_key BIGINT NOT NULL,
    date_key INTEGER NOT NULL,
    hour INTEGER NOT NULL,  -- 0-23

    -- Measures
    total_trips INTEGER DEFAULT 0,
    total_passengers INTEGER DEFAULT 0,
    total_distance_km DECIMAL(10, 2) DEFAULT 0,
    total_revenue DECIMAL(12, 2) DEFAULT 0,
    avg_speed DECIMAL(5, 2),
    avg_trip_duration_minutes DECIMAL(6, 2),
    avg_occupancy_level DECIMAL(3, 2),
    active_vehicles_count INTEGER DEFAULT 0,

    -- Metadata
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- Unique constraint
    CONSTRAINT uk_route_hourly UNIQUE (route_key, date_key, hour),

    -- Foreign Keys
    FOREIGN KEY (route_key) REFERENCES dwh.dim_route(route_key),
    FOREIGN KEY (date_key) REFERENCES dwh.dim_date(date_key)
);

CREATE INDEX idx_fact_route_hourly_route ON dwh.fact_route_hourly_stats(route_key, date_key);
CREATE INDEX idx_fact_route_hourly_date ON dwh.fact_route_hourly_stats(date_key, hour);

COMMENT ON TABLE dwh.fact_route_hourly_stats IS 'Агрегированная статистика по маршрутам за час';

-- Агрегат: Ежедневная статистика по пользователям
CREATE TABLE dwh.fact_user_daily_stats (
    stats_key BIGSERIAL PRIMARY KEY,

    -- Dimensions
    user_key BIGINT NOT NULL,
    date_key INTEGER NOT NULL,

    -- Measures
    trips_count INTEGER DEFAULT 0,
    total_distance_km DECIMAL(8, 2) DEFAULT 0,
    total_spent DECIMAL(10, 2) DEFAULT 0,
    unique_routes_count INTEGER DEFAULT 0,
    avg_trip_duration_minutes DECIMAL(6, 2),

    -- Metadata
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- Unique constraint
    CONSTRAINT uk_user_daily UNIQUE (user_key, date_key),

    -- Foreign Keys
    FOREIGN KEY (user_key) REFERENCES dwh.dim_user(user_key),
    FOREIGN KEY (date_key) REFERENCES dwh.dim_date(date_key)
);

CREATE INDEX idx_fact_user_daily_user ON dwh.fact_user_daily_stats(user_key, date_key DESC);
CREATE INDEX idx_fact_user_daily_date ON dwh.fact_user_daily_stats(date_key);

COMMENT ON TABLE dwh.fact_user_daily_stats IS 'Агрегированная статистика по пользователям за день';

-- ============================================================================
-- ПРЕДСТАВЛЕНИЯ для аналитики
-- ============================================================================

-- Представление: Завершенные поездки с деталями
CREATE OR REPLACE VIEW dwh.v_fact_trips_completed AS
SELECT
    ft.trip_key,
    ft.trip_id,

    -- User info
    du.user_id,
    du.username,
    du.user_segment,

    -- Route info
    dr.route_id,
    dr.route_number,
    dr.route_name,
    dr.transport_type,
    dr.base_fare,

    -- Vehicle info
    dv.vehicle_id,
    dv.vehicle_type,

    -- Station info
    ss.station_name as start_station,
    es.station_name as end_station,

    -- Date/Time info
    dd_start.date as trip_date,
    dd_start.day_name,
    dd_start.is_weekend,
    dt_start.hour as start_hour,
    dt_start.time_period,
    dt_start.is_rush_hour,

    -- Measures
    ft.distance_km,
    ft.duration_minutes,
    ft.actual_fare,
    ft.start_timestamp,
    ft.end_timestamp

FROM dwh.fact_trips ft
JOIN dwh.dim_user du ON ft.user_key = du.user_key
JOIN dwh.dim_route dr ON ft.route_key = dr.route_key
LEFT JOIN dwh.dim_vehicle dv ON ft.vehicle_key = dv.vehicle_key
LEFT JOIN dwh.dim_station ss ON ft.start_station_key = ss.station_key
LEFT JOIN dwh.dim_station es ON ft.end_station_key = es.station_key
JOIN dwh.dim_date dd_start ON ft.start_date_key = dd_start.date_key
JOIN dwh.dim_time dt_start ON ft.start_time_key = dt_start.time_key
WHERE ft.trip_status = 'completed';

COMMENT ON VIEW dwh.v_fact_trips_completed IS 'Завершенные поездки со всеми деталями для анализа';

-- Представление: Успешные транзакции
CREATE OR REPLACE VIEW dwh.v_fact_transactions_success AS
SELECT
    ftr.transaction_key,
    ftr.transaction_id,

    -- User info
    du.user_id,
    du.username,

    -- Payment method
    dpm.payment_method_name,
    dpm.payment_category,

    -- Date/Time
    dd.date as transaction_date,
    dd.day_name,
    dd.is_weekend,
    dt.hour,

    -- Measures
    ftr.amount,
    ftr.transaction_type,
    ftr.transaction_timestamp

FROM dwh.fact_transactions ftr
JOIN dwh.dim_user du ON ftr.user_key = du.user_key
JOIN dwh.dim_payment_method dpm ON ftr.payment_method_key = dpm.payment_method_key
JOIN dwh.dim_date dd ON ftr.date_key = dd.date_key
JOIN dwh.dim_time dt ON ftr.time_key = dt.time_key
WHERE ftr.transaction_status = 'success';

COMMENT ON VIEW dwh.v_fact_transactions_success IS 'Успешные транзакции для анализа выручки';

-- Представление: Последние позиции транспорта
CREATE OR REPLACE VIEW dwh.v_fact_positions_latest AS
WITH latest_positions AS (
    SELECT
        vehicle_key,
        MAX(event_timestamp) as latest_timestamp
    FROM dwh.fact_vehicle_positions
    WHERE event_timestamp > CURRENT_TIMESTAMP - INTERVAL '1 hour'
    GROUP BY vehicle_key
)
SELECT
    fp.position_key,

    -- Vehicle info
    dv.vehicle_id,
    dv.vehicle_type,

    -- Route info
    dr.route_id,
    dr.route_number,
    dr.route_name,

    -- Position data
    fp.latitude,
    fp.longitude,
    fp.speed,
    fp.bearing,

    -- Occupancy
    dol.occupancy_name,
    dol.occupancy_level,
    dol.color_code,

    -- Timestamp
    fp.event_timestamp,
    EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - fp.event_timestamp))/60 as minutes_ago

FROM dwh.fact_vehicle_positions fp
JOIN latest_positions lp ON fp.vehicle_key = lp.vehicle_key
    AND fp.event_timestamp = lp.latest_timestamp
JOIN dwh.dim_vehicle dv ON fp.vehicle_key = dv.vehicle_key
JOIN dwh.dim_route dr ON fp.route_key = dr.route_key
LEFT JOIN dwh.dim_occupancy_level dol ON fp.occupancy_key = dol.occupancy_key;

COMMENT ON VIEW dwh.v_fact_positions_latest IS 'Последние известные позиции транспорта';

-- ============================================================================
-- ТРИГГЕРЫ для автоматического расчета метрик
-- ============================================================================

-- Триггер: Расчет duration_minutes при вставке в fact_trips
CREATE OR REPLACE FUNCTION dwh.calculate_trip_duration()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.end_timestamp IS NOT NULL AND NEW.start_timestamp IS NOT NULL THEN
        NEW.duration_minutes := EXTRACT(EPOCH FROM (NEW.end_timestamp - NEW.start_timestamp))/60;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_calculate_trip_duration
    BEFORE INSERT OR UPDATE ON dwh.fact_trips
    FOR EACH ROW
    EXECUTE FUNCTION dwh.calculate_trip_duration();

COMMENT ON FUNCTION dwh.calculate_trip_duration IS 'Автоматический расчет длительности поездки';

-- ============================================================================
-- МАТЕРИАЛИЗОВАННЫЕ ПРЕДСТАВЛЕНИЯ для ускорения запросов
-- ============================================================================

-- Материализованное представление: Популярные маршруты за последний месяц
CREATE MATERIALIZED VIEW dwh.mv_popular_routes_last_month AS
SELECT
    dr.route_id,
    dr.route_number,
    dr.route_name,
    dr.transport_type,
    COUNT(ft.trip_key) as total_trips,
    COUNT(DISTINCT ft.user_key) as unique_passengers,
    SUM(ft.actual_fare) as total_revenue,
    AVG(ft.duration_minutes) as avg_duration_minutes,
    AVG(ft.distance_km) as avg_distance_km
FROM dwh.fact_trips ft
JOIN dwh.dim_route dr ON ft.route_key = dr.route_key
JOIN dwh.dim_date dd ON ft.start_date_key = dd.date_key
WHERE dd.date >= CURRENT_DATE - INTERVAL '30 days'
  AND ft.trip_status = 'completed'
GROUP BY dr.route_id, dr.route_number, dr.route_name, dr.transport_type
ORDER BY total_trips DESC;

CREATE UNIQUE INDEX idx_mv_popular_routes ON dwh.mv_popular_routes_last_month(route_id);

COMMENT ON MATERIALIZED VIEW dwh.mv_popular_routes_last_month IS 'Топ маршруты за последние 30 дней (обновлять ежедневно)';

-- ============================================================================
-- ПРОЦЕДУРЫ для работы с фактами
-- ============================================================================

-- Процедура: Обновление агрегированных фактов по маршрутам
CREATE OR REPLACE PROCEDURE dwh.refresh_route_hourly_stats(
    p_date_key INTEGER,
    p_hour INTEGER
) AS $$
BEGIN
    INSERT INTO dwh.fact_route_hourly_stats (
        route_key,
        date_key,
        hour,
        total_trips,
        total_passengers,
        total_distance_km,
        total_revenue,
        avg_trip_duration_minutes
    )
    SELECT
        ft.route_key,
        ft.start_date_key as date_key,
        dt.hour,
        COUNT(ft.trip_key) as total_trips,
        COUNT(DISTINCT ft.user_key) as total_passengers,
        SUM(ft.distance_km) as total_distance_km,
        SUM(ft.actual_fare) as total_revenue,
        AVG(ft.duration_minutes) as avg_trip_duration_minutes
    FROM dwh.fact_trips ft
    JOIN dwh.dim_time dt ON ft.start_time_key = dt.time_key
    WHERE ft.start_date_key = p_date_key
      AND dt.hour = p_hour
      AND ft.trip_status = 'completed'
    GROUP BY ft.route_key, ft.start_date_key, dt.hour
    ON CONFLICT (route_key, date_key, hour)
    DO UPDATE SET
        total_trips = EXCLUDED.total_trips,
        total_passengers = EXCLUDED.total_passengers,
        total_distance_km = EXCLUDED.total_distance_km,
        total_revenue = EXCLUDED.total_revenue,
        avg_trip_duration_minutes = EXCLUDED.avg_trip_duration_minutes,
        updated_date = CURRENT_TIMESTAMP;

    COMMIT;
END;
$$ LANGUAGE plpgsql;

COMMENT ON PROCEDURE dwh.refresh_route_hourly_stats IS 'Обновление почасовой статистики по маршрутам';

-- ============================================================================
-- GRANTS
-- ============================================================================

-- CREATE ROLE dwh_etl WITH LOGIN PASSWORD 'etl_password';
-- GRANT USAGE ON SCHEMA dwh TO dwh_etl;
-- GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA dwh TO dwh_etl;
-- GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA dwh TO dwh_etl;
-- GRANT EXECUTE ON ALL PROCEDURES IN SCHEMA dwh TO dwh_etl;

