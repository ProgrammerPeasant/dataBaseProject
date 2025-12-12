-- MetroPulse DWH - CORE LAYER: DIMENSIONS
-- PostgreSQL
-- Описание: Таблицы измерений (Kimball Star Schema)

-- Создание схемы core DWH
CREATE SCHEMA IF NOT EXISTS dwh;

-- Измерение: Календарь (Date Dimension)
CREATE TABLE dwh.dim_date (
    date_key INTEGER PRIMARY KEY,  -- YYYYMMDD формат
    date DATE NOT NULL UNIQUE,
    year INTEGER NOT NULL,
    quarter INTEGER NOT NULL,
    month INTEGER NOT NULL,
    month_name VARCHAR(20) NOT NULL,
    week INTEGER NOT NULL,
    day INTEGER NOT NULL,
    day_of_week INTEGER NOT NULL,  -- 1-7 (Пн-Вс)
    day_name VARCHAR(20) NOT NULL,
    day_of_year INTEGER NOT NULL,
    is_weekend BOOLEAN NOT NULL,
    is_holiday BOOLEAN DEFAULT FALSE,
    holiday_name VARCHAR(100),
    fiscal_year INTEGER,
    fiscal_quarter INTEGER,
    fiscal_month INTEGER,
    week_start_date DATE,
    week_end_date DATE,
    month_start_date DATE,
    month_end_date DATE,
    quarter_start_date DATE,
    quarter_end_date DATE,
    year_start_date DATE,
    year_end_date DATE
);

CREATE INDEX idx_dim_date_date ON dwh.dim_date(date);
CREATE INDEX idx_dim_date_year_month ON dwh.dim_date(year, month);
CREATE INDEX idx_dim_date_year_quarter ON dwh.dim_date(year, quarter);
CREATE INDEX idx_dim_date_weekend ON dwh.dim_date(is_weekend);
CREATE INDEX idx_dim_date_holiday ON dwh.dim_date(is_holiday);

COMMENT ON TABLE dwh.dim_date IS 'Календарное измерение (дни)';

-- Измерение: Время (Time Dimension)
CREATE TABLE dwh.dim_time (
    time_key INTEGER PRIMARY KEY,  -- HHMMSS формат
    time TIME NOT NULL UNIQUE,
    hour INTEGER NOT NULL,
    minute INTEGER NOT NULL,
    second INTEGER NOT NULL,
    hour_of_day INTEGER NOT NULL,  -- 0-23
    time_period VARCHAR(20) NOT NULL,  -- morning, afternoon, evening, night
    is_rush_hour BOOLEAN NOT NULL,
    rush_hour_type VARCHAR(20)  -- morning_rush, evening_rush, NULL
);

CREATE INDEX idx_dim_time_hour ON dwh.dim_time(hour);
CREATE INDEX idx_dim_time_period ON dwh.dim_time(time_period);
CREATE INDEX idx_dim_time_rush_hour ON dwh.dim_time(is_rush_hour);

COMMENT ON TABLE dwh.dim_time IS 'Временное измерение (время суток)';

-- Измерение: Пользователи (User Dimension) - SCD Type 1
CREATE TABLE dwh.dim_user (
    user_key BIGSERIAL PRIMARY KEY,  -- Surrogate Key
    user_id BIGINT NOT NULL,  -- Natural Key (Business Key)
    username VARCHAR(50),
    email VARCHAR(100),
    phone VARCHAR(20),
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    full_name VARCHAR(200),
    date_of_birth DATE,
    age_group VARCHAR(20),  -- '18-25', '26-35', '36-50', '51+'
    registration_date DATE,
    current_status VARCHAR(20),
    current_balance DECIMAL(10, 2),
    user_segment VARCHAR(50),  -- 'frequent', 'occasional', 'rare', 'new'

    -- Metadata
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_system VARCHAR(50) DEFAULT 'OLTP',

    -- Business Key Constraint
    CONSTRAINT uk_dim_user_business_key UNIQUE (user_id)
);

CREATE INDEX idx_dim_user_user_id ON dwh.dim_user(user_id);
CREATE INDEX idx_dim_user_email ON dwh.dim_user(email);
CREATE INDEX idx_dim_user_status ON dwh.dim_user(current_status);
CREATE INDEX idx_dim_user_segment ON dwh.dim_user(user_segment);
CREATE INDEX idx_dim_user_age_group ON dwh.dim_user(age_group);

COMMENT ON TABLE dwh.dim_user IS 'Измерение пользователей (SCD Type 1 - только текущее состояние)';

-- Измерение: Маршруты (Route Dimension) - SCD Type 2 ⭐
CREATE TABLE dwh.dim_route (
    route_key BIGSERIAL PRIMARY KEY,  -- Surrogate Key
    route_id VARCHAR(20) NOT NULL,  -- Natural Key (Business Key)
    route_number VARCHAR(10) NOT NULL,
    route_name VARCHAR(200) NOT NULL,
    transport_type VARCHAR(20) NOT NULL,
    transport_type_name VARCHAR(50),  -- 'Автобус', 'Трамвай', 'Метро'
    base_fare DECIMAL(10, 2) NOT NULL,
    is_active BOOLEAN NOT NULL,
    description TEXT,

    -- SCD Type 2 Fields
    effective_date DATE NOT NULL,  -- Дата начала действия версии
    expiration_date DATE,  -- Дата окончания действия (NULL для текущей версии)
    is_current BOOLEAN NOT NULL DEFAULT TRUE,  -- Флаг текущей версии
    version INTEGER NOT NULL DEFAULT 1,  -- Номер версии

    -- Metadata
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_system VARCHAR(50) DEFAULT 'OLTP',

    -- Constraint: только одна текущая версия для каждого route_id
    CONSTRAINT chk_dim_route_expiration CHECK (
        (is_current = TRUE AND expiration_date IS NULL) OR
        (is_current = FALSE AND expiration_date IS NOT NULL)
    )
);

CREATE INDEX idx_dim_route_route_id ON dwh.dim_route(route_id);
CREATE INDEX idx_dim_route_is_current ON dwh.dim_route(route_id, is_current);
CREATE INDEX idx_dim_route_effective_dates ON dwh.dim_route(route_id, effective_date, expiration_date);
CREATE INDEX idx_dim_route_number ON dwh.dim_route(route_number);
CREATE INDEX idx_dim_route_transport_type ON dwh.dim_route(transport_type);
CREATE INDEX idx_dim_route_active ON dwh.dim_route(is_active);

COMMENT ON TABLE dwh.dim_route IS 'Измерение маршрутов (SCD Type 2 - с историей изменений)';
COMMENT ON COLUMN dwh.dim_route.effective_date IS 'Дата начала действия этой версии маршрута';
COMMENT ON COLUMN dwh.dim_route.expiration_date IS 'Дата окончания действия этой версии (NULL = текущая)';
COMMENT ON COLUMN dwh.dim_route.is_current IS 'TRUE для текущей активной версии';
COMMENT ON COLUMN dwh.dim_route.version IS 'Порядковый номер версии для данного route_id';

-- Измерение: Транспортные средства (Vehicle Dimension) - SCD Type 1
CREATE TABLE dwh.dim_vehicle (
    vehicle_key BIGSERIAL PRIMARY KEY,  -- Surrogate Key
    vehicle_id VARCHAR(20) NOT NULL,  -- Natural Key (Business Key)
    vehicle_type VARCHAR(20) NOT NULL,
    vehicle_type_name VARCHAR(50),  -- 'Автобус', 'Трамвай', 'Вагон метро'
    current_route_id VARCHAR(20),
    capacity INTEGER,
    manufacture_year INTEGER,
    vehicle_age INTEGER,  -- Вычисляемое поле: текущий год - manufacture_year
    model VARCHAR(100),
    license_plate VARCHAR(20),
    last_maintenance_date DATE,
    maintenance_status VARCHAR(50),  -- 'up_to_date', 'due_soon', 'overdue'
    current_status VARCHAR(20),  -- 'active', 'maintenance', 'retired'

    -- Metadata
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_system VARCHAR(50) DEFAULT 'OLTP',

    -- Business Key Constraint
    CONSTRAINT uk_dim_vehicle_business_key UNIQUE (vehicle_id)
);

CREATE INDEX idx_dim_vehicle_vehicle_id ON dwh.dim_vehicle(vehicle_id);
CREATE INDEX idx_dim_vehicle_type ON dwh.dim_vehicle(vehicle_type);
CREATE INDEX idx_dim_vehicle_route ON dwh.dim_vehicle(current_route_id);
CREATE INDEX idx_dim_vehicle_status ON dwh.dim_vehicle(current_status);
CREATE INDEX idx_dim_vehicle_maintenance ON dwh.dim_vehicle(maintenance_status);

COMMENT ON TABLE dwh.dim_vehicle IS 'Измерение транспортных средств (SCD Type 1)';

-- Измерение: Станции/Остановки (Station Dimension) - SCD Type 1
CREATE TABLE dwh.dim_station (
    station_key BIGSERIAL PRIMARY KEY,  -- Surrogate Key
    station_id VARCHAR(20) NOT NULL,  -- Natural Key (Business Key)
    station_name VARCHAR(200) NOT NULL,
    latitude DECIMAL(10, 8) NOT NULL,
    longitude DECIMAL(11, 8) NOT NULL,
    station_type VARCHAR(20) NOT NULL,
    station_type_name VARCHAR(50),  -- 'Автобусная остановка', 'Станция метро', 'Трамвайная остановка'
    district VARCHAR(100),
    zone VARCHAR(10),
    is_active BOOLEAN,
    is_transfer_station BOOLEAN DEFAULT FALSE,  -- Станция пересадки

    -- Metadata
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_system VARCHAR(50) DEFAULT 'OLTP',

    -- Business Key Constraint
    CONSTRAINT uk_dim_station_business_key UNIQUE (station_id)
);

CREATE INDEX idx_dim_station_station_id ON dwh.dim_station(station_id);
CREATE INDEX idx_dim_station_name ON dwh.dim_station(station_name);
CREATE INDEX idx_dim_station_type ON dwh.dim_station(station_type);
CREATE INDEX idx_dim_station_district ON dwh.dim_station(district);
CREATE INDEX idx_dim_station_zone ON dwh.dim_station(zone);
CREATE INDEX idx_dim_station_active ON dwh.dim_station(is_active);
CREATE INDEX idx_dim_station_transfer ON dwh.dim_station(is_transfer_station);

COMMENT ON TABLE dwh.dim_station IS 'Измерение станций и остановок (SCD Type 1)';

-- Измерение: Способы оплаты (Payment Method Dimension)
CREATE TABLE dwh.dim_payment_method (
    payment_method_key SERIAL PRIMARY KEY,  -- Surrogate Key
    payment_method_code VARCHAR(20) NOT NULL UNIQUE,  -- Natural Key
    payment_method_name VARCHAR(50) NOT NULL,
    payment_category VARCHAR(50),  -- 'digital', 'cash', 'subscription'
    requires_balance BOOLEAN,
    is_active BOOLEAN DEFAULT TRUE,
    description TEXT,

    -- Metadata
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_system VARCHAR(50) DEFAULT 'SYSTEM'
);

CREATE INDEX idx_dim_payment_method_code ON dwh.dim_payment_method(payment_method_code);
CREATE INDEX idx_dim_payment_method_category ON dwh.dim_payment_method(payment_category);

COMMENT ON TABLE dwh.dim_payment_method IS 'Справочник способов оплаты';

-- Измерение: Уровни загруженности транспорта (Occupancy Level Dimension)
CREATE TABLE dwh.dim_occupancy_level (
    occupancy_key SERIAL PRIMARY KEY,
    occupancy_code VARCHAR(50) NOT NULL UNIQUE,  -- Natural Key
    occupancy_name VARCHAR(100) NOT NULL,
    occupancy_percentage_min INTEGER,
    occupancy_percentage_max INTEGER,
    occupancy_level INTEGER,  -- 1-5 (низкий-высокий)
    color_code VARCHAR(7),  -- HEX color for visualization
    description TEXT
);

CREATE INDEX idx_dim_occupancy_code ON dwh.dim_occupancy_level(occupancy_code);
CREATE INDEX idx_dim_occupancy_level ON dwh.dim_occupancy_level(occupancy_level);

COMMENT ON TABLE dwh.dim_occupancy_level IS 'Справочник уровней загруженности транспорта';

-- ============================================================================
-- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ для работы с измерениями
-- ============================================================================

-- Функция: Получить текущий route_key для заданного route_id
CREATE OR REPLACE FUNCTION dwh.get_current_route_key(p_route_id VARCHAR)
RETURNS BIGINT AS $$
DECLARE
    v_route_key BIGINT;
BEGIN
    SELECT route_key INTO v_route_key
    FROM dwh.dim_route
    WHERE route_id = p_route_id
      AND is_current = TRUE
    LIMIT 1;

    RETURN v_route_key;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION dwh.get_current_route_key IS 'Получить текущий surrogate key для route_id';

-- Функция: Получить route_key для заданной даты (для исторических запросов)
CREATE OR REPLACE FUNCTION dwh.get_route_key_at_date(
    p_route_id VARCHAR,
    p_date DATE
) RETURNS BIGINT AS $$
DECLARE
    v_route_key BIGINT;
BEGIN
    SELECT route_key INTO v_route_key
    FROM dwh.dim_route
    WHERE route_id = p_route_id
      AND effective_date <= p_date
      AND (expiration_date > p_date OR expiration_date IS NULL)
    LIMIT 1;

    RETURN v_route_key;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION dwh.get_route_key_at_date IS 'Получить route_key действительный на заданную дату';

-- Функция: Получить текущую версию маршрута
CREATE OR REPLACE FUNCTION dwh.get_route_current_version(p_route_id VARCHAR)
RETURNS INTEGER AS $$
DECLARE
    v_version INTEGER;
BEGIN
    SELECT COALESCE(MAX(version), 0) INTO v_version
    FROM dwh.dim_route
    WHERE route_id = p_route_id;

    RETURN v_version;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION dwh.get_route_current_version IS 'Получить текущий номер версии маршрута';

-- ============================================================================
-- ПРЕДСТАВЛЕНИЯ для удобной работы с измерениями
-- ============================================================================

-- Представление: Текущие маршруты (только актуальные версии)
CREATE OR REPLACE VIEW dwh.v_dim_route_current AS
SELECT
    route_key,
    route_id,
    route_number,
    route_name,
    transport_type,
    transport_type_name,
    base_fare,
    is_active,
    description,
    effective_date,
    version
FROM dwh.dim_route
WHERE is_current = TRUE;

COMMENT ON VIEW dwh.v_dim_route_current IS 'Текущие (актуальные) версии маршрутов';

-- Представление: История изменений маршрутов
CREATE OR REPLACE VIEW dwh.v_dim_route_history AS
SELECT
    route_key,
    route_id,
    route_number,
    route_name,
    transport_type,
    base_fare,
    effective_date,
    expiration_date,
    COALESCE(expiration_date, CURRENT_DATE) - effective_date as days_active,
    is_current,
    version,
    CASE
        WHEN version > 1 THEN
            LAG(base_fare) OVER (PARTITION BY route_id ORDER BY version)
    END as previous_fare,
    CASE
        WHEN version > 1 THEN
            base_fare - LAG(base_fare) OVER (PARTITION BY route_id ORDER BY version)
    END as fare_change
FROM dwh.dim_route
ORDER BY route_id, version;

COMMENT ON VIEW dwh.v_dim_route_history IS 'История изменений маршрутов с расчетом изменений тарифа';

-- Представление: Активные пользователи
CREATE OR REPLACE VIEW dwh.v_dim_user_active AS
SELECT
    user_key,
    user_id,
    username,
    email,
    full_name,
    age_group,
    current_status,
    current_balance,
    user_segment,
    registration_date,
    CURRENT_DATE - registration_date as days_since_registration
FROM dwh.dim_user
WHERE current_status = 'active';

COMMENT ON VIEW dwh.v_dim_user_active IS 'Активные пользователи';

-- Представление: Активные транспортные средства по маршрутам
CREATE OR REPLACE VIEW dwh.v_dim_vehicle_active AS
SELECT
    v.vehicle_key,
    v.vehicle_id,
    v.vehicle_type,
    v.vehicle_type_name,
    v.current_route_id,
    r.route_number,
    r.route_name,
    v.capacity,
    v.vehicle_age,
    v.model,
    v.maintenance_status,
    v.last_maintenance_date,
    CURRENT_DATE - v.last_maintenance_date as days_since_maintenance
FROM dwh.dim_vehicle v
LEFT JOIN dwh.v_dim_route_current r ON v.current_route_id = r.route_id
WHERE v.current_status = 'active';

COMMENT ON VIEW dwh.v_dim_vehicle_active IS 'Активные транспортные средства с информацией о маршрутах';

-- Заполнение dim_payment_method
INSERT INTO dwh.dim_payment_method (payment_method_code, payment_method_name, payment_category, requires_balance) VALUES
('card', 'Банковская карта', 'digital', FALSE),
('cash', 'Наличные', 'cash', FALSE),
('subscription', 'Подписка', 'subscription', FALSE),
('balance', 'Баланс приложения', 'digital', TRUE);

-- Заполнение dim_occupancy_level
INSERT INTO dwh.dim_occupancy_level (occupancy_code, occupancy_name, occupancy_percentage_min, occupancy_percentage_max, occupancy_level, color_code, description) VALUES
('empty', 'Пусто', 0, 20, 1, '#00FF00', 'Много свободных мест'),
('many_seats_available', 'Много мест', 21, 40, 2, '#90EE90', 'Комфортно, много свободных мест'),
('few_seats_available', 'Мало мест', 41, 60, 3, '#FFD700', 'Есть свободные места'),
('standing_room_only', 'Только стоячие места', 61, 80, 4, '#FFA500', 'Все сидячие места заняты'),
('crushed_standing_room_only', 'Переполнен', 81, 100, 5, '#FF0000', 'Очень много людей'),
('full', 'Полный', 95, 100, 5, '#8B0000', 'Новые пассажиры не помещаются'),
('not_accepting_passengers', 'Не принимает пассажиров', NULL, NULL, 0, '#808080', 'Транспорт не принимает пассажиров');

COMMENT ON TABLE dwh.dim_payment_method IS 'Справочные данные заполнены';
COMMENT ON TABLE dwh.dim_occupancy_level IS 'Справочные данные заполнены';

-- доступы к схеме dwh

-- CREATE ROLE dwh_user WITH LOGIN PASSWORD 'dwh_password';
-- GRANT USAGE ON SCHEMA dwh TO dwh_user;
-- GRANT SELECT ON ALL TABLES IN SCHEMA dwh TO dwh_user;
-- GRANT SELECT ON ALL SEQUENCES IN SCHEMA dwh TO dwh_user;
-- GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA dwh TO dwh_user;

