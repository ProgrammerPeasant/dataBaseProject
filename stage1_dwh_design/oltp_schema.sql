DROP SCHEMA IF EXISTS oltp CASCADE;
-- Создание схемы
CREATE SCHEMA IF NOT EXISTS oltp;

-- Таблица: Станции и остановки
CREATE TABLE IF NOT EXISTS oltp.stations (
    station_id VARCHAR(20) PRIMARY KEY,
    station_name VARCHAR(200) NOT NULL,
    latitude DECIMAL(10, 8) NOT NULL,
    longitude DECIMAL(11, 8) NOT NULL,
    station_type VARCHAR(20) NOT NULL CHECK (station_type IN ('bus_stop', 'metro_station', 'tram_stop')),
    district VARCHAR(100),
    zone VARCHAR(10),
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS  idx_stations_type ON oltp.stations(station_type);
CREATE INDEX IF NOT EXISTS  idx_stations_location ON oltp.stations(latitude, longitude);
CREATE INDEX IF NOT EXISTS  idx_stations_active ON oltp.stations(is_active);

COMMENT ON TABLE oltp.stations IS 'Справочник станций и остановок общественного транспорта';

-- Таблица: Маршруты
CREATE TABLE IF NOT EXISTS oltp.routes (
    route_id VARCHAR(20) PRIMARY KEY,
    route_number VARCHAR(10) NOT NULL,
    route_name VARCHAR(200) NOT NULL,
    transport_type VARCHAR(20) NOT NULL CHECK (transport_type IN ('bus', 'tram', 'metro')),
    base_fare DECIMAL(10, 2) NOT NULL,
    is_active BOOLEAN DEFAULT TRUE,
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_routes_number ON oltp.routes(route_number);
CREATE INDEX IF NOT EXISTS idx_routes_type ON oltp.routes(transport_type);
CREATE INDEX IF NOT EXISTS idx_routes_active ON oltp.routes(is_active);

COMMENT ON TABLE oltp.routes IS 'Справочник маршрутов общественного транспорта';

-- Таблица: Связь маршрутов и станций
CREATE TABLE IF NOT EXISTS oltp.route_stations (
    route_id VARCHAR(20) NOT NULL,
    station_id VARCHAR(20) NOT NULL,
    sequence_number INTEGER NOT NULL,
    estimated_travel_time_minutes INTEGER,
    distance_from_previous_km DECIMAL(5, 2),
    PRIMARY KEY (route_id, station_id),
    FOREIGN KEY (route_id) REFERENCES oltp.routes(route_id) ON DELETE CASCADE,
    FOREIGN KEY (station_id) REFERENCES oltp.stations(station_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_route_stations_route ON oltp.route_stations(route_id, sequence_number);
CREATE INDEX IF NOT EXISTS idx_route_stations_station ON oltp.route_stations(station_id);

COMMENT ON TABLE oltp.route_stations IS 'Связь маршрутов и остановок с порядком следования';

-- Таблица: Транспортные средства
CREATE TABLE IF NOT EXISTS oltp.vehicles (
    vehicle_id VARCHAR(20) PRIMARY KEY,
    vehicle_type VARCHAR(20) NOT NULL CHECK (vehicle_type IN ('bus', 'tram', 'metro')),
    route_id VARCHAR(20),
    capacity INTEGER,
    manufacture_year INTEGER,
    model VARCHAR(100),
    license_plate VARCHAR(20),
    last_maintenance_date DATE,
    status VARCHAR(20) DEFAULT 'active' CHECK (status IN ('active', 'maintenance', 'retired')),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (route_id) REFERENCES oltp.routes(route_id) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_vehicles_type ON oltp.vehicles(vehicle_type);
CREATE INDEX IF NOT EXISTS idx_vehicles_route ON oltp.vehicles(route_id);
CREATE INDEX IF NOT EXISTS idx_vehicles_status ON oltp.vehicles(status);

COMMENT ON TABLE oltp.vehicles IS 'Справочник транспортных средств';

-- ============================================================================
-- ПОЛЬЗОВАТЕЛЬСКИЕ ДАННЫЕ (User Data)
-- ============================================================================

-- Таблица: Пользователи
CREATE TABLE IF NOT EXISTS oltp.users (
    user_id BIGSERIAL PRIMARY KEY,
    username VARCHAR(50) UNIQUE NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    phone VARCHAR(20),
    password_hash VARCHAR(255) NOT NULL,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    date_of_birth DATE,
    registration_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(20) DEFAULT 'active' CHECK (status IN ('active', 'suspended', 'deleted')),
    balance DECIMAL(10, 2) DEFAULT 0.00,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_users_email ON oltp.users(email);
CREATE INDEX IF NOT EXISTS idx_users_status ON oltp.users(status);
CREATE INDEX IF NOT EXISTS idx_users_registration ON oltp.users(registration_date);

COMMENT ON TABLE oltp.users IS 'Профили пользователей мобильного приложения';

-- ============================================================================
-- ТРАНЗАКЦИОННЫЕ ДАННЫЕ (Transactional Data)
-- ============================================================================

-- Таблица: Поездки
CREATE TABLE IF NOT EXISTS oltp.trips (
    trip_id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL,
    route_id VARCHAR(20) NOT NULL,
    vehicle_id VARCHAR(20),
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    start_station_id VARCHAR(20),
    end_station_id VARCHAR(20),
    distance_km DECIMAL(6, 2),
    status VARCHAR(20) DEFAULT 'in_progress' CHECK (status IN ('in_progress', 'completed', 'cancelled')),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES oltp.users(user_id) ON DELETE CASCADE,
    FOREIGN KEY (route_id) REFERENCES oltp.routes(route_id) ON DELETE RESTRICT,
    FOREIGN KEY (vehicle_id) REFERENCES oltp.vehicles(vehicle_id) ON DELETE SET NULL,
    FOREIGN KEY (start_station_id) REFERENCES oltp.stations(station_id) ON DELETE SET NULL,
    FOREIGN KEY (end_station_id) REFERENCES oltp.stations(station_id) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_trips_user ON oltp.trips(user_id, start_time DESC);
CREATE INDEX IF NOT EXISTS idx_trips_route ON oltp.trips(route_id, start_time DESC);
CREATE INDEX IF NOT EXISTS idx_trips_vehicle ON oltp.trips(vehicle_id, start_time DESC);
CREATE INDEX IF NOT EXISTS idx_trips_start_time ON oltp.trips(start_time);
CREATE INDEX IF NOT EXISTS idx_trips_status ON oltp.trips(status);

-- Партиционирование по месяцам (для больших объемов)
-- CREATE TABLE oltp.trips_2025_01 PARTITION OF oltp.trips
-- FOR VALUES FROM ('2025-01-01') TO ('2025-02-01');

COMMENT ON TABLE oltp.trips IS 'История поездок пользователей';

-- Таблица: Транзакции
CREATE TABLE IF NOT EXISTS oltp.transactions (
    transaction_id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL,
    trip_id BIGINT,
    amount DECIMAL(10, 2) NOT NULL,
    payment_method VARCHAR(20) NOT NULL CHECK (payment_method IN ('card', 'cash', 'subscription', 'balance')),
    transaction_type VARCHAR(20) NOT NULL CHECK (transaction_type IN ('payment', 'refund', 'topup')),
    transaction_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(20) DEFAULT 'success' CHECK (status IN ('success', 'failed', 'pending', 'refunded')),
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES oltp.users(user_id) ON DELETE CASCADE,
    FOREIGN KEY (trip_id) REFERENCES oltp.trips(trip_id) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_transactions_user ON oltp.transactions(user_id, transaction_date DESC);
CREATE INDEX IF NOT EXISTS idx_transactions_trip ON oltp.transactions(trip_id);
CREATE INDEX IF NOT EXISTS idx_transactions_date ON oltp.transactions(transaction_date);
CREATE INDEX idx_transactions_status ON oltp.transactions(status);
CREATE INDEX idx_transactions_method ON oltp.transactions(payment_method);

COMMENT ON TABLE oltp.transactions IS 'История финансовых транзакций';

-- Таблица: Подписки
CREATE TABLE oltp.subscriptions (
    subscription_id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL,
    subscription_type VARCHAR(50) NOT NULL CHECK (subscription_type IN ('monthly', 'quarterly', 'annual', 'student')),
    start_date DATE NOT NULL,
    end_date DATE NOT NULL,
    price DECIMAL(10, 2) NOT NULL,
    is_active BOOLEAN DEFAULT TRUE,
    auto_renewal BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES oltp.users(user_id) ON DELETE CASCADE
);

CREATE INDEX idx_subscriptions_user ON oltp.subscriptions(user_id, is_active);
CREATE INDEX idx_subscriptions_dates ON oltp.subscriptions(start_date, end_date);

COMMENT ON TABLE oltp.subscriptions IS 'Подписки пользователей';

-- Таблица: Построенные маршруты (поиск)
CREATE TABLE oltp.route_searches (
    search_id BIGSERIAL PRIMARY KEY,
    user_id BIGINT,
    start_station_id VARCHAR(20),
    end_station_id VARCHAR(20),
    search_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    suggested_routes JSONB,
    selected_route_id VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES oltp.users(user_id) ON DELETE CASCADE,
    FOREIGN KEY (start_station_id) REFERENCES oltp.stations(station_id) ON DELETE SET NULL,
    FOREIGN KEY (end_station_id) REFERENCES oltp.stations(station_id) ON DELETE SET NULL
);

CREATE INDEX idx_route_searches_user ON oltp.route_searches(user_id, search_time DESC);
CREATE INDEX idx_route_searches_time ON oltp.route_searches(search_time);

COMMENT ON TABLE oltp.route_searches IS 'История поиска маршрутов пользователями';

-- ============================================================================
-- ТРИГГЕРЫ (Triggers)
-- ============================================================================

-- Триггер для обновления updated_at
CREATE OR REPLACE FUNCTION oltp.update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_users_updated_at BEFORE UPDATE ON oltp.users
    FOR EACH ROW EXECUTE FUNCTION oltp.update_updated_at_column();

CREATE TRIGGER update_routes_updated_at BEFORE UPDATE ON oltp.routes
    FOR EACH ROW EXECUTE FUNCTION oltp.update_updated_at_column();

CREATE TRIGGER update_stations_updated_at BEFORE UPDATE ON oltp.stations
    FOR EACH ROW EXECUTE FUNCTION oltp.update_updated_at_column();

-- Триггер для обновления баланса при транзакции
CREATE OR REPLACE FUNCTION oltp.update_user_balance()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.status = 'success' THEN
        IF NEW.transaction_type = 'topup' THEN
            UPDATE oltp.users SET balance = balance + NEW.amount WHERE user_id = NEW.user_id;
        ELSIF NEW.transaction_type = 'payment' THEN
            UPDATE oltp.users SET balance = balance - NEW.amount WHERE user_id = NEW.user_id;
        ELSIF NEW.transaction_type = 'refund' THEN
            UPDATE oltp.users SET balance = balance + NEW.amount WHERE user_id = NEW.user_id;
        END IF;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_balance_on_transaction AFTER INSERT ON oltp.transactions
    FOR EACH ROW EXECUTE FUNCTION oltp.update_user_balance();

-- ============================================================================
-- ПРЕДСТАВЛЕНИЯ (Views)
-- ============================================================================

-- Активные поездки
CREATE OR REPLACE VIEW oltp.active_trips AS
SELECT
    t.trip_id,
    t.user_id,
    u.username,
    t.route_id,
    r.route_number,
    r.route_name,
    t.vehicle_id,
    t.start_time,
    t.start_station_id,
    s.station_name as start_station_name,
    EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - t.start_time))/60 as duration_minutes
FROM oltp.trips t
JOIN oltp.users u ON t.user_id = u.user_id
JOIN oltp.routes r ON t.route_id = r.route_id
LEFT JOIN oltp.stations s ON t.start_station_id = s.station_id
WHERE t.status = 'in_progress';

COMMENT ON VIEW oltp.active_trips IS 'Текущие активные поездки';

-- Статистика по пользователям
CREATE OR REPLACE VIEW oltp.user_statistics AS
SELECT
    u.user_id,
    u.username,
    u.email,
    u.balance,
    COUNT(DISTINCT t.trip_id) as total_trips,
    COUNT(DISTINCT CASE WHEN t.status = 'completed' THEN t.trip_id END) as completed_trips,
    SUM(CASE WHEN tr.transaction_type = 'payment' AND tr.status = 'success' THEN tr.amount ELSE 0 END) as total_spent,
    MAX(t.start_time) as last_trip_date
FROM oltp.users u
LEFT JOIN oltp.trips t ON u.user_id = t.user_id
LEFT JOIN oltp.transactions tr ON u.user_id = tr.user_id
WHERE u.status = 'active'
GROUP BY u.user_id, u.username, u.email, u.balance;

COMMENT ON VIEW oltp.user_statistics IS 'Статистика по пользователям';

-- ============================================================================
-- ПРИМЕРЫ ДАННЫХ (Sample Data)
-- ============================================================================

-- Будут добавлены через генератор данных на этапе 2

-- ============================================================================
-- GRANTS (для разных ролей)
-- ============================================================================

-- CREATE ROLE oltp_app_user WITH LOGIN PASSWORD 'secure_password';
-- GRANT USAGE ON SCHEMA oltp TO oltp_app_user;
-- GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA oltp TO oltp_app_user;
-- GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA oltp TO oltp_app_user;

-- CREATE ROLE oltp_readonly WITH LOGIN PASSWORD 'readonly_password';
-- GRANT USAGE ON SCHEMA oltp TO oltp_readonly;
-- GRANT SELECT ON ALL TABLES IN SCHEMA oltp TO oltp_readonly;

