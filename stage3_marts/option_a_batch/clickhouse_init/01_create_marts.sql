-- Создание базы данных
CREATE DATABASE IF NOT EXISTS analytics;

-- ВИТРИНА 1: Почасовая статистика по маршрутам

CREATE TABLE IF NOT EXISTS analytics.mart_route_hourly_stats
(
    route_id String,
    route_number String,
    route_name String,
    transport_type String,
    date Date,
    hour UInt8,

    -- Метрики по поездкам
    total_trips UInt32,
    unique_passengers UInt32,
    total_distance_km Decimal(12, 2),
    avg_trip_duration_minutes Decimal(8, 2),

    -- Метрики по выручке
    total_revenue Decimal(14, 2),
    avg_fare Decimal(10, 2),

    -- Временные метки
    created_at DateTime DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (route_id, date, hour)
SETTINGS index_granularity = 8192;

COMMENT ON TABLE analytics.mart_route_hourly_stats IS
'Агрегированная статистика по маршрутам за каждый час';

-- ВИТРИНА 2: Популярность станций (топ станций по трафику)

CREATE TABLE IF NOT EXISTS analytics.mart_station_popularity
(
    station_id String,
    station_name String,
    station_type String,
    district String,
    date Date,

    -- Метрики
    departures_count UInt32,  -- Количество отправлений
    arrivals_count UInt32,    -- Количество прибытий
    total_traffic UInt32,     -- Общий трафик
    unique_users UInt32,      -- Уникальные пользователи

    -- Временные метки
    created_at DateTime DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (date, total_traffic DESC)
SETTINGS index_granularity = 8192;

-- ВИТРИНА 3: Анализ пассажиропотока по часам дня

CREATE TABLE IF NOT EXISTS analytics.mart_passenger_flow_by_hour
(
    date Date,
    hour UInt8,
    day_of_week UInt8,
    is_weekend UInt8,
    is_rush_hour UInt8,

    -- Метрики по типам транспорта
    bus_trips UInt32,
    tram_trips UInt32,
    metro_trips UInt32,
    total_trips UInt32,

    -- Метрики по выручке
    total_revenue Decimal(14, 2),
    avg_revenue_per_trip Decimal(10, 2),

    -- Уникальные пользователи
    unique_passengers UInt32,

    created_at DateTime DEFAULT now()
)
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (date, hour)
SETTINGS index_granularity = 8192;

-- ВИТРИНА 4: Сегментация пользователей по активности

CREATE TABLE IF NOT EXISTS analytics.mart_user_segmentation
(
    user_id UInt64,
    username String,
    registration_date Date,
    user_segment String,  -- 'frequent', 'occasional', 'rare', 'churned'

    -- Метрики за последние 30 дней
    trips_last_30d UInt32,
    total_spent_last_30d Decimal(12, 2),
    avg_trips_per_week Decimal(6, 2),

    -- Метрики за все время
    trips_total UInt32,
    total_spent_total Decimal(14, 2),

    -- Любимый маршрут
    favorite_route_id String,
    favorite_route_trips UInt32,

    -- Последняя активность
    last_trip_date Date,
    days_since_last_trip UInt16,

    -- Метка обновления
    updated_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY user_id
SETTINGS index_granularity = 8192;

-- ВИТРИНА 5: Производительность маршрутов (KPI)

CREATE TABLE IF NOT EXISTS analytics.mart_route_performance
(
    route_id String,
    route_number String,
    route_name String,
    transport_type String,
    date Date,

    -- Финансовые метрики
    total_revenue Decimal(14, 2),
    revenue_per_km Decimal(10, 2),
    revenue_per_vehicle Decimal(12, 2),

    -- Операционные метрики
    total_trips UInt32,
    total_distance_km Decimal(12, 2),
    avg_trip_duration_minutes Decimal(8, 2),
    active_vehicles_count UInt16,

    -- Показатели качества
    completion_rate Decimal(5, 2),  -- % завершенных поездок
    cancellation_rate Decimal(5, 2),  -- % отмененных поездок

    -- Рейтинг популярности (1-5)
    popularity_rank UInt16,

    created_at DateTime DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (date, total_revenue DESC)
SETTINGS index_granularity = 8192;

-- ВИТРИНА 6: Анализ скорости движения транспорта

CREATE TABLE IF NOT EXISTS analytics.mart_speed_analysis
(
    route_id String,
    route_number String,
    transport_type String,
    date Date,
    hour UInt8,

    -- Статистика по скорости
    avg_speed Decimal(6, 2),
    max_speed Decimal(6, 2),
    min_speed Decimal(6, 2),
    median_speed Decimal(6, 2),

    -- Количество замеров
    measurements_count UInt32,

    created_at DateTime DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (route_id, date, hour)
SETTINGS index_granularity = 8192;

-- МАТЕРИАЛИЗОВАННЫЕ ПРЕДСТАВЛЕНИЯ для автоматического заполнения витрин

-- МВ для почасовой статистики (пример структуры, требует источник данных)
-- CREATE MATERIALIZED VIEW analytics.mv_route_hourly_stats_auto
-- TO analytics.mart_route_hourly_stats
-- AS
-- SELECT
--     route_id,
--     route_number,
--     route_name,
--     transport_type,
--     toDate(start_timestamp) as date,
--     toHour(start_timestamp) as hour,
--     count() as total_trips,
--     uniq(user_id) as unique_passengers,
--     sum(distance_km) as total_distance_km,
--     avg(duration_minutes) as avg_trip_duration_minutes,
--     sum(actual_fare) as total_revenue,
--     avg(actual_fare) as avg_fare,
--     now() as created_at
-- FROM source_trips
-- WHERE trip_status = 'completed'
-- GROUP BY route_id, route_number, route_name, transport_type, date, hour;

