-- MetroPulse - Примеры аналитических запросов
-- Демонстрация возможностей DWH и витрин данных

-- РАЗДЕЛ 1: БАЗОВЫЕ ЗАПРОСЫ К DWH

-- 1.1 Проверка количества данных в измерениях
SELECT 'dim_user' as table_name, COUNT(*) as row_count FROM dwh.dim_user
UNION ALL
SELECT 'dim_route', COUNT(*) FROM dwh.dim_route
UNION ALL
SELECT 'dim_station', COUNT(*) FROM dwh.dim_station
UNION ALL
SELECT 'dim_vehicle', COUNT(*) FROM dwh.dim_vehicle
UNION ALL
SELECT 'dim_date', COUNT(*) FROM dwh.dim_date
UNION ALL
SELECT 'dim_time', COUNT(*) FROM dwh.dim_time;

-- 1.2 Проверка количества данных в фактах
SELECT 'fact_trips' as table_name, COUNT(*) as row_count FROM dwh.fact_trips
UNION ALL
SELECT 'fact_transactions', COUNT(*) FROM dwh.fact_transactions;

-- РАЗДЕЛ 2: АНАЛИЗ МАРШРУТОВ

-- 2.1 Топ-10 самых популярных маршрутов
SELECT
    dr.route_number,
    dr.route_name,
    dr.transport_type,
    COUNT(*) as total_trips,
    SUM(ft.actual_fare) as total_revenue,
    AVG(ft.duration_minutes) as avg_duration
FROM dwh.fact_trips ft
JOIN dwh.dim_route dr ON ft.route_key = dr.route_key
WHERE ft.trip_status = 'completed'
  AND dr.is_current = TRUE
GROUP BY dr.route_number, dr.route_name, dr.transport_type
ORDER BY total_trips DESC
LIMIT 10;

-- 2.2 Распределение поездок по типам транспорта
SELECT
    dr.transport_type,
    COUNT(*) as trips_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 2) as percentage,
    SUM(ft.actual_fare) as revenue
FROM dwh.fact_trips ft
JOIN dwh.dim_route dr ON ft.route_key = dr.route_key
WHERE ft.trip_status = 'completed'
GROUP BY dr.transport_type
ORDER BY trips_count DESC;

-- 2.3 История изменения тарифов маршрута (SCD Type 2)
SELECT
    route_id,
    route_number,
    version,
    base_fare,
    effective_date,
    expiration_date,
    CASE WHEN is_current THEN 'Текущая' ELSE 'Историческая' END as status,
    COALESCE(expiration_date, CURRENT_DATE) - effective_date as days_active
FROM dwh.dim_route
WHERE route_id = 'R001'
ORDER BY version;

-- РАЗДЕЛ 3: АНАЛИЗ ПАССАЖИРОПОТОКА

-- 3.1 Распределение поездок по часам суток
SELECT
    dt.hour as hour_of_day,
    dt.time_period,
    dt.is_rush_hour,
    COUNT(*) as trips_count,
    AVG(ft.duration_minutes) as avg_duration,
    SUM(ft.actual_fare) as revenue
FROM dwh.fact_trips ft
JOIN dwh.dim_time dt ON ft.start_time_key = dt.time_key
WHERE ft.trip_status = 'completed'
GROUP BY dt.hour, dt.time_period, dt.is_rush_hour
ORDER BY dt.hour;

-- 3.2 Сравнение будних дней и выходных
SELECT
    CASE WHEN dd.is_weekend THEN 'Выходные' ELSE 'Будни' END as day_type,
    COUNT(*) as trips_count,
    AVG(ft.duration_minutes) as avg_duration,
    AVG(ft.distance_km) as avg_distance,
    SUM(ft.actual_fare) as total_revenue
FROM dwh.fact_trips ft
JOIN dwh.dim_date dd ON ft.start_date_key = dd.date_key
WHERE ft.trip_status = 'completed'
GROUP BY dd.is_weekend
ORDER BY day_type;

-- 3.3 Анализ часов пик vs обычное время
SELECT
    CASE WHEN dt.is_rush_hour THEN 'Час пик' ELSE 'Обычное время' END as period_type,
    dt.rush_hour_type,
    COUNT(*) as trips_count,
    AVG(ft.duration_minutes) as avg_duration,
    COUNT(DISTINCT ft.user_key) as unique_passengers
FROM dwh.fact_trips ft
JOIN dwh.dim_time dt ON ft.start_time_key = dt.time_key
WHERE ft.trip_status = 'completed'
GROUP BY dt.is_rush_hour, dt.rush_hour_type
ORDER BY trips_count DESC;

-- РАЗДЕЛ 4: АНАЛИЗ ПОЛЬЗОВАТЕЛЕЙ

-- 4.1 Сегментация пользователей по возрасту и активности
SELECT
    du.age_group,
    du.user_segment,
    COUNT(*) as users_count,
    AVG(du.current_balance) as avg_balance
FROM dwh.dim_user du
WHERE du.current_status = 'active'
GROUP BY du.age_group, du.user_segment
ORDER BY du.age_group, users_count DESC;

-- 4.2 Топ-10 самых активных пользователей
SELECT
    du.username,
    du.user_segment,
    COUNT(ft.trip_key) as trips_count,
    SUM(ft.actual_fare) as total_spent,
    AVG(ft.duration_minutes) as avg_trip_duration
FROM dwh.dim_user du
JOIN dwh.fact_trips ft ON du.user_key = ft.user_key
WHERE ft.trip_status = 'completed'
GROUP BY du.user_id, du.username, du.user_segment
ORDER BY trips_count DESC
LIMIT 10;

-- 4.3 Анализ методов оплаты
SELECT
    dpm.payment_method_name,
    dpm.payment_category,
    COUNT(*) as transactions_count,
    SUM(ftr.amount) as total_amount,
    AVG(ftr.amount) as avg_transaction
FROM dwh.fact_transactions ftr
JOIN dwh.dim_payment_method dpm ON ftr.payment_method_key = dpm.payment_method_key
WHERE ftr.transaction_status = 'success'
  AND ftr.transaction_type = 'payment'
GROUP BY dpm.payment_method_name, dpm.payment_category
ORDER BY total_amount DESC;

-- РАЗДЕЛ 5: АНАЛИЗ СТАНЦИЙ

-- 5.1 Топ-10 самых популярных станций отправления
SELECT
    ds.station_name,
    ds.station_type,
    ds.district,
    COUNT(*) as departures_count
FROM dwh.fact_trips ft
JOIN dwh.dim_station ds ON ft.start_station_key = ds.station_key
WHERE ft.trip_status = 'completed'
GROUP BY ds.station_id, ds.station_name, ds.station_type, ds.district
ORDER BY departures_count DESC
LIMIT 10;

-- 5.2 Топ-10 популярных маршрутов поездок (пара станций)
SELECT
    ss.station_name as start_station,
    es.station_name as end_station,
    COUNT(*) as trips_count,
    AVG(ft.duration_minutes) as avg_duration,
    AVG(ft.distance_km) as avg_distance
FROM dwh.fact_trips ft
JOIN dwh.dim_station ss ON ft.start_station_key = ss.station_key
JOIN dwh.dim_station es ON ft.end_station_key = es.station_key
WHERE ft.trip_status = 'completed'
  AND ft.start_station_key != ft.end_station_key
GROUP BY ss.station_id, ss.station_name, es.station_id, es.station_name
ORDER BY trips_count DESC
LIMIT 10;

-- РАЗДЕЛ 6: АНАЛИЗ ТРАНСПОРТНЫХ СРЕДСТВ

-- 6.1 Статистика по типам транспортных средств
SELECT
    dv.vehicle_type,
    COUNT(DISTINCT dv.vehicle_key) as vehicles_count,
    AVG(dv.vehicle_age) as avg_age,
    AVG(dv.capacity) as avg_capacity,
    COUNT(ft.trip_key) as total_trips
FROM dwh.dim_vehicle dv
LEFT JOIN dwh.fact_trips ft ON dv.vehicle_key = ft.vehicle_key
WHERE dv.current_status = 'active'
GROUP BY dv.vehicle_type
ORDER BY vehicles_count DESC;

-- 6.2 Топ-10 самых загруженных транспортных средств
SELECT
    dv.vehicle_id,
    dv.vehicle_type,
    dr.route_number,
    COUNT(ft.trip_key) as trips_count,
    dv.manufacture_year,
    dv.vehicle_age
FROM dwh.dim_vehicle dv
JOIN dwh.fact_trips ft ON dv.vehicle_key = ft.vehicle_key
JOIN dwh.dim_route dr ON dv.current_route_id = dr.route_id AND dr.is_current = TRUE
WHERE ft.trip_status = 'completed'
GROUP BY dv.vehicle_id, dv.vehicle_type, dr.route_number, dv.manufacture_year, dv.vehicle_age
ORDER BY trips_count DESC
LIMIT 10;

-- РАЗДЕЛ 7: ВРЕМЕННОЙ АНАЛИЗ

-- 7.1 Динамика поездок по дням
SELECT
    dd.date,
    dd.day_name,
    dd.is_weekend,
    COUNT(*) as trips_count,
    SUM(ft.actual_fare) as daily_revenue
FROM dwh.fact_trips ft
JOIN dwh.dim_date dd ON ft.start_date_key = dd.date_key
WHERE ft.trip_status = 'completed'
GROUP BY dd.date, dd.day_name, dd.is_weekend
ORDER BY dd.date;

-- 7.2 Недельная статистика
SELECT
    dd.week,
    dd.year,
    COUNT(*) as trips_count,
    SUM(ft.actual_fare) as weekly_revenue,
    AVG(ft.duration_minutes) as avg_duration
FROM dwh.fact_trips ft
JOIN dwh.dim_date dd ON ft.start_date_key = dd.date_key
WHERE ft.trip_status = 'completed'
GROUP BY dd.year, dd.week
ORDER BY dd.year, dd.week;

-- РАЗДЕЛ 8: КОМПЛЕКСНЫЙ АНАЛИЗ (KPI)

-- 8.1 Общие показатели системы
SELECT
    COUNT(DISTINCT ft.user_key) as total_active_users,
    COUNT(DISTINCT ft.route_key) as total_active_routes,
    COUNT(DISTINCT ft.vehicle_key) as total_active_vehicles,
    COUNT(*) as total_trips,
    COUNT(CASE WHEN ft.trip_status = 'completed' THEN 1 END) as completed_trips,
    COUNT(CASE WHEN ft.trip_status = 'cancelled' THEN 1 END) as cancelled_trips,
    ROUND(100.0 * COUNT(CASE WHEN ft.trip_status = 'completed' THEN 1 END) / COUNT(*), 2) as completion_rate,
    SUM(ft.actual_fare) as total_revenue,
    AVG(ft.duration_minutes) as avg_trip_duration,
    AVG(ft.distance_km) as avg_trip_distance
FROM dwh.fact_trips ft;

-- 8.2 KPI по типам транспорта за последние 7 дней
SELECT
    dr.transport_type,
    COUNT(*) as trips_count,
    SUM(ft.actual_fare) as revenue,
    AVG(ft.duration_minutes) as avg_duration,
    COUNT(DISTINCT ft.user_key) as unique_passengers,
    COUNT(DISTINCT ft.vehicle_key) as active_vehicles
FROM dwh.fact_trips ft
JOIN dwh.dim_route dr ON ft.route_key = dr.route_key
JOIN dwh.dim_date dd ON ft.start_date_key = dd.date_key
WHERE ft.trip_status = 'completed'
  AND dd.date >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY dr.transport_type
ORDER BY revenue DESC;

-- РАЗДЕЛ 9: ЗАПРОСЫ К CLICKHOUSE ВИТРИНАМ

-- Эти запросы выполняются в ClickHouse для демонстрации производительности

-- 9.1 Топ-10 маршрутов по выручке за последний месяц
/*
SELECT
    route_number,
    route_name,
    SUM(total_revenue) as revenue,
    SUM(total_trips) as trips
FROM analytics.mart_route_hourly_stats
WHERE date >= today() - 30
GROUP BY route_number, route_name
ORDER BY revenue DESC
LIMIT 10;
*/

-- 9.2 Анализ пассажиропотока по часам
/*
SELECT
    hour,
    AVG(total_trips) as avg_trips,
    AVG(total_revenue) as avg_revenue,
    AVG(unique_passengers) as avg_passengers
FROM analytics.mart_passenger_flow_by_hour
WHERE date >= today() - 7
GROUP BY hour
ORDER BY hour;
*/

-- 9.3 Сегментация пользователей
/*
SELECT
    user_segment,
    COUNT(*) as users_count,
    AVG(trips_last_30d) as avg_trips,
    AVG(total_spent_last_30d) as avg_spent,
    AVG(days_since_last_trip) as avg_days_inactive
FROM analytics.mart_user_segmentation
GROUP BY user_segment
ORDER BY users_count DESC;
*/

-- РАЗДЕЛ 10: ADVANCED АНАЛИТИКА

-- 10.1 Когортный анализ пользователей по месяцам регистрации
WITH user_cohorts AS (
    SELECT
        user_key,
        DATE_TRUNC('month', registration_date) as cohort_month
    FROM dwh.dim_user
),
cohort_activity AS (
    SELECT
        uc.cohort_month,
        DATE_TRUNC('month', ft.start_timestamp)::date as activity_month,
        COUNT(DISTINCT uc.user_key) as active_users
    FROM user_cohorts uc
    JOIN dwh.fact_trips ft ON uc.user_key = ft.user_key
    WHERE ft.trip_status = 'completed'
    GROUP BY uc.cohort_month, activity_month
)
SELECT
    cohort_month,
    activity_month,
    active_users,
    ROUND(100.0 * active_users /
          FIRST_VALUE(active_users) OVER (
              PARTITION BY cohort_month
              ORDER BY activity_month
          ), 2) as retention_pct
FROM cohort_activity
ORDER BY cohort_month, activity_month;

-- 10.2 Анализ влияния изменения тарифа на спрос (SCD Type 2)
WITH fare_periods AS (
    SELECT
        route_id,
        route_number,
        base_fare,
        effective_date,
        COALESCE(expiration_date, CURRENT_DATE) as period_end,
        is_current
    FROM dwh.dim_route
),
demand_by_period AS (
    SELECT
        fp.route_id,
        fp.route_number,
        fp.base_fare,
        fp.effective_date,
        COUNT(ft.trip_key) as trips_count,
        AVG(ft.duration_minutes) as avg_duration
    FROM fare_periods fp
    JOIN dwh.fact_trips ft ON fp.route_id IN (
        SELECT route_id FROM dwh.dim_route WHERE route_key = ft.route_key
    )
    JOIN dwh.dim_date dd ON ft.start_date_key = dd.date_key
    WHERE dd.date BETWEEN fp.effective_date AND fp.period_end
      AND ft.trip_status = 'completed'
    GROUP BY fp.route_id, fp.route_number, fp.base_fare, fp.effective_date
)
SELECT
    route_number,
    base_fare,
    effective_date,
    trips_count,
    LAG(trips_count) OVER (PARTITION BY route_id ORDER BY effective_date) as prev_trips,
    trips_count - LAG(trips_count) OVER (PARTITION BY route_id ORDER BY effective_date) as demand_change
FROM demand_by_period
ORDER BY route_number, effective_date;

-- 10.3 RFM анализ пользователей (Recency, Frequency, Monetary)
WITH user_rfm AS (
    SELECT
        du.user_key,
        du.username,
        -- Recency: дней с последней поездки
        EXTRACT(DAY FROM CURRENT_DATE - MAX(dd.date)) as recency_days,
        -- Frequency: количество поездок
        COUNT(ft.trip_key) as frequency,
        -- Monetary: общая сумма потраченная
        SUM(ft.actual_fare) as monetary
    FROM dwh.dim_user du
    LEFT JOIN dwh.fact_trips ft ON du.user_key = ft.user_key
    LEFT JOIN dwh.dim_date dd ON ft.start_date_key = dd.date_key
    WHERE ft.trip_status = 'completed' OR ft.trip_key IS NULL
    GROUP BY du.user_key, du.username
)
SELECT
    username,
    recency_days,
    frequency,
    monetary,
    -- RFM скоринг (1-5, где 5 - лучший)
    NTILE(5) OVER (ORDER BY recency_days DESC) as R_score,
    NTILE(5) OVER (ORDER BY frequency) as F_score,
    NTILE(5) OVER (ORDER BY monetary) as M_score,
    -- Общий RFM сегмент
    CASE
        WHEN NTILE(5) OVER (ORDER BY recency_days DESC) >= 4
         AND NTILE(5) OVER (ORDER BY frequency) >= 4
         AND NTILE(5) OVER (ORDER BY monetary) >= 4 THEN 'VIP'
        WHEN NTILE(5) OVER (ORDER BY frequency) >= 4 THEN 'Loyal'
        WHEN NTILE(5) OVER (ORDER BY recency_days DESC) <= 2 THEN 'At Risk'
        ELSE 'Regular'
    END as rfm_segment
FROM user_rfm
ORDER BY monetary DESC
LIMIT 100;

