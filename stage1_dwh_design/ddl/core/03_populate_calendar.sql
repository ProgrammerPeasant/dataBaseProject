-- MetroPulse DWH - Заполнение календарных измерений
-- PostgreSQL 15+
-- Описание: Генерация данных для dim_date и dim_time

-- ЗАПОЛНЕНИЕ dim_date (календарное измерение)

-- Функция для генерации календаря
CREATE OR REPLACE FUNCTION dwh.populate_dim_date(
    start_date DATE,
    end_date DATE
) RETURNS INTEGER AS $$
DECLARE
    current_date DATE;
    rows_inserted INTEGER := 0;
BEGIN
    current_date := start_date;

    WHILE current_date <= end_date LOOP
        INSERT INTO dwh.dim_date (
            date_key,
            date,
            year,
            quarter,
            month,
            month_name,
            week,
            day,
            day_of_week,
            day_name,
            day_of_year,
            is_weekend,
            is_holiday,
            fiscal_year,
            fiscal_quarter,
            fiscal_month,
            week_start_date,
            week_end_date,
            month_start_date,
            month_end_date,
            quarter_start_date,
            quarter_end_date,
            year_start_date,
            year_end_date
        )
        SELECT
            TO_CHAR(current_date, 'YYYYMMDD')::INTEGER,
            current_date,
            EXTRACT(YEAR FROM current_date)::INTEGER,
            EXTRACT(QUARTER FROM current_date)::INTEGER,
            EXTRACT(MONTH FROM current_date)::INTEGER,
            TO_CHAR(current_date, 'Month'),
            EXTRACT(WEEK FROM current_date)::INTEGER,
            EXTRACT(DAY FROM current_date)::INTEGER,
            EXTRACT(DOW FROM current_date)::INTEGER + 1,  -- 1-7 (Пн-Вс)
            TO_CHAR(current_date, 'Day'),
            EXTRACT(DOY FROM current_date)::INTEGER,
            CASE WHEN EXTRACT(DOW FROM current_date) IN (0, 6) THEN TRUE ELSE FALSE END,
            FALSE,  -- is_holiday - заполним отдельно
            EXTRACT(YEAR FROM current_date)::INTEGER,  -- fiscal_year = calendar year
            EXTRACT(QUARTER FROM current_date)::INTEGER,
            EXTRACT(MONTH FROM current_date)::INTEGER,
            DATE_TRUNC('week', current_date)::DATE,
            (DATE_TRUNC('week', current_date) + INTERVAL '6 days')::DATE,
            DATE_TRUNC('month', current_date)::DATE,
            (DATE_TRUNC('month', current_date) + INTERVAL '1 month' - INTERVAL '1 day')::DATE,
            DATE_TRUNC('quarter', current_date)::DATE,
            (DATE_TRUNC('quarter', current_date) + INTERVAL '3 months' - INTERVAL '1 day')::DATE,
            DATE_TRUNC('year', current_date)::DATE,
            (DATE_TRUNC('year', current_date) + INTERVAL '1 year' - INTERVAL '1 day')::DATE
        ON CONFLICT (date_key) DO NOTHING;

        rows_inserted := rows_inserted + 1;
        current_date := current_date + INTERVAL '1 day';
    END LOOP;

    RETURN rows_inserted;
END;
$$ LANGUAGE plpgsql;

-- Генерация календаря на 2024-2026 годы
SELECT dwh.populate_dim_date('2024-01-01'::DATE, '2026-12-31'::DATE);

-- Обновление праздничных дней для России
UPDATE dwh.dim_date SET is_holiday = TRUE, holiday_name = 'Новый год'
WHERE month = 1 AND day IN (1, 2, 3, 4, 5, 6, 7, 8);

UPDATE dwh.dim_date SET is_holiday = TRUE, holiday_name = 'День защитника Отечества'
WHERE month = 2 AND day = 23;

UPDATE dwh.dim_date SET is_holiday = TRUE, holiday_name = 'Международный женский день'
WHERE month = 3 AND day = 8;

UPDATE dwh.dim_date SET is_holiday = TRUE, holiday_name = 'Праздник Весны и Труда'
WHERE month = 5 AND day = 1;

UPDATE dwh.dim_date SET is_holiday = TRUE, holiday_name = 'День Победы'
WHERE month = 5 AND day = 9;

UPDATE dwh.dim_date SET is_holiday = TRUE, holiday_name = 'День России'
WHERE month = 6 AND day = 12;

UPDATE dwh.dim_date SET is_holiday = TRUE, holiday_name = 'День народного единства'
WHERE month = 11 AND day = 4;

COMMENT ON FUNCTION dwh.populate_dim_date IS 'Заполнение календарного измерения';

-- ЗАПОЛНЕНИЕ dim_time (временное измерение)

-- Функция для генерации времени
CREATE OR REPLACE FUNCTION dwh.populate_dim_time() RETURNS INTEGER AS $$
DECLARE
    current_time TIME;
    current_hour INTEGER;
    current_minute INTEGER;
    current_second INTEGER;
    time_period VARCHAR(20);
    is_rush_hour BOOLEAN;
    rush_hour_type VARCHAR(20);
    rows_inserted INTEGER := 0;
BEGIN
    -- Генерация каждой минуты (можно изменить на каждую секунду если нужно)
    current_hour := 0;

    WHILE current_hour < 24 LOOP
        current_minute := 0;

        WHILE current_minute < 60 LOOP
            current_time := MAKE_TIME(current_hour, current_minute, 0);

            -- Определение периода дня
            time_period := CASE
                WHEN current_hour >= 6 AND current_hour < 12 THEN 'morning'
                WHEN current_hour >= 12 AND current_hour < 18 THEN 'afternoon'
                WHEN current_hour >= 18 AND current_hour < 22 THEN 'evening'
                ELSE 'night'
            END;

            -- Определение часа пик
            is_rush_hour := CASE
                WHEN (current_hour >= 7 AND current_hour < 10) OR
                     (current_hour >= 17 AND current_hour < 20) THEN TRUE
                ELSE FALSE
            END;

            rush_hour_type := CASE
                WHEN current_hour >= 7 AND current_hour < 10 THEN 'morning_rush'
                WHEN current_hour >= 17 AND current_hour < 20 THEN 'evening_rush'
                ELSE NULL
            END;

            INSERT INTO dwh.dim_time (
                time_key,
                time,
                hour,
                minute,
                second,
                hour_of_day,
                time_period,
                is_rush_hour,
                rush_hour_type
            ) VALUES (
                current_hour * 10000 + current_minute * 100,  -- HHMMSS format
                current_time,
                current_hour,
                current_minute,
                0,
                current_hour,
                time_period,
                is_rush_hour,
                rush_hour_type
            ) ON CONFLICT (time_key) DO NOTHING;

            rows_inserted := rows_inserted + 1;
            current_minute := current_minute + 1;
        END LOOP;

        current_hour := current_hour + 1;
    END LOOP;

    RETURN rows_inserted;
END;
$$ LANGUAGE plpgsql;

-- Генерация временного измерения
SELECT dwh.populate_dim_time();

COMMENT ON FUNCTION dwh.populate_dim_time IS 'Заполнение временного измерения';

-- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ для работы с календарем

-- Функция: Получить date_key по дате
CREATE OR REPLACE FUNCTION dwh.get_date_key(p_date DATE)
RETURNS INTEGER AS $$
BEGIN
    RETURN TO_CHAR(p_date, 'YYYYMMDD')::INTEGER;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- Функция: Получить time_key по времени
CREATE OR REPLACE FUNCTION dwh.get_time_key(p_time TIME)
RETURNS INTEGER AS $$
BEGIN
    RETURN EXTRACT(HOUR FROM p_time)::INTEGER * 10000 +
           EXTRACT(MINUTE FROM p_time)::INTEGER * 100 +
           EXTRACT(SECOND FROM p_time)::INTEGER;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- Функция: Получить date_key и time_key из timestamp
CREATE OR REPLACE FUNCTION dwh.get_datetime_keys(p_timestamp TIMESTAMP)
RETURNS TABLE(date_key INTEGER, time_key INTEGER) AS $$
BEGIN
    RETURN QUERY
    SELECT
        TO_CHAR(p_timestamp, 'YYYYMMDD')::INTEGER as date_key,
        (EXTRACT(HOUR FROM p_timestamp)::INTEGER * 10000 +
         EXTRACT(MINUTE FROM p_timestamp)::INTEGER * 100 +
         EXTRACT(SECOND FROM p_timestamp)::INTEGER) as time_key;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- ПРОВЕРКА заполненности
-- ============================================================================

-- Проверить количество дней в dim_date
SELECT
    'dim_date' as table_name,
    COUNT(*) as total_rows,
    MIN(date) as min_date,
    MAX(date) as max_date,
    COUNT(CASE WHEN is_weekend THEN 1 END) as weekend_days,
    COUNT(CASE WHEN is_holiday THEN 1 END) as holiday_days
FROM dwh.dim_date;

-- Проверить количество записей в dim_time
SELECT
    'dim_time' as table_name,
    COUNT(*) as total_rows,
    MIN(time) as min_time,
    MAX(time) as max_time,
    COUNT(CASE WHEN is_rush_hour THEN 1 END) as rush_hour_records
FROM dwh.dim_time;

-- Примеры использования вспомогательных функций
SELECT dwh.get_date_key('2025-12-12'::DATE) as date_key;
SELECT dwh.get_time_key('14:30:00'::TIME) as time_key;
SELECT * FROM dwh.get_datetime_keys('2025-12-12 14:30:45'::TIMESTAMP);

