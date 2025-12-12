# Архитектурный документ MetroPulse DWH

## 1. Описание источников данных

### 1.1 Kafka Stream: vehicle_positions

Поток событий от транспортных средств, поступающий каждые 10 секунд.

**Структура события:**
```json
{
  "vehicle_id": "BUS_001",
  "route_id": "R45",
  "latitude": 55.7558,
  "longitude": 37.6173,
  "speed": 45.5,
  "timestamp": "2025-12-12T10:30:45Z",
  "vehicle_type": "bus",
  "bearing": 180,
  "occupancy": "many_seats_available"
}
```

**Характеристики:**
- Частота: ~6 событий в минуту на единицу транспорта
- Объем: ~1000 единиц транспорта → 6000 событий/мин → 8.6M событий/день
- Формат: JSON в Kafka topic

### 1.2 PostgreSQL OLTP Database

Основная транзакционная база данных.

**Основные сущности:**

#### Users (Пользователи)
- user_id (PK)
- username
- email
- phone
- registration_date
- status (active/suspended)
- balance

#### Routes (Маршруты)
- route_id (PK)
- route_number
- route_name
- transport_type (bus/tram/metro)
- base_fare
- is_active
- created_at
- updated_at

#### Trips (Поездки)
- trip_id (PK)
- user_id (FK)
- route_id (FK)
- vehicle_id
- start_time
- end_time
- start_station
- end_station
- distance_km
- status (completed/cancelled)

#### Transactions (Транзакции)
- transaction_id (PK)
- user_id (FK)
- trip_id (FK)
- amount
- payment_method (card/cash/subscription)
- transaction_date
- status (success/failed/refunded)

#### Stations (Станции/Остановки)
- station_id (PK)
- station_name
- latitude
- longitude
- station_type (bus_stop/metro_station/tram_stop)

#### Route_Stations (Связь маршрутов и станций)
- route_id (FK)
- station_id (FK)
- sequence_number
- estimated_travel_time_minutes

**Характеристики:**
- Объем: ~100K активных пользователей
- Транзакции: ~500K поездок в день
- Нормализованная структура (3NF)

## 2. Выбор методологии моделирования DWH

### 2.1 Сравнение подходов

| Критерий | Kimball (Star Schema) | Data Vault 2.0 |
|----------|----------------------|----------------|
| Сложность внедрения | Низкая | Высокая |
| Гибкость к изменениям | Средняя | Очень высокая |
| Производительность запросов | Отличная | Хорошая (требует представлений) |
| Скорость разработки | Быстрая | Медленная |
| Аудит изменений | Ограниченный | Полный |
| Понятность бизнесу | Высокая | Низкая |

### 2.2 Обоснование выбора: Kimball (Dimensional Modeling)

**Выбрана методология Кимбалла** по следующим причинам:

1. **Скорость внедрения**: Стартап требует быстрого MVP
2. **Производительность**: Схема "звезда" оптимальна для аналитических запросов
3. **Простота**: Аналитики и менеджеры легко поймут структуру
4. **Достаточная гибкость**: SCD Type 2 покрывает требования к истории изменений
5. **Зрелость технологии**: Множество инструментов и best practices

**Риски и митигация:**
- Риск: Необходимость переделки схемы при изменении бизнес-требований
- Митигация: Использование промежуточного слоя Staging для гибкости

## 3. Архитектура слоев DWH

### 3.1 Staging Layer (Сырые данные)

**Цель**: Хранение данных в "сыром" виде из источников без трансформаций.

**Компоненты:**
- MinIO (S3): Parquet файлы с выгрузками из OLTP
- Staging таблицы: Временные таблицы для загрузки из Kafka

**Стратегия загрузки:**
- Full load для справочников (Routes, Stations)
- Incremental load для транзакционных данных (Trips, Transactions)
- Streaming для vehicle_positions

### 3.2 Core DWH Layer (Dimensional Model)

**Схема "Звезда"** с центральными таблицами фактов и окружающими измерениями.

#### Fact Tables

**1. fact_vehicle_positions** - Факты позиций транспорта
- position_key (SK)
- vehicle_key (FK)
- route_key (FK)
- time_key (FK)
- date_key (FK)
- latitude
- longitude
- speed
- bearing
- occupancy_level

**2. fact_trips** - Факты поездок
- trip_key (SK)
- user_key (FK)
- route_key (FK)
- vehicle_key (FK)
- start_time_key (FK)
- end_time_key (FK)
- start_station_key (FK)
- end_station_key (FK)
- distance_km
- duration_minutes
- trip_status

**3. fact_transactions** - Факты транзакций
- transaction_key (SK)
- user_key (FK)
- trip_key (FK)
- payment_method_key (FK)
- date_key (FK)
- time_key (FK)
- amount
- transaction_status

#### Dimension Tables

**1. dim_date** - Календарное измерение
- date_key (SK)
- date
- year, quarter, month, week, day
- day_of_week, day_name
- is_weekend, is_holiday
- fiscal_year, fiscal_quarter

**2. dim_time** - Временное измерение
- time_key (SK)
- time
- hour, minute, second
- hour_of_day (0-23)
- time_period (morning/afternoon/evening/night)
- is_rush_hour

**3. dim_user** - Пользователи (SCD Type 1)
- user_key (SK)
- user_id (NK)
- username
- email
- phone
- registration_date
- current_status
- current_balance

**4. dim_route** - Маршруты (SCD Type 2)
- route_key (SK)
- route_id (NK)
- route_number
- route_name
- transport_type
- base_fare
- is_active
- effective_date
- expiration_date
- is_current
- version

**5. dim_vehicle** - Транспортные средства (SCD Type 1)
- vehicle_key (SK)
- vehicle_id (NK)
- vehicle_type
- capacity
- manufacture_year
- last_maintenance_date

**6. dim_station** - Станции/Остановки (SCD Type 1)
- station_key (SK)
- station_id (NK)
- station_name
- latitude
- longitude
- station_type
- district
- zone

**7. dim_payment_method** - Способы оплаты
- payment_method_key (SK)
- payment_method_code
- payment_method_name
- payment_category

## 4. Slowly Changing Dimensions (SCD Type 2)

### 4.1 Выбор измерения: dim_route

**Обоснование:**
Маршруты подвержены изменениям:
- Изменение стоимости проезда (base_fare)
- Изменение схемы движения (набора остановок)
- Изменение статуса (активен/неактивен)
- Переименование маршрутов

**Критично для анализа:**
- Сравнение выручки до и после изменения цены
- Анализ популярности маршрута в разные периоды
- Историческая точность расчетов

### 4.2 Реализация SCD Type 2 для dim_route

**Дополнительные поля:**
- `effective_date` - дата начала действия версии
- `expiration_date` - дата окончания действия (NULL для текущей)
- `is_current` - флаг текущей версии (TRUE/FALSE)
- `version` - номер версии записи

**Пример версионирования:**

| route_key | route_id | route_number | base_fare | effective_date | expiration_date | is_current | version |
|-----------|----------|--------------|-----------|----------------|-----------------|------------|---------|
| 1001 | R45 | 45 | 50.00 | 2025-01-01 | 2025-06-30 | FALSE | 1 |
| 1002 | R45 | 45 | 55.00 | 2025-07-01 | NULL | TRUE | 2 |

**Логика обновления:**

1. При поступлении изменения:
   - Закрыть текущую запись (expiration_date = today, is_current = FALSE)
   - Вставить новую запись (effective_date = today, is_current = TRUE, version++)

2. При запросе данных:
   - Для текущего состояния: WHERE is_current = TRUE
   - Для исторического: WHERE effective_date <= :date AND (expiration_date > :date OR expiration_date IS NULL)

## 5. Физическая модель и оптимизация

### 5.1 Индексы

**Факты:**
- Кластеризованные индексы по date_key/time_key
- B-tree индексы по всем FK измерениям
- Composite индексы для частых комбинаций фильтров

**Измерения:**
- PK индексы (route_key, user_key, etc.)
- Индексы по NK (business keys)
- Для SCD Type 2: индексы по (NK, is_current) и (NK, effective_date, expiration_date)

### 5.2 Партиционирование

**fact_vehicle_positions**: Партиционирование по date_key (по дням, retention 90 дней)
**fact_trips**: Партиционирование по date_key (по месяцам, retention 3 года)
**fact_transactions**: Партиционирование по date_key (по месяцам, retention 7 лет - финансовые данные)

### 5.3 Стратегия загрузки

**Dimension Load Strategy:**
1. Load staging tables
2. Perform SCD logic (detect changes)
3. Update/Insert dimensions
4. Generate surrogate keys

**Fact Load Strategy:**
1. Load staging tables
2. Lookup surrogate keys from dimensions
3. Insert facts (append-only)
4. Update aggregates


