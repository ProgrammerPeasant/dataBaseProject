# MetroPulse - Аналитическая платформа для мониторинга общественного транспорта

[![Python](https://img.shields.io/badge/Python-3.9+-blue.svg)](https://www.python.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-blue.svg)](https://www.postgresql.org/)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.4-orange.svg)](https://spark.apache.org/)
[![Docker](https://img.shields.io/badge/Docker-required-blue.svg)](https://www.docker.com/)

# Участники
*  **Серых Богдан Александрович БПИ232**
* **Пысин Артем Александрович БПИ232**

## Описание проекта

**MetroPulse** - это комплексная аналитическая платформа для обработки данных о движении общественного транспорта в реальном времени и анализа поведения пользователей мобильного приложения.

## Архитектура решения

### Источники данных

1. **Kafka Stream (vehicle_positions)** 
   - Частота: каждые 10 секунд
   - Объем: ~8.6M событий/день
   - Данные: GPS координаты, скорость, загруженность

2. **PostgreSQL OLTP**
   - Пользователи: ~100K активных
   - Поездки: ~500K в день
   - Транзакции, маршруты, станции

### Компоненты платформы

```
Sources → Staging → Core DWH → Data Marts → BI Tools
   ↓         ↓          ↓           ↓          ↓
 OLTP     MinIO    PostgreSQL  ClickHouse  Dashboards
 Kafka              (Star)      Redis
```

- **Staging Layer**: MinIO (S3) + PostgreSQL staging tables
- **Core DWH**: Star Schema (Kimball) с SCD Type 2
- **Batch Processing**: Apache Spark для ETL
- **Stream Processing**: Apache Flink для real-time
- **Data Marts**: ClickHouse (batch) / Redis (real-time)

## Структура проекта

```
databaseProject/
├── docs/                          # Документация
│   ├── architecture.md
│   ├── data_sources.md
│   └── presentation.pptx
├── stage1_dwh_design/            # Этап 1: Проектирование DWH
│   ├── ddl/
│   │   ├── staging/              # Staging таблицы
│   │   └── core/                 # Core DWH таблицы
│   ├── diagrams/
│   └── oltp_schema.sql
├── stage2_batch_pipeline/        # Этап 2: Batch пайплайн
│   ├── spark_jobs/
│   ├── data_generators/          # Генераторы тестовых данных
│   └── airflow_dags/
├── stage3_marts/                 # Этап 3: Витрины данных
│   ├── option_a_batch/           # Вариант А: ClickHouse
│   └── option_b_stream/          # Вариант Б: Flink + Redis
├── docker/
│   ├── docker-compose.yml
│   └── services/
├── scripts/
└── requirements.txt
```

## Быстрый старт

### Предварительные требования
- Docker и Docker Compose
- Python 3.9+
- Java 11+ (для Spark)

### Установка и запуск

1. Клонировать репозиторий
2. Запустить окружение:
```bash
docker-compose up -d
```

3. Установить Python зависимости:
```bash
pip install -r requirements.txt
```

4. Инициализировать схемы DWH:
```bash
python scripts/init_dwh.py
```

5. Запустить batch пайплайн:
```bash
python scripts/run_batch_pipeline.py
```

## Этапы реализации

### Этап 1: Проектирование DWH
- Выбор методологии: Kimball (Dimensional Modeling)
- Разработка схемы "звезда"
- Реализация SCD Type 2 для измерения маршрутов
- DDL скрипты для всех таблиц

### Этап 2: Batch пайплайн
- Docker окружение (Spark, PostgreSQL, MinIO)
- Генерация тестовых данных
- ETL процессы на PySpark
- Загрузка данных в DWH с поддержкой SCD Type 2

### Этап 3: Витрины данных
- **Вариант А**: Агрегированные витрины в ClickHouse
- **Вариант Б**: Real-time витрина с Flink и Redis


## Технологический стек

- **DWH**: PostgreSQL 15 / Greenplum
- **Batch Processing**: Apache Spark 3.4 (PySpark)
- **Stream Processing**: Apache Flink 1.17
- **Message Queue**: Apache Kafka 3.5
- **Object Storage**: MinIO (S3-compatible)
- **Analytics DB**: ClickHouse 23.8
- **In-Memory DB**: Redis 7.2
- **Orchestration**: Apache Airflow 2.7 (описание)
- **Language**: Python 3.9+