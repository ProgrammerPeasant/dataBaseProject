from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, current_timestamp, to_date, to_timestamp,
    year, month, dayofmonth, hour, minute, second,
    when, row_number, max as spark_max, coalesce
)
from pyspark.sql.window import Window
from pyspark.sql.types import *
from datetime import datetime, date
import logging

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class MetroPulseETL:
    """ETL Pipeline для загрузки данных в MetroPulse DWH"""

    def __init__(self, config):
        """
        Инициализация ETL

        Args:
            config: dict с параметрами подключения
        """
        self.config = config
        self.spark = self._create_spark_session()

    def _create_spark_session(self):
        """Создание Spark сессии с необходимыми конфигурациями"""
        logger.info("Создание Spark сессии...")

        spark = SparkSession.builder \
            .appName("MetroPulse-ETL") \
            .config("spark.jars.packages",
                   "org.postgresql:postgresql:42.6.0,"
                   "org.apache.hadoop:hadoop-aws:3.3.4") \
            .config("spark.hadoop.fs.s3a.endpoint", self.config.get('minio_endpoint', 'http://minio:9000')) \
            .config("spark.hadoop.fs.s3a.access.key", self.config.get('minio_access_key', 'minioadmin')) \
            .config("spark.hadoop.fs.s3a.secret.key", self.config.get('minio_secret_key', 'minioadmin')) \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .getOrCreate()

        spark.sparkContext.setLogLevel("WARN")
        logger.info("Spark сессия создана успешно")
        return spark

    def get_jdbc_properties(self):
        """Получить JDBC properties для PostgreSQL"""
        return {
            "user": self.config.get('pg_user', 'postgres'),
            "password": self.config.get('pg_password', 'postgres'),
            "driver": "org.postgresql.Driver"
        }

    def read_from_staging(self, table_name):
        """Чтение данных из staging таблицы PostgreSQL"""
        jdbc_url = self.config.get('pg_jdbc_url', 'jdbc:postgresql://postgres:5432/metropulse')

        logger.info(f"Чтение данных из staging.{table_name}...")
        df = self.spark.read \
            .jdbc(jdbc_url, f"staging.{table_name}", properties=self.get_jdbc_properties())

        logger.info(f"Прочитано {df.count()} записей из staging.{table_name}")
        return df

    def read_from_parquet(self, path):
        """Чтение данных из Parquet файлов в MinIO"""
        logger.info(f"Чтение данных из {path}...")
        df = self.spark.read.parquet(path)
        logger.info(f"Прочитано {df.count()} записей")
        return df

    def write_to_dwh(self, df, table_name, mode="append"):
        """Запись данных в DWH таблицу"""
        jdbc_url = self.config.get('pg_jdbc_url', 'jdbc:postgresql://postgres:5432/metropulse')

        logger.info(f"Запись данных в dwh.{table_name} (mode={mode})...")
        df.write \
            .jdbc(jdbc_url, f"dwh.{table_name}",
                  mode=mode,
                  properties=self.get_jdbc_properties())

        logger.info(f"Записано {df.count()} записей в dwh.{table_name}")

    def load_dim_user(self, df_users):
        """
        Загрузка измерения пользователей (SCD Type 1)
        Обновляет существующие записи или вставляет новые
        """
        logger.info("Загрузка dim_user...")

        # Трансформация данных
        df_dim_user = df_users.select(
            col("user_id"),
            col("username"),
            col("email"),
            col("phone"),
            col("first_name"),
            col("last_name"),
            when(col("first_name").isNotNull() & col("last_name").isNotNull(),
                 concat(col("first_name"), lit(" "), col("last_name"))
            ).otherwise(col("username")).alias("full_name"),
            col("date_of_birth"),
            when(year(current_timestamp()) - year(col("date_of_birth")).between(18, 25), "18-25")
            .when(year(current_timestamp()) - year(col("date_of_birth")).between(26, 35), "26-35")
            .when(year(current_timestamp()) - year(col("date_of_birth")).between(36, 50), "36-50")
            .otherwise("51+").alias("age_group"),
            to_date(col("registration_date")).alias("registration_date"),
            col("status").alias("current_status"),
            col("balance").cast("decimal(10,2)").alias("current_balance"),
            lit("occasional").alias("user_segment"),  # Будет обновлено позже на основе активности
            current_timestamp().alias("created_date"),
            current_timestamp().alias("updated_date"),
            lit("OLTP").alias("source_system")
        )

        # В реальности нужно делать upsert, но для упрощения делаем truncate + insert
        # Для production использовать merge или temporary table
        self.write_to_dwh(df_dim_user, "dim_user", mode="overwrite")

        logger.info("dim_user загружена успешно")
        return df_dim_user

    def load_dim_route_scd2(self, df_routes):
        """
        Загрузка измерения маршрутов с SCD Type 2
        Отслеживает историю изменений base_fare и других атрибутов
        """
        logger.info("Загрузка dim_route с SCD Type 2...")

        # Читаем текущее состояние dim_route
        jdbc_url = self.config.get('pg_jdbc_url', 'jdbc:postgresql://postgres:5432/metropulse')

        try:
            df_existing = self.spark.read \
                .jdbc(jdbc_url, "dwh.dim_route", properties=self.get_jdbc_properties())
            logger.info(f"Найдено {df_existing.count()} существующих записей в dim_route")
        except Exception as e:
            logger.info("dim_route пуста, выполняем начальную загрузку")
            df_existing = self.spark.createDataFrame([], schema=StructType([
                StructField("route_key", LongType(), False),
                StructField("route_id", StringType(), False),
                StructField("route_number", StringType(), False),
                StructField("route_name", StringType(), False),
                StructField("transport_type", StringType(), False),
                StructField("base_fare", DecimalType(10, 2), False),
                StructField("is_active", BooleanType(), False),
                StructField("effective_date", DateType(), False),
                StructField("expiration_date", DateType(), True),
                StructField("is_current", BooleanType(), False),
                StructField("version", IntegerType(), False)
            ]))

        # Подготовка новых данных
        df_new = df_routes.select(
            col("route_id"),
            col("route_number"),
            col("route_name"),
            col("transport_type"),
            when(col("transport_type") == "bus", "Автобус")
            .when(col("transport_type") == "tram", "Трамвай")
            .when(col("transport_type") == "metro", "Метро")
            .otherwise("Неизвестно").alias("transport_type_name"),
            col("base_fare").cast("decimal(10,2)"),
            col("is_active"),
            col("description")
        )

        if df_existing.count() == 0:
            # Начальная загрузка - все записи с version 1
            logger.info("Начальная загрузка dim_route...")

            df_initial = df_new.withColumn("effective_date", lit(date.today())) \
                .withColumn("expiration_date", lit(None).cast(DateType())) \
                .withColumn("is_current", lit(True)) \
                .withColumn("version", lit(1)) \
                .withColumn("created_date", current_timestamp()) \
                .withColumn("updated_date", current_timestamp()) \
                .withColumn("source_system", lit("OLTP"))

            # Добавляем surrogate key
            window = Window.orderBy("route_id")
            df_initial = df_initial.withColumn("route_key", row_number().over(window))

            self.write_to_dwh(df_initial, "dim_route", mode="overwrite")
            logger.info(f"Начальная загрузка: {df_initial.count()} записей")

        else:
            # SCD Type 2 логика: поиск изменений
            logger.info("Обработка изменений (SCD Type 2)...")

            # Только текущие версии
            df_current = df_existing.filter(col("is_current") == True)

            # Join для поиска изменений
            df_changes = df_current.alias("cur").join(
                df_new.alias("new"),
                col("cur.route_id") == col("new.route_id"),
                "full_outer"
            )

            # Новые маршруты (есть в new, нет в current)
            df_inserts = df_changes.filter(col("cur.route_id").isNull()) \
                .select(
                    col("new.*"),
                    lit(date.today()).alias("effective_date"),
                    lit(None).cast(DateType()).alias("expiration_date"),
                    lit(True).alias("is_current"),
                    lit(1).alias("version")
                )

            # Измененные маршруты (base_fare изменилась)
            df_updates = df_changes.filter(
                (col("cur.route_id").isNotNull()) &
                (col("new.route_id").isNotNull()) &
                (col("cur.base_fare") != col("new.base_fare"))
            ).select(col("new.*"))

            # Для измененных: закрываем старые версии и создаем новые
            if df_updates.count() > 0:
                logger.info(f"Найдено {df_updates.count()} измененных маршрутов")

                # Закрываем старые версии (expiration_date, is_current = false)
                # В реальности нужно делать UPDATE в БД
                # Для упрощения пересоздаем всю таблицу

                # Получаем максимальный route_key
                max_key = df_existing.agg(spark_max("route_key")).collect()[0][0] or 0

                # Создаем новые версии
                df_new_versions = df_updates \
                    .join(df_current, "route_id") \
                    .select(
                        col("df_updates.*"),
                        (col("version") + 1).alias("version"),
                        lit(date.today()).alias("effective_date"),
                        lit(None).cast(DateType()).alias("expiration_date"),
                        lit(True).alias("is_current")
                    )

                logger.warning("SCD Type 2 UPDATE требует дополнительной логики")

            if df_inserts.count() > 0:
                logger.info(f"Вставка {df_inserts.count()} новых маршрутов")
                # Добавляем новые маршруты
                max_key = df_existing.agg(spark_max("route_key")).collect()[0][0] or 0
                window = Window.orderBy("route_id")
                df_inserts = df_inserts.withColumn("route_key", row_number().over(window) + max_key)

                self.write_to_dwh(df_inserts, "dim_route", mode="append")

        logger.info("dim_route загружена успешно")

    def load_dim_station(self, df_stations):
        """Загрузка измерения станций (SCD Type 1)"""
        logger.info("Загрузка dim_station...")

        df_dim_station = df_stations.select(
            col("station_id"),
            col("station_name"),
            col("latitude").cast("decimal(10,8)"),
            col("longitude").cast("decimal(11,8)"),
            col("station_type"),
            when(col("station_type") == "bus_stop", "Автобусная остановка")
            .when(col("station_type") == "tram_stop", "Трамвайная остановка")
            .when(col("station_type") == "metro_station", "Станция метро")
            .otherwise("Неизвестно").alias("station_type_name"),
            col("district"),
            col("zone"),
            col("is_active"),
            lit(False).alias("is_transfer_station"),  # Будет обновлено позже
            current_timestamp().alias("created_date"),
            current_timestamp().alias("updated_date"),
            lit("OLTP").alias("source_system")
        )

        # Добавляем surrogate key
        window = Window.orderBy("station_id")
        df_dim_station = df_dim_station.withColumn("station_key", row_number().over(window))

        self.write_to_dwh(df_dim_station, "dim_station", mode="overwrite")
        logger.info("dim_station загружена успешно")

    def load_dim_vehicle(self, df_vehicles):
        """Загрузка измерения транспортных средств (SCD Type 1)"""
        logger.info("Загрузка dim_vehicle...")

        df_dim_vehicle = df_vehicles.select(
            col("vehicle_id"),
            col("vehicle_type"),
            when(col("vehicle_type") == "bus", "Автобус")
            .when(col("vehicle_type") == "tram", "Трамвай")
            .when(col("vehicle_type") == "metro", "Вагон метро")
            .otherwise("Неизвестно").alias("vehicle_type_name"),
            col("route_id").alias("current_route_id"),
            col("capacity"),
            col("manufacture_year"),
            (year(current_timestamp()) - col("manufacture_year")).alias("vehicle_age"),
            col("model"),
            col("license_plate"),
            to_date(col("last_maintenance_date")).alias("last_maintenance_date"),
            when(
                (current_timestamp().cast("date").cast("long") -
                 to_date(col("last_maintenance_date")).cast("long")) <= 30, "up_to_date"
            ).when(
                (current_timestamp().cast("date").cast("long") -
                 to_date(col("last_maintenance_date")).cast("long")) <= 60, "due_soon"
            ).otherwise("overdue").alias("maintenance_status"),
            col("status").alias("current_status"),
            current_timestamp().alias("created_date"),
            current_timestamp().alias("updated_date"),
            lit("OLTP").alias("source_system")
        )

        # Добавляем surrogate key
        window = Window.orderBy("vehicle_id")
        df_dim_vehicle = df_dim_vehicle.withColumn("vehicle_key", row_number().over(window))

        self.write_to_dwh(df_dim_vehicle, "dim_vehicle", mode="overwrite")
        logger.info("dim_vehicle загружена успешно")

    def load_fact_trips(self, df_trips, df_dim_user, df_dim_route, df_dim_vehicle, df_dim_station):
        """Загрузка фактов поездок"""
        logger.info("Загрузка fact_trips...")

        # Join с измерениями для получения surrogate keys
        df_fact = df_trips \
            .join(df_dim_user.select("user_key", "user_id"), "user_id", "left") \
            .join(df_dim_route.filter(col("is_current") == True)
                  .select("route_key", col("route_id").alias("r_route_id")),
                  df_trips.route_id == col("r_route_id"), "left") \
            .join(df_dim_vehicle.select("vehicle_key", col("vehicle_id").alias("v_vehicle_id")),
                  df_trips.vehicle_id == col("v_vehicle_id"), "left") \
            .join(df_dim_station.select(col("station_key").alias("start_station_key"),
                                       col("station_id").alias("start_station_id")),
                  df_trips.start_station_id == col("start_station_id"), "left") \
            .join(df_dim_station.select(col("station_key").alias("end_station_key"),
                                       col("station_id").alias("end_station_id")),
                  df_trips.end_station_id == col("end_station_id"), "left")

        # Подготовка итоговых данных
        df_fact_final = df_fact.select(
            col("user_key"),
            col("route_key"),
            col("vehicle_key"),
            # Date/Time keys
            (year(col("start_time")) * 10000 +
             month(col("start_time")) * 100 +
             dayofmonth(col("start_time"))).alias("start_date_key"),
            (hour(col("start_time")) * 10000 +
             minute(col("start_time")) * 100 +
             second(col("start_time"))).alias("start_time_key"),
            when(col("end_time").isNotNull(),
                 year(col("end_time")) * 10000 +
                 month(col("end_time")) * 100 +
                 dayofmonth(col("end_time"))
            ).alias("end_date_key"),
            when(col("end_time").isNotNull(),
                 hour(col("end_time")) * 10000 +
                 minute(col("end_time")) * 100 +
                 second(col("end_time"))
            ).alias("end_time_key"),
            col("start_station_key"),
            col("end_station_key"),
            # Measures
            col("distance_km").cast("decimal(6,2)"),
            when(col("end_time").isNotNull(),
                 (col("end_time").cast("long") - col("start_time").cast("long")) / 60
            ).cast("integer").alias("duration_minutes"),
            lit(50.0).cast("decimal(10,2)").alias("actual_fare"),  # Заглушка, нужно брать из транзакций
            # Degenerate dimensions
            col("trip_id"),
            col("status").alias("trip_status"),
            col("start_time").alias("start_timestamp"),
            col("end_time").alias("end_timestamp"),
            # Metadata
            current_timestamp().alias("created_date"),
            lit("OLTP").alias("source_system")
        )

        self.write_to_dwh(df_fact_final, "fact_trips", mode="append")
        logger.info(f"fact_trips загружена: {df_fact_final.count()} записей")

    def run_full_pipeline(self):
        """Запуск полного ETL пайплайна"""
        logger.info("=== Запуск полного ETL пайплайна ===")

        try:
            # 1. Загрузка измерений
            logger.info("Этап 1: Загрузка измерений")

            df_users_staging = self.read_from_staging("stg_users")
            df_dim_user = self.load_dim_user(df_users_staging)

            df_routes_staging = self.read_from_staging("stg_routes")
            self.load_dim_route_scd2(df_routes_staging)

            df_stations_staging = self.read_from_staging("stg_stations")
            self.load_dim_station(df_stations_staging)

            df_vehicles_staging = self.read_from_staging("stg_vehicles")
            self.load_dim_vehicle(df_vehicles_staging)

            # 2. Перечитываем измерения для получения surrogate keys
            logger.info("Этап 2: Чтение измерений для фактов")
            jdbc_url = self.config.get('pg_jdbc_url', 'jdbc:postgresql://postgres:5432/metropulse')

            df_dim_user = self.spark.read.jdbc(jdbc_url, "dwh.dim_user",
                                              properties=self.get_jdbc_properties())
            df_dim_route = self.spark.read.jdbc(jdbc_url, "dwh.dim_route",
                                                properties=self.get_jdbc_properties())
            df_dim_vehicle = self.spark.read.jdbc(jdbc_url, "dwh.dim_vehicle",
                                                  properties=self.get_jdbc_properties())
            df_dim_station = self.spark.read.jdbc(jdbc_url, "dwh.dim_station",
                                                  properties=self.get_jdbc_properties())

            # 3. Загрузка фактов
            logger.info("Этап 3: Загрузка фактов")

            df_trips_staging = self.read_from_staging("stg_trips")
            self.load_fact_trips(df_trips_staging, df_dim_user, df_dim_route,
                               df_dim_vehicle, df_dim_station)

            logger.info("=== ETL пайплайн завершен успешно ===")

        except Exception as e:
            logger.error(f"Ошибка в ETL пайплайне: {str(e)}", exc_info=True)
            raise
        finally:
            self.spark.stop()


if __name__ == "__main__":
    # Конфигурация подключений
    config = {
        'pg_jdbc_url': 'jdbc:postgresql://localhost:5432/metropulse',
        'pg_user': 'postgres',
        'pg_password': 'postgres',
        'minio_endpoint': 'http://localhost:9000',
        'minio_access_key': 'minioadmin',
        'minio_secret_key': 'minioadmin'
    }

    # Запуск ETL
    etl = MetroPulseETL(config)
    etl.run_full_pipeline()

