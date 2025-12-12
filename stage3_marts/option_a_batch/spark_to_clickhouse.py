from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum as spark_sum, avg, max as spark_max, min as spark_min,
    countDistinct, to_date, hour, dayofweek, when, lit, current_timestamp,
    row_number, desc
)
from pyspark.sql.window import Window
import logging
from clickhouse_driver import Client

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ClickHouseDataMartBuilder:
    """Построение агрегированных витрин в ClickHouse"""

    def __init__(self, spark_config, dwh_config, clickhouse_config):
        self.spark = self._create_spark_session(spark_config)
        self.dwh_config = dwh_config
        self.ch_config = clickhouse_config
        self.ch_client = Client(
            host=clickhouse_config.get('host', 'localhost'),
            port=clickhouse_config.get('port', 9000),
            user=clickhouse_config.get('user', 'default'),
            password=clickhouse_config.get('password', ''),
            database=clickhouse_config.get('database', 'analytics')
        )

    def _create_spark_session(self, config):
        """Создание Spark сессии"""
        logger.info("Создание Spark сессии...")

        spark = SparkSession.builder \
            .appName("MetroPulse-ClickHouse-ETL") \
            .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
            .config("spark.sql.adaptive.enabled", "true") \
            .getOrCreate()

        spark.sparkContext.setLogLevel("WARN")
        return spark

    def read_from_dwh(self, query):
        """Чтение данных из DWH"""
        jdbc_url = self.dwh_config.get('jdbc_url', 'jdbc:postgresql://localhost:5432/metropulse')
        properties = {
            "user": self.dwh_config.get('user', 'postgres'),
            "password": self.dwh_config.get('password', 'postgres'),
            "driver": "org.postgresql.Driver"
        }

        logger.info(f"Чтение данных из DWH...")
        df = self.spark.read.jdbc(jdbc_url, f"({query}) as subquery", properties=properties)
        logger.info(f"Прочитано {df.count()} записей")
        return df

    def write_to_clickhouse(self, df, table_name):
        """Запись данных в ClickHouse"""
        logger.info(f"Запись данных в ClickHouse таблицу: {table_name}")

        # Собираем данные в driver (для небольших витрин)
        data = df.collect()

        if not data:
            logger.warning(f"Нет данных для записи в {table_name}")
            return

        # Подготовка данных для вставки
        columns = df.columns
        values = []
        for row in data:
            row_values = []
            for col_name in columns:
                value = row[col_name]
                if value is None:
                    row_values.append('NULL')
                elif isinstance(value, str):
                    row_values.append(f"'{value}'")
                else:
                    row_values.append(str(value))
            values.append(f"({','.join(row_values)})")

        # Формирование и выполнение INSERT запроса
        insert_query = f"INSERT INTO {table_name} ({','.join(columns)}) VALUES {','.join(values)}"

        try:
            self.ch_client.execute(f"TRUNCATE TABLE {table_name}")
            self.ch_client.execute(insert_query)
            logger.info(f"Записано {len(data)} записей в {table_name}")
        except Exception as e:
            logger.error(f"Ошибка записи в ClickHouse: {str(e)}")
            raise

    def build_mart_route_hourly_stats(self):
        """Построение витрины: Почасовая статистика по маршрутам"""
        logger.info("Построение mart_route_hourly_stats...")

        query = """
        SELECT
            dr.route_id,
            dr.route_number,
            dr.route_name,
            dr.transport_type,
            dd.date,
            dt.hour,
            COUNT(ft.trip_key) as total_trips,
            COUNT(DISTINCT ft.user_key) as unique_passengers,
            SUM(ft.distance_km) as total_distance_km,
            AVG(ft.duration_minutes) as avg_trip_duration_minutes,
            SUM(ft.actual_fare) as total_revenue,
            AVG(ft.actual_fare) as avg_fare
        FROM dwh.fact_trips ft
        JOIN dwh.dim_route dr ON ft.route_key = dr.route_key
        JOIN dwh.dim_date dd ON ft.start_date_key = dd.date_key
        JOIN dwh.dim_time dt ON ft.start_time_key = dt.time_key
        WHERE ft.trip_status = 'completed'
          AND dd.date >= CURRENT_DATE - INTERVAL '30 days'
        GROUP BY dr.route_id, dr.route_number, dr.route_name, dr.transport_type, dd.date, dt.hour
        ORDER BY dd.date DESC, dt.hour
        """

        df = self.read_from_dwh(query)

        # Добавляем created_at
        df = df.withColumn("created_at", current_timestamp())

        self.write_to_clickhouse(df, "analytics.mart_route_hourly_stats")
        logger.info("mart_route_hourly_stats построена успешно")

    def build_mart_station_popularity(self):
        """Построение витрины: Популярность станций"""
        logger.info("Построение mart_station_popularity...")

        # Отправления
        query_departures = """
        SELECT
            ds.station_id,
            ds.station_name,
            ds.station_type,
            ds.district,
            dd.date,
            COUNT(*) as departures_count,
            COUNT(DISTINCT ft.user_key) as unique_users_dep
        FROM dwh.fact_trips ft
        JOIN dwh.dim_station ds ON ft.start_station_key = ds.station_key
        JOIN dwh.dim_date dd ON ft.start_date_key = dd.date_key
        WHERE ft.trip_status = 'completed'
          AND dd.date >= CURRENT_DATE - INTERVAL '30 days'
        GROUP BY ds.station_id, ds.station_name, ds.station_type, ds.district, dd.date
        """

        df_dep = self.read_from_dwh(query_departures)

        # Прибытия
        query_arrivals = """
        SELECT
            ds.station_id,
            dd.date,
            COUNT(*) as arrivals_count,
            COUNT(DISTINCT ft.user_key) as unique_users_arr
        FROM dwh.fact_trips ft
        JOIN dwh.dim_station ds ON ft.end_station_key = ds.station_key
        JOIN dwh.dim_date dd ON ft.start_date_key = dd.date_key
        WHERE ft.trip_status = 'completed'
          AND dd.date >= CURRENT_DATE - INTERVAL '30 days'
        GROUP BY ds.station_id, dd.date
        """

        df_arr = self.read_from_dwh(query_arrivals)

        # Объединяем данные
        df = df_dep.join(
            df_arr.select("station_id", "date", "arrivals_count", "unique_users_arr"),
            ["station_id", "date"],
            "left"
        )

        df = df.withColumn("arrivals_count", when(col("arrivals_count").isNull(), 0).otherwise(col("arrivals_count"))) \
               .withColumn("total_traffic", col("departures_count") + col("arrivals_count")) \
               .withColumn("unique_users",
                          when(col("unique_users_arr") > col("unique_users_dep"), col("unique_users_arr"))
                          .otherwise(col("unique_users_dep"))) \
               .withColumn("created_at", current_timestamp())

        df_final = df.select(
            "station_id", "station_name", "station_type", "district", "date",
            "departures_count", "arrivals_count", "total_traffic", "unique_users", "created_at"
        )

        self.write_to_clickhouse(df_final, "analytics.mart_station_popularity")
        logger.info("mart_station_popularity построена успешно")

    def build_mart_passenger_flow_by_hour(self):
        """Построение витрины: Пассажиропоток по часам"""
        logger.info("Построение mart_passenger_flow_by_hour...")

        query = """
        SELECT
            dd.date,
            dt.hour,
            dd.day_of_week,
            CASE WHEN dd.is_weekend THEN 1 ELSE 0 END as is_weekend,
            CASE WHEN dt.is_rush_hour THEN 1 ELSE 0 END as is_rush_hour,
            COUNT(CASE WHEN dr.transport_type = 'bus' THEN 1 END) as bus_trips,
            COUNT(CASE WHEN dr.transport_type = 'tram' THEN 1 END) as tram_trips,
            COUNT(CASE WHEN dr.transport_type = 'metro' THEN 1 END) as metro_trips,
            COUNT(*) as total_trips,
            SUM(ft.actual_fare) as total_revenue,
            AVG(ft.actual_fare) as avg_revenue_per_trip,
            COUNT(DISTINCT ft.user_key) as unique_passengers
        FROM dwh.fact_trips ft
        JOIN dwh.dim_route dr ON ft.route_key = dr.route_key
        JOIN dwh.dim_date dd ON ft.start_date_key = dd.date_key
        JOIN dwh.dim_time dt ON ft.start_time_key = dt.time_key
        WHERE ft.trip_status = 'completed'
          AND dd.date >= CURRENT_DATE - INTERVAL '30 days'
        GROUP BY dd.date, dt.hour, dd.day_of_week, dd.is_weekend, dt.is_rush_hour
        ORDER BY dd.date DESC, dt.hour
        """

        df = self.read_from_dwh(query)
        df = df.withColumn("created_at", current_timestamp())

        self.write_to_clickhouse(df, "analytics.mart_passenger_flow_by_hour")
        logger.info("mart_passenger_flow_by_hour построена успешно")

    def build_mart_user_segmentation(self):
        """Построение витрины: Сегментация пользователей"""
        logger.info("Построение mart_user_segmentation...")

        query = """
        WITH user_stats AS (
            SELECT
                du.user_key,
                du.user_id,
                du.username,
                du.registration_date,
                COUNT(CASE WHEN dd.date >= CURRENT_DATE - INTERVAL '30 days' THEN 1 END) as trips_last_30d,
                SUM(CASE WHEN dd.date >= CURRENT_DATE - INTERVAL '30 days' THEN ft.actual_fare ELSE 0 END) as total_spent_last_30d,
                COUNT(*) as trips_total,
                SUM(ft.actual_fare) as total_spent_total,
                MAX(dd.date) as last_trip_date
            FROM dwh.dim_user du
            LEFT JOIN dwh.fact_trips ft ON du.user_key = ft.user_key AND ft.trip_status = 'completed'
            LEFT JOIN dwh.dim_date dd ON ft.start_date_key = dd.date_key
            GROUP BY du.user_key, du.user_id, du.username, du.registration_date
        ),
        favorite_routes AS (
            SELECT
                ft.user_key,
                dr.route_id,
                COUNT(*) as route_trips,
                ROW_NUMBER() OVER (PARTITION BY ft.user_key ORDER BY COUNT(*) DESC) as rn
            FROM dwh.fact_trips ft
            JOIN dwh.dim_route dr ON ft.route_key = dr.route_key
            WHERE ft.trip_status = 'completed'
            GROUP BY ft.user_key, dr.route_id
        )
        SELECT
            us.user_id,
            us.username,
            us.registration_date,
            CASE
                WHEN us.trips_last_30d >= 20 THEN 'frequent'
                WHEN us.trips_last_30d >= 5 THEN 'occasional'
                WHEN us.trips_last_30d >= 1 THEN 'rare'
                ELSE 'churned'
            END as user_segment,
            us.trips_last_30d,
            us.total_spent_last_30d,
            CAST(us.trips_last_30d / 4.0 AS DECIMAL(6,2)) as avg_trips_per_week,
            us.trips_total,
            us.total_spent_total,
            COALESCE(fr.route_id, 'N/A') as favorite_route_id,
            COALESCE(fr.route_trips, 0) as favorite_route_trips,
            us.last_trip_date,
            CASE
                WHEN us.last_trip_date IS NOT NULL 
                THEN EXTRACT(DAY FROM CURRENT_DATE - us.last_trip_date)
                ELSE 9999
            END as days_since_last_trip
        FROM user_stats us
        LEFT JOIN favorite_routes fr ON us.user_key = fr.user_key AND fr.rn = 1
        WHERE us.user_id IS NOT NULL
        """

        df = self.read_from_dwh(query)
        df = df.withColumn("updated_at", current_timestamp())

        self.write_to_clickhouse(df, "analytics.mart_user_segmentation")
        logger.info("mart_user_segmentation построена успешно")

    def build_mart_route_performance(self):
        """Построение витрины: Производительность маршрутов"""
        logger.info("Построение mart_route_performance...")

        query = """
        WITH route_metrics AS (
            SELECT
                dr.route_id,
                dr.route_number,
                dr.route_name,
                dr.transport_type,
                dd.date,
                SUM(ft.actual_fare) as total_revenue,
                COUNT(*) as total_trips,
                SUM(ft.distance_km) as total_distance_km,
                AVG(ft.duration_minutes) as avg_trip_duration_minutes,
                COUNT(DISTINCT ft.vehicle_key) as active_vehicles_count,
                COUNT(CASE WHEN ft.trip_status = 'completed' THEN 1 END) * 100.0 / COUNT(*) as completion_rate,
                COUNT(CASE WHEN ft.trip_status = 'cancelled' THEN 1 END) * 100.0 / COUNT(*) as cancellation_rate
            FROM dwh.fact_trips ft
            JOIN dwh.dim_route dr ON ft.route_key = dr.route_key
            JOIN dwh.dim_date dd ON ft.start_date_key = dd.date_key
            WHERE dd.date >= CURRENT_DATE - INTERVAL '30 days'
            GROUP BY dr.route_id, dr.route_number, dr.route_name, dr.transport_type, dd.date
        )
        SELECT
            route_id,
            route_number,
            route_name,
            transport_type,
            date,
            total_revenue,
            CASE WHEN total_distance_km > 0 THEN total_revenue / total_distance_km ELSE 0 END as revenue_per_km,
            CASE WHEN active_vehicles_count > 0 THEN total_revenue / active_vehicles_count ELSE 0 END as revenue_per_vehicle,
            total_trips,
            total_distance_km,
            avg_trip_duration_minutes,
            active_vehicles_count,
            completion_rate,
            cancellation_rate,
            ROW_NUMBER() OVER (PARTITION BY date ORDER BY total_trips DESC) as popularity_rank
        FROM route_metrics
        ORDER BY date DESC, total_revenue DESC
        """

        df = self.read_from_dwh(query)
        df = df.withColumn("created_at", current_timestamp())

        self.write_to_clickhouse(df, "analytics.mart_route_performance")
        logger.info("mart_route_performance построена успешно")

    def build_all_marts(self):
        """Построение всех витрин данных"""
        logger.info("=== Построение всех витрин данных в ClickHouse ===")

        try:
            self.build_mart_route_hourly_stats()
            self.build_mart_station_popularity()
            self.build_mart_passenger_flow_by_hour()
            self.build_mart_user_segmentation()
            self.build_mart_route_performance()

            logger.info("=== Все витрины успешно построены ===")

        except Exception as e:
            logger.error(f"Ошибка построения витрин: {str(e)}", exc_info=True)
            raise
        finally:
            self.spark.stop()


if __name__ == "__main__":
    spark_config = {}

    dwh_config = {
        'jdbc_url': 'jdbc:postgresql://localhost:5432/metropulse',
        'user': 'postgres',
        'password': 'postgres'
    }

    clickhouse_config = {
        'host': 'localhost',
        'port': 9000,
        'user': 'default',
        'password': '',
        'database': 'analytics'
    }

    builder = ClickHouseDataMartBuilder(spark_config, dwh_config, clickhouse_config)
    builder.build_all_marts()

