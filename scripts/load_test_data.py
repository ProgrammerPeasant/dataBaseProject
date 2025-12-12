import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from stage2_batch_pipeline.data_generators.generate_test_data import DataGenerator
import psycopg2
from psycopg2.extras import execute_batch
import logging
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DataLoader:
    """Загрузка сгенерированных данных в базу"""

    def __init__(self, config):
        self.config = config
        self.conn = None

    def connect(self):
        """Подключение к PostgreSQL"""
        logger.info("Подключение к PostgreSQL...")

        try:
            self.conn = psycopg2.connect(
                host=self.config.get('host', 'localhost'),
                port=self.config.get('port', 5432),
                database=self.config.get('database', 'metropulse'),
                user=self.config.get('user', 'postgres'),
                password=self.config.get('password', 'postgres')
            )
            self.conn.autocommit = False
            logger.info("Подключение установлено")
            return True
        except Exception as e:
            logger.error(f"Ошибка подключения: {str(e)}")
            return False

    def load_stations_to_oltp(self, stations):
        """Загрузка станций в OLTP"""
        logger.info(f"Загрузка {len(stations)} станций в oltp.stations...")

        query = """
        INSERT INTO oltp.stations 
        (station_id, station_name, latitude, longitude, station_type, district, zone, is_active, created_at)
        VALUES (%(station_id)s, %(station_name)s, %(latitude)s, %(longitude)s, 
                %(station_type)s, %(district)s, %(zone)s, %(is_active)s, %(created_at)s)
        ON CONFLICT (station_id) DO UPDATE SET
            station_name = EXCLUDED.station_name,
            latitude = EXCLUDED.latitude,
            longitude = EXCLUDED.longitude,
            updated_at = CURRENT_TIMESTAMP
        """

        cursor = self.conn.cursor()
        execute_batch(cursor, query, stations)
        self.conn.commit()
        cursor.close()
        logger.info("✓ Станции загружены")

    def load_routes_to_oltp(self, routes):
        """Загрузка маршрутов в OLTP"""
        logger.info(f"Загрузка {len(routes)} маршрутов в oltp.routes...")

        query = """
        INSERT INTO oltp.routes
        (route_id, route_number, route_name, transport_type, base_fare, is_active, description, created_at)
        VALUES (%(route_id)s, %(route_number)s, %(route_name)s, %(transport_type)s,
                %(base_fare)s, %(is_active)s, %(description)s, %(created_at)s)
        ON CONFLICT (route_id) DO UPDATE SET
            route_name = EXCLUDED.route_name,
            base_fare = EXCLUDED.base_fare,
            is_active = EXCLUDED.is_active,
            updated_at = CURRENT_TIMESTAMP
        """

        cursor = self.conn.cursor()
        execute_batch(cursor, query, routes)
        self.conn.commit()
        cursor.close()
        logger.info("✓ Маршруты загружены")

    def load_route_stations_to_oltp(self, route_stations):
        """Загрузка связей маршрутов и станций в OLTP"""
        logger.info(f"Загрузка {len(route_stations)} связей маршрут-станция...")

        query = """
        INSERT INTO oltp.route_stations
        (route_id, station_id, sequence_number, estimated_travel_time_minutes, distance_from_previous_km)
        VALUES (%(route_id)s, %(station_id)s, %(sequence_number)s, 
                %(estimated_travel_time_minutes)s, %(distance_from_previous_km)s)
        ON CONFLICT (route_id, station_id) DO UPDATE SET
            sequence_number = EXCLUDED.sequence_number,
            estimated_travel_time_minutes = EXCLUDED.estimated_travel_time_minutes
        """

        cursor = self.conn.cursor()
        execute_batch(cursor, query, route_stations)
        self.conn.commit()
        cursor.close()
        logger.info("✓ Связи маршрут-станция загружены")

    def load_vehicles_to_oltp(self, vehicles):
        """Загрузка транспортных средств в OLTP"""
        logger.info(f"Загрузка {len(vehicles)} транспортных средств...")

        query = """
        INSERT INTO oltp.vehicles
        (vehicle_id, vehicle_type, route_id, capacity, manufacture_year, model, 
         license_plate, last_maintenance_date, status, created_at)
        VALUES (%(vehicle_id)s, %(vehicle_type)s, %(route_id)s, %(capacity)s,
                %(manufacture_year)s, %(model)s, %(license_plate)s,
                %(last_maintenance_date)s, %(status)s, %(created_at)s)
        ON CONFLICT (vehicle_id) DO UPDATE SET
            route_id = EXCLUDED.route_id,
            status = EXCLUDED.status
        """

        cursor = self.conn.cursor()
        execute_batch(cursor, query, vehicles)
        self.conn.commit()
        cursor.close()
        logger.info("✓ Транспортные средства загружены")

    def load_users_to_oltp(self, users):
        """Загрузка пользователей в OLTP"""
        logger.info(f"Загрузка {len(users)} пользователей...")

        query = """
        INSERT INTO oltp.users
        (username, email, phone, password_hash, first_name, last_name, date_of_birth,
         registration_date, status, balance, created_at)
        VALUES (%(username)s, %(email)s, %(phone)s, %(password_hash)s, %(first_name)s,
                %(last_name)s, %(date_of_birth)s, %(registration_date)s, %(status)s,
                %(balance)s, %(created_at)s)
        ON CONFLICT (username) DO NOTHING
        RETURNING user_id
        """

        cursor = self.conn.cursor()
        # Для users нужно получить ID, поэтому используем цикл
        for user in users:
            cursor.execute(query, user)
        self.conn.commit()
        cursor.close()
        logger.info("✓ Пользователи загружены")

    def load_trips_to_oltp(self, trips, batch_size=1000):
        """Загрузка поездок в OLTP"""
        logger.info(f"Загрузка {len(trips)} поездок...")

        query = """
        INSERT INTO oltp.trips
        (user_id, route_id, vehicle_id, start_time, end_time, start_station_id,
         end_station_id, distance_km, status, created_at)
        VALUES (%(user_id)s, %(route_id)s, %(vehicle_id)s, %(start_time)s, %(end_time)s,
                %(start_station_id)s, %(end_station_id)s, %(distance_km)s, %(status)s, %(created_at)s)
        """

        cursor = self.conn.cursor()

        # Загружаем батчами
        for i in range(0, len(trips), batch_size):
            batch = trips[i:i+batch_size]
            execute_batch(cursor, query, batch)
            self.conn.commit()

            if (i + batch_size) % 10000 == 0:
                logger.info(f"  Загружено {i + batch_size} поездок...")

        cursor.close()
        logger.info("✓ Поездки загружены")

    def load_transactions_to_oltp(self, transactions, batch_size=1000):
        """Загрузка транзакций в OLTP"""
        logger.info(f"Загрузка {len(transactions)} транзакций...")

        query = """
        INSERT INTO oltp.transactions
        (user_id, trip_id, amount, payment_method, transaction_type, transaction_date,
         status, description, created_at)
        VALUES (%(user_id)s, %(trip_id)s, %(amount)s, %(payment_method)s, %(transaction_type)s,
                %(transaction_date)s, %(status)s, %(description)s, %(created_at)s)
        """

        cursor = self.conn.cursor()

        for i in range(0, len(transactions), batch_size):
            batch = transactions[i:i+batch_size]
            execute_batch(cursor, query, batch)
            self.conn.commit()

            if (i + batch_size) % 10000 == 0:
                logger.info(f"  Загружено {i + batch_size} транзакций...")

        cursor.close()
        logger.info("✓ Транзакции загружены")

    def copy_oltp_to_staging(self):
        """Копирование данных из OLTP в Staging"""
        logger.info("=== Копирование данных в Staging ===")

        tables = [
            ('oltp.users', 'staging.stg_users'),
            ('oltp.routes', 'staging.stg_routes'),
            ('oltp.stations', 'staging.stg_stations'),
            ('oltp.vehicles', 'staging.stg_vehicles'),
            ('oltp.trips', 'staging.stg_trips'),
            ('oltp.transactions', 'staging.stg_transactions'),
        ]

        cursor = self.conn.cursor()

        for oltp_table, stg_table in tables:
            logger.info(f"Копирование {oltp_table} -> {stg_table}...")

            # Получаем список колонок
            cursor.execute(f"""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_schema = 'oltp' AND table_name = '{oltp_table.split('.')[1]}'
                ORDER BY ordinal_position
            """)

            oltp_columns = [row[0] for row in cursor.fetchall()]

            # Получаем список колонок staging
            cursor.execute(f"""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_schema = 'staging' AND table_name = '{stg_table.split('.')[1]}'
                  AND column_name NOT IN ('load_date', 'source_system')
                ORDER BY ordinal_position
            """)

            stg_columns = [row[0] for row in cursor.fetchall()]

            # Находим общие колонки
            common_columns = [col for col in oltp_columns if col in stg_columns]
            columns_str = ', '.join(common_columns)

            # Копируем данные
            copy_query = f"""
                INSERT INTO {stg_table} ({columns_str})
                SELECT {columns_str} FROM {oltp_table}
            """

            cursor.execute(copy_query)
            self.conn.commit()

            # Получаем количество загруженных записей
            cursor.execute(f"SELECT COUNT(*) FROM {stg_table}")
            count = cursor.fetchone()[0]
            logger.info(f"✓ Скопировано {count} записей в {stg_table}")

        cursor.close()
        logger.info("✓ Копирование в Staging завершено")

    def load_all_data(self):
        """Загрузка всех данных"""
        logger.info("MetroPulse Data Loading Script           ")

        if not self.connect():
            return False

        try:
            # Генерация данных
            logger.info("\n=== Генерация тестовых данных ===")
            generator = DataGenerator()
            datasets = generator.generate_all()

            # Загрузка в OLTP
            logger.info("\n=== Загрузка данных в OLTP ===")
            self.load_stations_to_oltp(generator.stations)
            self.load_routes_to_oltp(generator.routes)
            self.load_route_stations_to_oltp(generator.route_stations)
            self.load_vehicles_to_oltp(generator.vehicles)
            self.load_users_to_oltp(generator.users)
            self.load_trips_to_oltp(generator.trips)
            self.load_transactions_to_oltp(generator.transactions)

            # Копирование в Staging
            logger.info("\n=== Копирование в Staging ===")
            self.copy_oltp_to_staging()

            logger.info("Загрузка данных завершена успешно!")

            return True

        except Exception as e:
            logger.error(f"Ошибка загрузки данных: {str(e)}", exc_info=True)
            self.conn.rollback()
            return False

        finally:
            if self.conn:
                self.conn.close()


def main():
    import argparse

    parser = argparse.ArgumentParser(description='Load test data into MetroPulse OLTP and Staging')
    parser.add_argument('--host', default='localhost', help='PostgreSQL host')
    parser.add_argument('--port', type=int, default=5432, help='PostgreSQL port')
    parser.add_argument('--database', default='metropulse', help='Database name')
    parser.add_argument('--user', default='postgres', help='PostgreSQL user')
    parser.add_argument('--password', default='postgres', help='PostgreSQL password')

    args = parser.parse_args()

    config = {
        'host': args.host,
        'port': args.port,
        'database': args.database,
        'user': args.user,
        'password': args.password
    }

    loader = DataLoader(config)
    success = loader.load_all_data()

    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()

