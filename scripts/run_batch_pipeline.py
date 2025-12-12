"""
MetroPulse - Запуск полного batch pipeline
Скрипт для последовательного выполнения всех этапов ETL
"""

import sys
import os
import logging
from datetime import datetime

# Добавляем путь к модулям проекта
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def run_batch_pipeline():
    """Запуск полного batch pipeline"""

    logger.info("╔════════════════════════════════════════════════════════════╗")
    logger.info("║         MetroPulse Batch Pipeline Runner                  ║")
    logger.info("╚════════════════════════════════════════════════════════════╝")
    logger.info(f"Время запуска: {datetime.now()}")

    try:
        # Этап 1: Загрузка тестовых данных (если еще не загружены)
        logger.info("\n" + "="*60)
        logger.info("ЭТАП 1: Проверка и загрузка тестовых данных")
        logger.info("="*60)

        from load_test_data import DataLoader

        config = {
            'host': os.getenv('PG_HOST', 'localhost'),
            'port': int(os.getenv('PG_PORT', 5432)),
            'database': os.getenv('PG_DATABASE', 'metropulse'),
            'user': os.getenv('PG_USER', 'postgres'),
            'password': os.getenv('PG_PASSWORD', 'postgres')
        }

        loader = DataLoader(config)
        if not loader.connect():
            logger.error("Не удалось подключиться к базе данных")
            return False

        # Проверяем, есть ли данные в OLTP
        import psycopg2
        conn = psycopg2.connect(**config)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM oltp.users")
        user_count = cursor.fetchone()[0]
        cursor.close()
        conn.close()

        if user_count == 0:
            logger.info("Данные в OLTP отсутствуют. Запускаем загрузку...")
            success = loader.load_all_data()
            if not success:
                logger.error("Ошибка загрузки тестовых данных")
                return False
        else:
            logger.info(f"✓ Данные уже загружены в OLTP ({user_count} пользователей)")

        # Этап 2: Запуск Spark ETL
        logger.info("\n" + "="*60)
        logger.info("ЭТАП 2: Запуск Spark ETL Pipeline")
        logger.info("="*60)

        # Импортируем и запускаем ETL
        sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'stage2_batch_pipeline', 'spark_jobs'))

        try:
            from etl_pipeline import MetroPulseETL

            etl_config = {
                'pg_jdbc_url': f"jdbc:postgresql://{config['host']}:{config['port']}/{config['database']}",
                'pg_user': config['user'],
                'pg_password': config['password'],
                'minio_endpoint': os.getenv('MINIO_ENDPOINT', 'http://localhost:9002'),
                'minio_access_key': os.getenv('MINIO_ACCESS_KEY', 'minioadmin'),
                'minio_secret_key': os.getenv('MINIO_SECRET_KEY', 'minioadmin')
            }

            logger.info("Инициализация Spark ETL...")
            etl = MetroPulseETL(etl_config)

            logger.info("Запуск полного ETL пайплайна...")
            etl.run_full_pipeline()

            logger.info("✓ Spark ETL завершен успешно")

        except ImportError as e:
            logger.warning(f"Не удалось импортировать Spark ETL модуль: {e}")
            logger.warning("PySpark может быть не установлен. Установите: pip install pyspark")
            logger.info("Пропускаем этап Spark ETL...")
        except Exception as e:
            logger.error(f"Ошибка в Spark ETL: {e}")
            logger.warning("Продолжаем выполнение...")

        # Этап 3: Проверка результатов
        logger.info("\n" + "="*60)
        logger.info("ЭТАП 3: Проверка результатов")
        logger.info("="*60)

        conn = psycopg2.connect(**config)
        cursor = conn.cursor()

        checks = [
            ("OLTP Users", "SELECT COUNT(*) FROM oltp.users"),
            ("OLTP Trips", "SELECT COUNT(*) FROM oltp.trips"),
            ("OLTP Transactions", "SELECT COUNT(*) FROM oltp.transactions"),
            ("Staging Users", "SELECT COUNT(*) FROM staging.stg_users"),
            ("Staging Trips", "SELECT COUNT(*) FROM staging.stg_trips"),
            ("DWH Users", "SELECT COUNT(*) FROM dwh.dim_user"),
            ("DWH Routes", "SELECT COUNT(*) FROM dwh.dim_route"),
            ("DWH Trips", "SELECT COUNT(*) FROM dwh.fact_trips"),
            ("DWH Transactions", "SELECT COUNT(*) FROM dwh.fact_transactions"),
        ]

        logger.info("\nСтатистика данных:")
        logger.info("-" * 60)

        for check_name, query in checks:
            try:
                cursor.execute(query)
                count = cursor.fetchone()[0]
                logger.info(f"{check_name:.<30} {count:>10,} записей")
            except Exception as e:
                logger.warning(f"{check_name:.<30} {'ERROR':>10}")

        cursor.close()
        conn.close()

        logger.info("\n╔════════════════════════════════════════════════════════════╗")
        logger.info("║          Batch Pipeline завершен успешно!                  ║")
        logger.info("╚════════════════════════════════════════════════════════════╝")

        logger.info("\nСледующие шаги:")
        logger.info("  1. Проверьте данные в DWH:")
        logger.info("     docker exec -it metropulse-postgres psql -U postgres -d metropulse")
        logger.info("  2. Запустите построение витрин ClickHouse:")
        logger.info("     python stage3_marts/option_a_batch/spark_to_clickhouse.py")
        logger.info("  3. Запустите генератор Kafka событий:")
        logger.info("     python stage3_marts/option_b_stream/kafka_vehicle_generator.py")

        return True

    except KeyboardInterrupt:
        logger.info("\n\nПолучен сигнал остановки (Ctrl+C)")
        return False
    except Exception as e:
        logger.error(f"\n\nКритическая ошибка: {e}", exc_info=True)
        return False


def main():
    """Точка входа"""
    import argparse

    parser = argparse.ArgumentParser(description='Run MetroPulse Batch Pipeline')
    parser.add_argument('--host', default='localhost', help='PostgreSQL host')
    parser.add_argument('--port', type=int, default=5432, help='PostgreSQL port')
    parser.add_argument('--database', default='metropulse', help='Database name')
    parser.add_argument('--user', default='postgres', help='PostgreSQL user')
    parser.add_argument('--password', default='postgres', help='PostgreSQL password')

    args = parser.parse_args()

    # Устанавливаем переменные окружения
    os.environ['PG_HOST'] = args.host
    os.environ['PG_PORT'] = str(args.port)
    os.environ['PG_DATABASE'] = args.database
    os.environ['PG_USER'] = args.user
    os.environ['PG_PASSWORD'] = args.password

    success = run_batch_pipeline()
    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()

