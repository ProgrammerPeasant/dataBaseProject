import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import logging
import os
import sys

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DWHInitializer:
    """Инициализация DWH"""

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
            self.conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            logger.info("Подключение успешно установлено")
            return True
        except Exception as e:
            logger.error(f"Ошибка подключения: {str(e)}")
            return False

    def execute_sql_file(self, filepath):
        """Выполнение SQL скрипта из файла"""
        logger.info(f"Выполнение SQL файла: {filepath}")

        if not os.path.exists(filepath):
            logger.error(f"Файл не найден: {filepath}")
            return False

        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                sql = f.read()

            cursor = self.conn.cursor()
            cursor.execute(sql)
            cursor.close()

            logger.info(f"✓ Файл выполнен успешно: {filepath}")
            return True

        except Exception as e:
            logger.error(f"✗ Ошибка выполнения файла {filepath}: {str(e)}")
            return False

    def initialize_oltp(self):
        """Инициализация OLTP схемы"""
        logger.info("=== Инициализация OLTP схемы ===")

        sql_file = 'stage1_dwh_design/oltp_schema.sql'
        return self.execute_sql_file(sql_file)

    def initialize_staging(self):
        """Инициализация Staging слоя"""
        logger.info("=== Инициализация Staging слоя ===")

        sql_file = 'stage1_dwh_design/ddl/staging/01_staging_tables.sql'
        return self.execute_sql_file(sql_file)

    def initialize_dwh_dimensions(self):
        """Инициализация измерений DWH"""
        logger.info("=== Инициализация измерений DWH ===")

        sql_file = 'stage1_dwh_design/ddl/core/01_dimensions.sql'
        return self.execute_sql_file(sql_file)

    def initialize_dwh_facts(self):
        """Инициализация фактов DWH"""
        logger.info("=== Инициализация фактов DWH ===")

        sql_file = 'stage1_dwh_design/ddl/core/02_facts.sql'
        return self.execute_sql_file(sql_file)

    def populate_calendar(self):
        """Заполнение календарных измерений"""
        logger.info("=== Заполнение календарных измерений ===")

        sql_file = 'stage1_dwh_design/ddl/core/03_populate_calendar.sql'
        return self.execute_sql_file(sql_file)

    def verify_initialization(self):
        """Проверка успешности инициализации"""
        logger.info("=== Проверка инициализации ===")

        checks = [
            ("OLTP схема", "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'oltp'"),
            ("Staging схема", "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'staging'"),
            ("DWH схема", "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'dwh'"),
            ("Календарные данные (dim_date)", "SELECT COUNT(*) FROM dwh.dim_date"),
            ("Временные данные (dim_time)", "SELECT COUNT(*) FROM dwh.dim_time"),
        ]

        cursor = self.conn.cursor()
        all_passed = True

        for check_name, query in checks:
            try:
                cursor.execute(query)
                result = cursor.fetchone()[0]
                logger.info(f"✓ {check_name}: {result} записей")
            except Exception as e:
                logger.error(f"✗ {check_name}: {str(e)}")
                all_passed = False

        cursor.close()
        return all_passed

    def run_full_initialization(self):
        """Запуск полной инициализации DWH"""
        logger.info("╔════════════════════════════════════════════╗")
        logger.info("║  MetroPulse DWH Initialization Script     ║")
        logger.info("╚════════════════════════════════════════════╝")

        if not self.connect():
            logger.error("Не удалось подключиться к базе данных")
            return False

        steps = [
            ("OLTP Schema", self.initialize_oltp),
            ("Staging Layer", self.initialize_staging),
            ("DWH Dimensions", self.initialize_dwh_dimensions),
            ("DWH Facts", self.initialize_dwh_facts),
            ("Calendar Population", self.populate_calendar),
        ]

        for step_name, step_func in steps:
            logger.info(f"\n>>> {step_name}")
            if not step_func():
                logger.error(f"Ошибка на этапе: {step_name}")
                return False

        logger.info("\n=== Проверка результатов ===")
        success = self.verify_initialization()

        if success:
            logger.info("\n╔════════════════════════════════════════════╗")
            logger.info("║  ✓ Инициализация завершена успешно!       ║")
            logger.info("╚════════════════════════════════════════════╝")
        else:
            logger.error("\n╔════════════════════════════════════════════╗")
            logger.error("║  ✗ Инициализация завершена с ошибками     ║")
            logger.error("╚════════════════════════════════════════════╝")

        return success

    def close(self):
        """Закрытие подключения"""
        if self.conn:
            self.conn.close()
            logger.info("Подключение закрыто")


def main():
    """Точка входа"""
    import argparse

    parser = argparse.ArgumentParser(description='Initialize MetroPulse DWH')
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

    initializer = DWHInitializer(config)

    try:
        success = initializer.run_full_initialization()
        sys.exit(0 if success else 1)
    finally:
        initializer.close()


if __name__ == '__main__':
    main()

