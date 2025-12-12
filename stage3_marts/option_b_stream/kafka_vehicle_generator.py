import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaError
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class VehiclePositionGenerator:
    """Генератор позиций транспорта для Kafka"""

    # Координаты Москвы (примерные границы)
    LAT_MIN, LAT_MAX = 55.55, 55.95
    LON_MIN, LON_MAX = 37.35, 37.85

    OCCUPANCY_LEVELS = [
        'empty',
        'many_seats_available',
        'few_seats_available',
        'standing_room_only',
        'crushed_standing_room_only',
        'full'
    ]

    def __init__(self, bootstrap_servers, topic='vehicle_positions'):
        """
        Инициализация генератора

        Args:
            bootstrap_servers: Kafka bootstrap servers
            topic: Kafka topic для отправки событий
        """
        self.topic = topic
        self.vehicles = self._generate_vehicles()

        # Инициализация Kafka producer
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            acks='all',
            retries=3
        )

        logger.info(f"Kafka Producer создан. Topic: {topic}")
        logger.info(f"Сгенерировано {len(self.vehicles)} транспортных средств")

    def _generate_vehicles(self, count=300):
        """Генерация транспортных средств с начальными позициями"""
        vehicles = []

        for i in range(count):
            vehicle_type = random.choice(['bus', 'tram', 'metro'])

            vehicle = {
                'vehicle_id': f'{vehicle_type.upper()}_{i+1:04d}',
                'vehicle_type': vehicle_type,
                'route_id': f'R{random.randint(1, 50):03d}',
                'latitude': random.uniform(self.LAT_MIN, self.LAT_MAX),
                'longitude': random.uniform(self.LON_MIN, self.LON_MAX),
                'speed': random.uniform(0, 60),  # км/ч
                'bearing': random.randint(0, 360),
                'occupancy': random.choice(self.OCCUPANCY_LEVELS)
            }
            vehicles.append(vehicle)

        return vehicles

    def _update_vehicle_position(self, vehicle):
        """Обновление позиции транспорта (симуляция движения)"""
        # Скорость движения зависит от типа транспорта
        speed_range = {
            'bus': (10, 50),
            'tram': (15, 40),
            'metro': (40, 80)
        }

        min_speed, max_speed = speed_range.get(vehicle['vehicle_type'], (10, 50))

        # Обновляем скорость (с небольшими изменениями)
        speed_change = random.uniform(-5, 5)
        vehicle['speed'] = max(0, min(max_speed, vehicle['speed'] + speed_change))

        # Обновляем позицию (примерное движение)
        # Скорость в км/ч в градусы за 10 секунд
        # 1 км ≈ 0.009 градусов на экваторе (примерно)
        distance_per_10s = (vehicle['speed'] / 3600) * 10  # км за 10 секунд
        delta_degrees = distance_per_10s * 0.009

        # Изменение координат в зависимости от направления (bearing)
        bearing_rad = vehicle['bearing'] * (3.14159 / 180)
        vehicle['latitude'] += delta_degrees * abs(random.uniform(-1, 1))
        vehicle['longitude'] += delta_degrees * abs(random.uniform(-1, 1))

        # Ограничиваем координаты границами города
        vehicle['latitude'] = max(self.LAT_MIN, min(self.LAT_MAX, vehicle['latitude']))
        vehicle['longitude'] = max(self.LON_MIN, min(self.LON_MAX, vehicle['longitude']))

        # Иногда меняем направление
        if random.random() < 0.1:
            vehicle['bearing'] = (vehicle['bearing'] + random.randint(-45, 45)) % 360

        # Изменяем загруженность в зависимости от времени суток
        hour = datetime.now().hour
        if 7 <= hour <= 9 or 17 <= hour <= 19:
            # Часы пик - больше загруженность
            vehicle['occupancy'] = random.choices(
                self.OCCUPANCY_LEVELS,
                weights=[5, 10, 20, 30, 25, 10]
            )[0]
        else:
            # Обычное время
            vehicle['occupancy'] = random.choices(
                self.OCCUPANCY_LEVELS,
                weights=[15, 25, 30, 20, 8, 2]
            )[0]

    def generate_event(self, vehicle):
        """Генерация события о позиции транспорта"""
        event = {
            'vehicle_id': vehicle['vehicle_id'],
            'route_id': vehicle['route_id'],
            'latitude': round(vehicle['latitude'], 8),
            'longitude': round(vehicle['longitude'], 8),
            'speed': round(vehicle['speed'], 2),
            'bearing': vehicle['bearing'],
            'occupancy': vehicle['occupancy'],
            'timestamp': datetime.utcnow().isoformat() + 'Z',
            'vehicle_type': vehicle['vehicle_type']
        }
        return event

    def send_event(self, event):
        """Отправка события в Kafka"""
        try:
            future = self.producer.send(
                self.topic,
                key=event['vehicle_id'],
                value=event
            )

            # Ждем подтверждения
            record_metadata = future.get(timeout=10)

            logger.debug(f"Event sent: {event['vehicle_id']} -> "
                        f"partition {record_metadata.partition}, "
                        f"offset {record_metadata.offset}")

            return True

        except KafkaError as e:
            logger.error(f"Kafka error: {str(e)}")
            return False
        except Exception as e:
            logger.error(f"Error sending event: {str(e)}")
            return False

    def run(self, interval_seconds=10, duration_seconds=None):
        """
        Запуск генератора событий

        Args:
            interval_seconds: Интервал между отправкой событий (сек)
            duration_seconds: Длительность работы (сек), None = бесконечно
        """
        logger.info(f"Запуск генератора событий (интервал: {interval_seconds}s)")

        start_time = time.time()
        events_sent = 0

        try:
            while True:
                # Проверка времени работы
                if duration_seconds and (time.time() - start_time) > duration_seconds:
                    logger.info(f"Время работы истекло. Остановка генератора.")
                    break

                batch_start = time.time()

                # Генерация и отправка событий для всех транспортных средств
                for vehicle in self.vehicles:
                    self._update_vehicle_position(vehicle)
                    event = self.generate_event(vehicle)

                    if self.send_event(event):
                        events_sent += 1

                batch_duration = time.time() - batch_start

                # Логирование статистики
                if events_sent % 1000 == 0:
                    rate = events_sent / (time.time() - start_time)
                    logger.info(f"Events sent: {events_sent}, Rate: {rate:.2f} events/sec")

                # Ждем до следующего интервала
                sleep_time = max(0, interval_seconds - batch_duration)
                if sleep_time > 0:
                    time.sleep(sleep_time)

        except KeyboardInterrupt:
            logger.info("Получен сигнал остановки (Ctrl+C)")

        finally:
            logger.info(f"Всего отправлено событий: {events_sent}")
            self.producer.flush()
            self.producer.close()
            logger.info("Генератор остановлен")

    def run_continuous(self, interval_seconds=10):
        """Запуск генератора в непрерывном режиме"""
        self.run(interval_seconds=interval_seconds, duration_seconds=None)


def main():
    """Точка входа"""
    import argparse

    parser = argparse.ArgumentParser(description='Vehicle Position Event Generator for Kafka')
    parser.add_argument('--bootstrap-servers', default='localhost:9092',
                       help='Kafka bootstrap servers (default: localhost:9092)')
    parser.add_argument('--topic', default='vehicle_positions',
                       help='Kafka topic (default: vehicle_positions)')
    parser.add_argument('--interval', type=int, default=10,
                       help='Interval between events in seconds (default: 10)')
    parser.add_argument('--duration', type=int, default=None,
                       help='Duration in seconds (default: None = infinite)')
    parser.add_argument('--num-vehicles', type=int, default=300,
                       help='Number of vehicles to simulate (default: 300)')

    args = parser.parse_args()

    logger.info("=== MetroPulse Vehicle Position Generator ===")
    logger.info(f"Bootstrap Servers: {args.bootstrap_servers}")
    logger.info(f"Topic: {args.topic}")
    logger.info(f"Interval: {args.interval}s")
    logger.info(f"Number of Vehicles: {args.num_vehicles}")

    generator = VehiclePositionGenerator(
        bootstrap_servers=args.bootstrap_servers.split(','),
        topic=args.topic
    )

    generator.run(
        interval_seconds=args.interval,
        duration_seconds=args.duration
    )


if __name__ == '__main__':
    main()

