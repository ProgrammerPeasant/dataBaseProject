import random
import string
from datetime import datetime, timedelta
from faker import Faker
import pandas as pd
import numpy as np

# Инициализация
fake = Faker('ru_RU')
random.seed(42)
np.random.seed(42)

# Константы
NUM_USERS = 10000
NUM_ROUTES = 50
NUM_STATIONS = 200
NUM_VEHICLES = 300
NUM_DAYS = 30
BASE_DATE = datetime(2025, 11, 12)

# Списки для справочников
TRANSPORT_TYPES = ['bus', 'tram', 'metro']
STATION_TYPES = ['bus_stop', 'tram_stop', 'metro_station']
DISTRICTS = ['Центральный', 'Северный', 'Южный', 'Восточный', 'Западный',
             'Ленинский', 'Советский', 'Октябрьский', 'Железнодорожный']
PAYMENT_METHODS = ['card', 'cash', 'subscription', 'balance']
TRIP_STATUSES = ['completed', 'cancelled', 'in_progress']
TRANSACTION_TYPES = ['payment', 'refund', 'topup']


class DataGenerator:
    """Генератор тестовых данных для MetroPulse"""

    def __init__(self):
        self.users = []
        self.routes = []
        self.stations = []
        self.vehicles = []
        self.route_stations = []
        self.trips = []
        self.transactions = []
        self.subscriptions = []

    def generate_stations(self, num_stations=NUM_STATIONS):
        """Генерация станций и остановок"""
        print(f"Генерация {num_stations} станций...")

        # Координаты Москвы (примерные границы)
        lat_min, lat_max = 55.55, 55.95
        lon_min, lon_max = 37.35, 37.85

        stations = []
        for i in range(num_stations):
            station_type = random.choice(STATION_TYPES)

            station = {
                'station_id': f'ST{i+1:04d}',
                'station_name': self._generate_station_name(station_type, i),
                'latitude': round(random.uniform(lat_min, lat_max), 8),
                'longitude': round(random.uniform(lon_min, lon_max), 8),
                'station_type': station_type,
                'district': random.choice(DISTRICTS),
                'zone': random.choice(['A', 'B', 'C']),
                'is_active': True,
                'created_at': BASE_DATE - timedelta(days=random.randint(365, 1095))
            }
            stations.append(station)

        self.stations = stations
        return pd.DataFrame(stations)

    def _generate_station_name(self, station_type, index):
        """Генерация названия станции"""
        if station_type == 'metro_station':
            prefixes = ['ст. м.', 'Станция метро']
            names = ['Площадь Революции', 'Комсомольская', 'Киевская', 'Парк Культуры',
                    'Маяковская', 'Театральная', 'Курская', 'Таганская', 'Новослободская',
                    'Проспект Мира', 'Красные Ворота', 'Сокольники', 'Преображенская']
            return f"{random.choice(prefixes)} {random.choice(names)}-{index}"
        elif station_type == 'tram_stop':
            return f"Трамвайная остановка {fake.street_name()}"
        else:
            return f"Остановка {fake.street_name()}"

    def generate_routes(self, num_routes=NUM_ROUTES):
        """Генерация маршрутов"""
        print(f"Генерация {num_routes} маршрутов...")

        routes = []
        for i in range(num_routes):
            transport_type = random.choice(TRANSPORT_TYPES)
            route_number = self._generate_route_number(transport_type, i)

            route = {
                'route_id': f'R{i+1:03d}',
                'route_number': route_number,
                'route_name': self._generate_route_name(transport_type, route_number),
                'transport_type': transport_type,
                'base_fare': round(random.uniform(40, 80), 2),
                'is_active': random.random() > 0.1,  # 90% активных
                'description': fake.text(max_nb_chars=100),
                'created_at': BASE_DATE - timedelta(days=random.randint(365, 1825))
            }
            routes.append(route)

        self.routes = routes
        return pd.DataFrame(routes)

    def _generate_route_number(self, transport_type, index):
        """Генерация номера маршрута"""
        if transport_type == 'bus':
            return str(random.randint(1, 999))
        elif transport_type == 'tram':
            return str(random.randint(1, 50))
        else:  # metro
            return random.choice(['1', '2', '3', '4', '5', '6', '7', '8', '9'])

    def _generate_route_name(self, transport_type, route_number):
        """Генерация названия маршрута"""
        prefixes = {
            'bus': f'Автобус №{route_number}',
            'tram': f'Трамвай №{route_number}',
            'metro': f'Метро линия {route_number}'
        }
        return prefixes.get(transport_type, f'Маршрут {route_number}')

    def generate_route_stations(self):
        """Генерация связей маршрутов и станций"""
        print("Генерация связей маршрутов и станций...")

        route_stations = []
        for route in self.routes:
            route_id = route['route_id']
            transport_type = route['transport_type']

            # Фильтруем станции по типу транспорта
            compatible_stations = [
                s for s in self.stations
                if s['station_type'].replace('_stop', '').replace('_station', '') == transport_type
            ]

            if not compatible_stations:
                compatible_stations = self.stations

            # Количество остановок на маршруте
            num_stops = random.randint(10, 30)
            selected_stations = random.sample(
                compatible_stations,
                min(num_stops, len(compatible_stations))
            )

            for seq, station in enumerate(selected_stations, 1):
                route_station = {
                    'route_id': route_id,
                    'station_id': station['station_id'],
                    'sequence_number': seq,
                    'estimated_travel_time_minutes': random.randint(2, 10),
                    'distance_from_previous_km': round(random.uniform(0.5, 3.0), 2)
                }
                route_stations.append(route_station)

        self.route_stations = route_stations
        return pd.DataFrame(route_stations)

    def generate_vehicles(self, num_vehicles=NUM_VEHICLES):
        """Генерация транспортных средств"""
        print(f"Генерация {num_vehicles} транспортных средств...")

        vehicles = []
        active_routes = [r['route_id'] for r in self.routes if r['is_active']]

        for i in range(num_vehicles):
            transport_type = random.choice(TRANSPORT_TYPES)

            vehicle = {
                'vehicle_id': f'{transport_type.upper()}_{i+1:04d}',
                'vehicle_type': transport_type,
                'route_id': random.choice(active_routes) if active_routes else None,
                'capacity': self._get_vehicle_capacity(transport_type),
                'manufacture_year': random.randint(2010, 2025),
                'model': self._get_vehicle_model(transport_type),
                'license_plate': self._generate_license_plate(),
                'last_maintenance_date': BASE_DATE - timedelta(days=random.randint(1, 180)),
                'status': random.choices(['active', 'maintenance', 'retired'],
                                       weights=[0.85, 0.10, 0.05])[0],
                'created_at': BASE_DATE - timedelta(days=random.randint(365, 2555))
            }
            vehicles.append(vehicle)

        self.vehicles = vehicles
        return pd.DataFrame(vehicles)

    def _get_vehicle_capacity(self, transport_type):
        """Получить типичную вместимость транспорта"""
        capacities = {
            'bus': random.randint(40, 80),
            'tram': random.randint(100, 200),
            'metro': random.randint(200, 400)
        }
        return capacities.get(transport_type, 50)

    def _get_vehicle_model(self, transport_type):
        """Получить модель транспорта"""
        models = {
            'bus': ['ЛиАЗ-5292', 'МАЗ-203', 'НефАЗ-5299', 'ПАЗ-3237'],
            'tram': ['71-623', '71-631', 'PESA 71-414', 'Витязь-М'],
            'metro': ['81-760/761', '81-765/766', '81-740/741']
        }
        return random.choice(models.get(transport_type, ['Неизвестно']))

    def _generate_license_plate(self):
        """Генерация номерного знака"""
        letters = ''.join(random.choices('АВЕКМНОРСТУХ', k=3))
        numbers = ''.join(random.choices(string.digits, k=3))
        region = random.randint(1, 199)
        return f'{letters[0]}{numbers}{letters[1:]}{region:02d}'

    def generate_users(self, num_users=NUM_USERS):
        """Генерация пользователей"""
        print(f"Генерация {num_users} пользователей...")

        users = []
        for i in range(num_users):
            registration_date = BASE_DATE - timedelta(days=random.randint(1, 1095))

            user = {
                'user_id': i + 1,
                'username': fake.user_name() + str(random.randint(1, 999)),
                'email': fake.email(),
                'phone': fake.phone_number(),
                'password_hash': fake.sha256(),
                'first_name': fake.first_name(),
                'last_name': fake.last_name(),
                'date_of_birth': fake.date_of_birth(minimum_age=18, maximum_age=80),
                'registration_date': registration_date,
                'status': random.choices(['active', 'suspended', 'deleted'],
                                        weights=[0.90, 0.05, 0.05])[0],
                'balance': round(random.uniform(0, 1000), 2),
                'created_at': registration_date
            }
            users.append(user)

        self.users = users
        return pd.DataFrame(users)

    def generate_trips(self, num_days=NUM_DAYS):
        """Генерация поездок"""
        print(f"Генерация поездок за {num_days} дней...")

        trips = []
        trip_id = 1

        active_users = [u for u in self.users if u['status'] == 'active']
        active_routes = [r for r in self.routes if r['is_active']]
        active_vehicles = [v for v in self.vehicles if v['status'] == 'active']

        for day in range(num_days):
            current_date = BASE_DATE - timedelta(days=num_days - day - 1)

            # Количество поездок в день зависит от дня недели
            is_weekend = current_date.weekday() >= 5
            trips_per_day = random.randint(10000, 15000) if not is_weekend else random.randint(5000, 8000)

            # Распределение поездок по времени суток
            for _ in range(trips_per_day):
                user = random.choice(active_users)
                route = random.choice(active_routes)
                vehicle = random.choice([v for v in active_vehicles if v['route_id'] == route['route_id']])

                # Генерация времени начала поездки (с пиками)
                hour = self._generate_trip_hour(is_weekend)
                minute = random.randint(0, 59)
                start_time = current_date.replace(hour=hour, minute=minute, second=random.randint(0, 59))

                # Длительность поездки
                duration_minutes = random.randint(10, 90)
                end_time = start_time + timedelta(minutes=duration_minutes)

                # Получаем станции маршрута
                route_stops = [rs for rs in self.route_stations if rs['route_id'] == route['route_id']]
                if len(route_stops) >= 2:
                    start_stop = random.choice(route_stops)
                    end_stop = random.choice([s for s in route_stops if s['sequence_number'] > start_stop['sequence_number']])
                    start_station_id = start_stop['station_id']
                    end_station_id = end_stop['station_id']
                else:
                    start_station_id = None
                    end_station_id = None

                trip = {
                    'trip_id': trip_id,
                    'user_id': user['user_id'],
                    'route_id': route['route_id'],
                    'vehicle_id': vehicle['vehicle_id'] if vehicle else None,
                    'start_time': start_time,
                    'end_time': end_time if random.random() > 0.05 else None,  # 5% незавершенных
                    'start_station_id': start_station_id,
                    'end_station_id': end_station_id,
                    'distance_km': round(random.uniform(2, 25), 2),
                    'status': random.choices(TRIP_STATUSES, weights=[0.90, 0.05, 0.05])[0],
                    'created_at': start_time
                }
                trips.append(trip)
                trip_id += 1

        self.trips = trips
        print(f"Сгенерировано {len(trips)} поездок")
        return pd.DataFrame(trips)

    def _generate_trip_hour(self, is_weekend):
        """Генерация часа поездки с учетом пиков"""
        if is_weekend:
            return random.choices(range(24), weights=[
                1, 1, 1, 1, 1, 2, 4, 6, 8, 10,  # 0-9
                12, 14, 16, 18, 18, 16, 14, 12, 10, 8,  # 10-19
                6, 4, 3, 2  # 20-23
            ])[0]
        else:
            # Будние дни - пики утром (7-9) и вечером (17-19)
            return random.choices(range(24), weights=[
                1, 1, 1, 1, 1, 3, 8, 20, 22, 10,  # 0-9 (утренний пик)
                6, 6, 8, 8, 8, 8, 8, 18, 22, 20,  # 10-19 (вечерний пик)
                10, 6, 3, 2  # 20-23
            ])[0]

    def generate_transactions(self):
        """Генерация транзакций"""
        print("Генерация транзакций...")

        transactions = []
        transaction_id = 1

        for trip in self.trips:
            if trip['status'] == 'completed':
                # Находим маршрут для получения стоимости
                route = next((r for r in self.routes if r['route_id'] == trip['route_id']), None)
                if not route:
                    continue

                amount = route['base_fare']

                transaction = {
                    'transaction_id': transaction_id,
                    'user_id': trip['user_id'],
                    'trip_id': trip['trip_id'],
                    'amount': amount,
                    'payment_method': random.choice(PAYMENT_METHODS),
                    'transaction_type': 'payment',
                    'transaction_date': trip['start_time'],
                    'status': random.choices(['success', 'failed'], weights=[0.95, 0.05])[0],
                    'description': f"Оплата поездки #{trip['trip_id']}",
                    'created_at': trip['start_time']
                }
                transactions.append(transaction)
                transaction_id += 1

        # Добавляем пополнения баланса
        active_users = [u for u in self.users if u['status'] == 'active']
        for _ in range(len(active_users) // 10):  # 10% пользователей пополняют баланс
            user = random.choice(active_users)
            topup_date = BASE_DATE - timedelta(days=random.randint(1, NUM_DAYS))

            transaction = {
                'transaction_id': transaction_id,
                'user_id': user['user_id'],
                'trip_id': None,
                'amount': round(random.choice([100, 200, 500, 1000, 2000]), 2),
                'payment_method': 'card',
                'transaction_type': 'topup',
                'transaction_date': topup_date,
                'status': 'success',
                'description': 'Пополнение баланса',
                'created_at': topup_date
            }
            transactions.append(transaction)
            transaction_id += 1

        self.transactions = transactions
        print(f"Сгенерировано {len(transactions)} транзакций")
        return pd.DataFrame(transactions)

    def generate_all(self):
        """Генерация всех данных"""
        print("=== Генерация всех данных для MetroPulse ===\n")

        df_stations = self.generate_stations()
        df_routes = self.generate_routes()
        df_route_stations = self.generate_route_stations()
        df_vehicles = self.generate_vehicles()
        df_users = self.generate_users()
        df_trips = self.generate_trips()
        df_transactions = self.generate_transactions()

        datasets = {
            'stations': df_stations,
            'routes': df_routes,
            'route_stations': df_route_stations,
            'vehicles': df_vehicles,
            'users': df_users,
            'trips': df_trips,
            'transactions': df_transactions
        }

        print("\n=== Статистика сгенерированных данных ===")
        for name, df in datasets.items():
            print(f"{name}: {len(df)} записей")

        return datasets


if __name__ == '__main__':
    generator = DataGenerator()
    datasets = generator.generate_all()

    # Сохранение в CSV для проверки
    import os
    output_dir = 'generated_data'
    os.makedirs(output_dir, exist_ok=True)

    print(f"\n=== Сохранение данных в {output_dir} ===")
    for name, df in datasets.items():
        filepath = os.path.join(output_dir, f'{name}.csv')
        df.to_csv(filepath, index=False)
        print(f"Сохранено: {filepath}")

    print("\n✅ Генерация данных завершена!")

