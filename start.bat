@echo off
chcp 65001 > nul
echo ╔════════════════════════════════════════════════════════════╗
echo ║         MetroPulse - Аналитическая платформа              ║
echo ║              Скрипт быстрого запуска                       ║
echo ╚════════════════════════════════════════════════════════════╝
echo.

REM Проверка наличия Docker
docker --version >nul 2>&1
if errorlevel 1 (
    echo [ERROR] Docker не установлен или не запущен!
    echo Установите Docker Desktop и запустите его.
    pause
    exit /b 1
)

echo [1/6] Запуск Docker контейнеров...
docker-compose up -d

echo.
echo [2/6] Ожидание запуска PostgreSQL...
timeout /t 15 /nobreak > nul

echo.
echo [3/6] Установка Python зависимостей...
pip install -r requirements.txt

echo.
echo [4/6] Инициализация DWH схем...
python scripts\init_dwh.py

echo.
echo [5/6] Загрузка тестовых данных...
python scripts\load_test_data.py

echo.
echo [6/6] Запуск ETL пайплайна...
REM python stage2_batch_pipeline\spark_jobs\etl_pipeline.py

echo.
echo ╔════════════════════════════════════════════════════════════╗
echo ║                   Система запущена!                        ║
echo ╚════════════════════════════════════════════════════════════╝
echo.
echo Доступные сервисы:
echo   PostgreSQL:  localhost:5432
echo   MinIO:       http://localhost:9001 (admin/admin)
echo   Kafka:       localhost:9092
echo   Spark UI:    http://localhost:8080
echo   ClickHouse:  http://localhost:8123
echo   Jupyter:     http://localhost:8888
echo   Redis:       localhost:6379
echo.
echo Для остановки всех сервисов выполните: docker-compose down
echo.
pause

