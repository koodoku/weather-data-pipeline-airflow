# weather-data-pipeline-airflow

Airflow-пайплайн, который раз в час собирает данные о погоде с OpenWeatherMap,
валидирует их, отсеивает дубликаты и сохраняет в Postgres. Учебный проект:
демонстрирует branch/skip-операторы, обработку ошибок и хранение credentials
через Airflow Variables/Connections.

## Стек

- **Apache Airflow 3.3.0** (CeleryExecutor) — Docker Compose
- **Postgres** — метабаза Airflow + отдельная БД для результатов (`postgres-weather`)
- **Redis** — брокер для Celery
- **Adminer** — просмотр сохранённых данных
- **OpenWeatherMap API** — источник данных

## Архитектура DAG (`weather_data_pipeline`)

```
create_table
     │
check_api_availability
     │
branch_on_availability ──(недоступен)──► notify_api_unavailable ──┐
     │ (доступен)                                                  │
fetch_weather_data                                                 │
     │                                                              │
validate_data                                                      │
     │                                                              │
check_has_valid_data (ShortCircuit) ──(невалидных нет)─────────────┤
     │ (есть валидные)                                             │
filter_duplicates                                                  │
     │                                                              │
check_has_new_data (ShortCircuit) ──(всё дубликаты)────────────────┤
     │ (есть новые)                                                │
save_data                                                           │
     │                                                              │
     └──────────────────────────► log_summary ◄───────────────────┘
```

| Требование | Реализация |
|---|---|
| Окружение + сервисы Airflow | `docker-compose.yaml` (официальный, Airflow 3.3.0) + `docker-compose.override.yaml` |
| Сбор данных с API, 1 DAG, автоматизация | `dags/weather_data_pipeline.py`, расписание `0 * * * *` |
| Валидация: дубликаты / некорректные данные / недоступность API | `check_api_availability`+`branch_on_availability`, `validate_data`, `filter_duplicates` |
| branch / skip операторы | `BranchPythonOperator`, `ShortCircuitOperator` ×2 |
| Креды через Variables/Connections | Variable `openweather_api_key`, Connection `weather_db` |
| Логирование | `logging` в каждой задаче — Airflow UI → Task → Logs, либо `./logs/` |

## Запуск

```powershell
git clone https://github.com/<your-username>/weather-data-pipeline-airflow.git
cd weather-data-pipeline-airflow

"AIRFLOW_UID=50000" | Out-File -Encoding ASCII -FilePath .env

docker compose up airflow-init
docker compose up -d
```

UI: http://localhost:8080 (`airflow` / `airflow`)
Adminer: http://localhost:8081

### Настройка credentials (Admin в Web UI)

**Admin → Variables**
| Key | Value |
|---|---|
| `openweather_api_key` | ваш ключ с https://openweathermap.org/api |

**Admin → Connections**
| Поле | Значение |
|---|---|
| Connection Id | `weather_db` |
| Connection Type | Postgres |
| Host | `postgres-weather` |
| Database | значение `WEATHER_DB_NAME` из вашего `.env` |
| Login | значение `WEATHER_DB_USER` из вашего `.env` |
| Password | значение `WEATHER_DB_PASSWORD` из вашего `.env` |
| Port | `5432` |

Сами значения — только в локальном `.env` (см. `.env.example` за шаблоном), в репозиторий не попадают.

Включить DAG (тумблер в списке DAGs) — по умолчанию новые DAG ставятся на паузу.

## Структура проекта

```
.
├── dags/
│   └── weather_data_pipeline.py
├── docker-compose.yaml
├── docker-compose.override.yaml
├── .env
├── .gitignore
└── README.md
```