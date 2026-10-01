from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

import requests
from airflow.exceptions import AirflowException
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.python import (
    BranchPythonOperator,
    PythonOperator,
    ShortCircuitOperator,
)
from airflow.sdk import DAG, Variable
from airflow.utils.trigger_rule import TriggerRule

logger = logging.getLogger(__name__)

# Конфигурация
CITIES = ["Prague", "Berlin", "London", "Tokyo"]

WEATHER_API_KEY_VAR = "openweather_api_key"   
WEATHER_DB_CONN_ID = "weather_db"             

API_BASE_URL = "https://api.openweathermap.org"
REQUEST_TIMEOUT = 10  #секунд

default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
}



# Подготовка таблицы для результатов
def create_table(**context) -> None:
    hook = PostgresHook(postgres_conn_id=WEATHER_DB_CONN_ID)
    hook.run(
        """
        CREATE TABLE IF NOT EXISTS weather_data (
            id SERIAL PRIMARY KEY,
            city VARCHAR(100) NOT NULL,
            temperature NUMERIC(5, 2) NOT NULL,
            humidity INTEGER NOT NULL,
            weather_description VARCHAR(200),
            observed_at TIMESTAMPTZ NOT NULL,
            inserted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            UNIQUE (city, observed_at)
        );
        """
    )
    logger.info("Таблица weather_data готова к использованию.")



# Проверка доступности API + branch
def check_api_availability(**context) -> None:
    try:
        response = requests.get(f"{API_BASE_URL}/", timeout=REQUEST_TIMEOUT)
        available = response.status_code < 500
    except requests.exceptions.RequestException as exc:
        logger.warning("Не удалось связаться с OpenWeatherMap: %s", exc)
        available = False

    logger.info("Результат проверки доступности API: %s", available)
    context["ti"].xcom_push(key="api_available", value=available)


def branch_on_availability(**context) -> str:
    available = context["ti"].xcom_pull(
        task_ids="check_api_availability", key="api_available"
    )
    return "fetch_weather_data" if available else "notify_api_unavailable"


def notify_api_unavailable(**context) -> None:
    logger.error(
        "API OpenWeatherMap недоступен на момент запуска run_id=%s. "
        "Сбор данных пропущен, повторная попытка — по расписанию.",
        context["run_id"],
    )

# 2. Сбор данных
def fetch_weather_data(**context) -> None:
    try:
        api_key = Variable.get(WEATHER_API_KEY_VAR)
    except KeyError as exc:
        raise AirflowException(
            f"Airflow Variable '{WEATHER_API_KEY_VAR}' не найдена. "
            "Добавьте её в Admin -> Variables."
        ) from exc

    records: list[dict] = []
    failed_cities: list[str] = []

    for city in CITIES:
        try:
            resp = requests.get(
                f"{API_BASE_URL}/data/2.5/weather",
                params={"q": city, "appid": api_key, "units": "metric"},
                timeout=REQUEST_TIMEOUT,
            )
            if resp.status_code == 401:
                raise AirflowException(
                    "OpenWeatherMap вернул 401 Unauthorized — проверьте значение "
                    f"Variable '{WEATHER_API_KEY_VAR}'."
                )
            resp.raise_for_status()
            payload = resp.json()

            records.append(
                {
                    "city": city,
                    "temperature": payload["main"]["temp"],
                    "humidity": payload["main"]["humidity"],
                    "weather_description": payload["weather"][0]["description"],
                    "observed_at": datetime.fromtimestamp(
                        payload["dt"], tz=timezone.utc
                    ).isoformat(),
                }
            )
            logger.info("Данные по городу '%s' получены.", city)
        except requests.exceptions.RequestException as exc:
            logger.warning("Ошибка запроса для города '%s': %s", city, exc)
            failed_cities.append(city)

    if not records:
        raise AirflowException(
            f"Не удалось получить данные ни по одному городу. Ошибки: {failed_cities}"
        )

    logger.info(
        "Успешно получено: %s из %s городов (ошибки: %s).",
        len(records),
        len(CITIES),
        failed_cities or "нет",
    )
    context["ti"].xcom_push(key="raw_records", value=records)

# Валидация данных
def validate_data(**context) -> None:
    raw_records = (
        context["ti"].xcom_pull(task_ids="fetch_weather_data", key="raw_records") or []
    )

    valid, invalid = [], []
    for rec in raw_records:
        reasons = []

        if not rec.get("city"):
            reasons.append("пустое название города")

        temp = rec.get("temperature")
        if not isinstance(temp, (int, float)) or not (-90 <= temp <= 60):
            reasons.append(f"некорректная температура: {temp}")

        humidity = rec.get("humidity")
        if not isinstance(humidity, (int, float)) or not (0 <= humidity <= 100):
            reasons.append(f"некорректная влажность: {humidity}")

        if not rec.get("observed_at"):
            reasons.append("отсутствует observed_at")

        if reasons:
            logger.warning(
                "Запись по городу '%s' отбракована: %s",
                rec.get("city", "?"),
                "; ".join(reasons),
            )
            invalid.append({**rec, "reasons": reasons})
        else:
            valid.append(rec)

    logger.info("Валидация завершена: valid=%s, invalid=%s", len(valid), len(invalid))
    context["ti"].xcom_push(key="valid_records", value=valid)
    context["ti"].xcom_push(key="invalid_records", value=invalid)


def has_valid_data(**context) -> bool:
    valid = context["ti"].xcom_pull(task_ids="validate_data", key="valid_records") or []
    if not valid:
        logger.info("Валидных записей нет — дальнейшие шаги будут пропущены (skip).")
    return bool(valid)



# Проверка на дубликаты
def filter_duplicates(**context) -> None:
    valid_records = (
        context["ti"].xcom_pull(task_ids="validate_data", key="valid_records") or []
    )
    hook = PostgresHook(postgres_conn_id=WEATHER_DB_CONN_ID)

    new_records, duplicate_count = [], 0
    for rec in valid_records:
        exists = hook.get_first(
            "SELECT 1 FROM weather_data WHERE city = %s AND observed_at = %s",
            parameters=(rec["city"], rec["observed_at"]),
        )
        if exists:
            duplicate_count += 1
            logger.info(
                "Дубликат пропущен: %s @ %s", rec["city"], rec["observed_at"]
            )
        else:
            new_records.append(rec)

    logger.info(
        "Проверка дубликатов завершена: новых=%s, дубликатов=%s",
        len(new_records),
        duplicate_count,
    )
    context["ti"].xcom_push(key="new_records", value=new_records)


def has_new_data(**context) -> bool:
    new_records = (
        context["ti"].xcom_pull(task_ids="filter_duplicates", key="new_records") or []
    )
    if not new_records:
        logger.info("Новых записей нет (всё — дубликаты) — сохранение пропущено (skip).")
    return bool(new_records)


# Сохранение результата
def save_data(**context) -> None:
    new_records = (
        context["ti"].xcom_pull(task_ids="filter_duplicates", key="new_records") or []
    )
    hook = PostgresHook(postgres_conn_id=WEATHER_DB_CONN_ID)

    insert_sql = """
        INSERT INTO weather_data
            (city, temperature, humidity, weather_description, observed_at)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (city, observed_at) DO NOTHING
        RETURNING id;
    """

    inserted = 0
    conn = hook.get_conn()
    try:
        with conn.cursor() as cur:
            for rec in new_records:
                cur.execute(
                    insert_sql,
                    (
                        rec["city"],
                        rec["temperature"],
                        rec["humidity"],
                        rec["weather_description"],
                        rec["observed_at"],
                    ),
                )
                if cur.fetchone() is not None:
                    inserted += 1
        conn.commit()
    finally:
        conn.close()

    logger.info(
        "Сохранение завершено: вставлено %s новых строк из %s кандидатов.",
        inserted,
        len(new_records),
    )



# Итоговое логирование 
def log_summary(**context) -> None:
    ti = context["ti"]
    api_available = ti.xcom_pull(task_ids="check_api_availability", key="api_available")
    valid = ti.xcom_pull(task_ids="validate_data", key="valid_records")
    invalid = ti.xcom_pull(task_ids="validate_data", key="invalid_records")
    new_records = ti.xcom_pull(task_ids="filter_duplicates", key="new_records")

    logger.info("========== Итог запуска weather_data_pipeline ==========")
    logger.info("run_id: %s", context["run_id"])
    logger.info("API был доступен: %s", api_available)
    if valid is not None:
        logger.info(
            "Валидных записей: %s, отбраковано: %s", len(valid), len(invalid or [])
        )
    if new_records is not None:
        logger.info("Новых (не дублирующихся) записей: %s", len(new_records))
    logger.info("==========================================================")


# Определение DAG
with DAG(
    dag_id="weather_data_pipeline",
    description="Сбор данных о погоде с OpenWeatherMap: валидация, дедупликация, сохранение в Postgres",
    default_args=default_args,
    schedule="0 * * * *",       
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["weather", "api", "etl"],
) as dag:

    t_create_table = PythonOperator(
        task_id="create_table",
        python_callable=create_table,
    )

    t_check_api = PythonOperator(
        task_id="check_api_availability",
        python_callable=check_api_availability,
    )

    t_branch = BranchPythonOperator(
        task_id="branch_on_availability",
        python_callable=branch_on_availability,
    )

    t_notify_down = PythonOperator(
        task_id="notify_api_unavailable",
        python_callable=notify_api_unavailable,
    )

    t_fetch = PythonOperator(
        task_id="fetch_weather_data",
        python_callable=fetch_weather_data,
    )

    t_validate = PythonOperator(
        task_id="validate_data",
        python_callable=validate_data,
    )

    t_has_valid = ShortCircuitOperator(
        task_id="check_has_valid_data",
        python_callable=has_valid_data,
        ignore_downstream_trigger_rules=False,
    )

    t_dedupe = PythonOperator(
        task_id="filter_duplicates",
        python_callable=filter_duplicates,
    )

    t_has_new = ShortCircuitOperator(
        task_id="check_has_new_data",
        python_callable=has_new_data,
        ignore_downstream_trigger_rules=False,
    )

    t_save = PythonOperator(
        task_id="save_data",
        python_callable=save_data,
    )

    t_summary = PythonOperator(
        task_id="log_summary",
        python_callable=log_summary,
        trigger_rule=TriggerRule.NONE_FAILED,
    )

    t_create_table >> t_check_api >> t_branch
    t_branch >> t_fetch >> t_validate >> t_has_valid >> t_dedupe >> t_has_new >> t_save >> t_summary
    t_branch >> t_notify_down >> t_summary