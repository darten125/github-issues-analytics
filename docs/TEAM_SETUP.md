# Запуск проекта участниками DDS/CDM

> Эта памятка фиксирует состояние передачи участникам. Текущее развёртывание полного конвейера и Metabase описано в [README](../README.md) и [словаре CDM](CDM_METRICS.md).

Команды ниже предназначены для **Git Bash на Windows** с Docker Desktop в режиме Linux containers. Подробная первоначальная настройка паролей, портов и диагностика находятся в [README](../README.md); этот документ объясняет, как получить данные для своей разработки и передать код на основную базу.

## 1. Клонирование и локальное окружение

После публикации текущих файлов в GitHub:

```bash
git clone https://github.com/darten125/github-issues-analytics.git
cd github-issues-analytics
cp .env.example .env
notepad.exe .env
```

Заполните пустые пароли, ключи и при загрузке GitHub — `GITHUB_TOKEN` по инструкции README. `.env` локален и исключён из Git. Проверьте и запустите Compose:

```bash
docker compose config --quiet
docker compose up -d --wait --wait-timeout 600
docker compose ps --all
```

Сервис `airflow-init` должен завершиться с кодом `0`; остальные сервисы должны стать `healthy`. Не используйте `docker compose down -v` для обычной остановки: это удалит локальные volumes. `docker compose down` их сохраняет.

**Клонирование Git-репозитория не переносит содержимое Docker volume.** На новой машине PostgreSQL создаётся без данных GitHub; начальный скрипт создаёт схемы `stg`, `dds`, `cdm`, но не наполняет их. Подтверждённые 993 Issues и 4 552 события находятся в локальном volume компьютера, на котором выполнялась полная загрузка, а не в GitHub. В репозитории есть код, миграции и агрегированный отчёт; дампа БД и полных ответов API нет.

## 2. Миграции и проверочная выборка

Первый старт `analytics-db` создаёт пустые схемы. Примените текущие повторяемые миграции STG `001`–`004`:

```bash
docker compose run --rm --no-deps airflow-scheduler python -m src.stg.cli init-db
```

Существующий DAG сам выполняет `prepare_stg_schema` перед загрузкой, но явный `init-db` удобен для проверки нового окружения. Он не загружает GitHub. Команда для проверки схем:

```bash
docker compose exec -T analytics-db psql -U analytics -d github_issues -Atc "SELECT schema_name FROM information_schema.schemata WHERE schema_name IN ('stg','dds','cdm') ORDER BY schema_name"
```

Для локальной разработки DDS рекомендуется **малая выборка**. В `.env` укажите собственный GitHub-токен, затем пересоздайте scheduler с обновлённым окружением:

```bash
docker compose up -d --wait --wait-timeout 600
```

В Airflow на `http://localhost:8080` включите DAG `github_issues_stg` (если он на паузе), нажмите **Trigger DAG** и задайте `repository=all`, `mode=backfill`, `created_since=2023-01-01T00:00:00Z`, `max_issues=8`, `full_load=false`. Это даст до восьми Issues на репозиторий и все доступные `/events` для них. Партии получат `issue_scan_status=sample`: ими удобно проверять структуру и SQL, но они **не подтверждают полноту** популяции. События `reopened` или `converted_to_discussion` могут не попасть в такую выборку; проверяйте эти ветки синтетическими тестовыми данными, не помещая реальные сырые JSON в Git.

После согласования DDS-кода интеграционные проверки на полном наборе запускаются **на компьютере с основным уже загруженным volume**. Для других участников `localhost:5433` означает их собственную локальную БД, а не основную. Если нужен независимый полный набор на другой машине, требуется отдельный полный запуск DAG с собственным токеном и проверкой лимитов GitHub; простое клонирование его не создаст. Дамп БД для этой передачи не создаётся.

## 3. Повтор полной загрузки при необходимости

Вручную запустите тот же DAG с параметрами:

```json
{
  "repository": "all",
  "mode": "backfill",
  "created_since": "2023-01-01T00:00:00Z",
  "max_issues": null,
  "full_load": true
}
```

Перед запуском убедитесь, что нет другого активного массового запуска и доступен лимит GitHub API. `backfill` заново перечисляет Issues, но пропускает `/events` для ранее успешно обработанных Issue ID; для последующих изменений предназначен `sync`. Не запускайте параллельно CLI-загрузчик. Проверяйте **каждую** новую партию: `status=succeeded`, `issue_scan_status=verified`, минимум два прохода и отсутствие `failed`/`running` попыток истории. `unverified` не подтверждает полный охват. В текущей задаче повторная выгрузка не выполнялась.

## 4. Доступ к PostgreSQL и проверка данных

В DBeaver создайте подключение PostgreSQL: host `127.0.0.1`, port `5433` (или `ANALYTICS_DB_PORT` из `.env`), database `github_issues`, user `analytics`, password `ANALYTICS_DB_PASSWORD` из своего `.env`. Этот пользователь — администратор учебной аналитической БД, а не будущая BI-роль. Порт Airflow DB наружу не открыт. В SQL-редакторе DBeaver можно выполнить:

```sql
SELECT repository_full_name, batch_id, status, issue_scan_status,
       issue_scan_passes, issue_scan_issue_count, finished_at
FROM stg.load_batches
ORDER BY started_at DESC;

WITH latest_repositories AS (
    SELECT DISTINCT ON (r.repository_id) r.repository_id, r.full_name
    FROM stg.repository_observations o
    JOIN stg.repositories_raw r ON r.snapshot_id = o.snapshot_id
    ORDER BY r.repository_id, o.observed_at DESC, o.observation_id DESC
)
SELECT r.full_name, count(DISTINCT i.issue_id) AS unique_issues
FROM stg.issues_raw i
JOIN latest_repositories r USING (repository_id)
GROUP BY r.full_name ORDER BY r.full_name;

SELECT count(DISTINCT event_id) AS unique_events
FROM stg.issue_events_raw;
```

На основной БД ожидаются 493 / 108 / 392 уникальных Issues и 4 552 уникальных события. На своей выборке значения будут меньше. Для чтения последнего JSON, всех уникальных событий, подтверждения истории и определения `sample`/`verified` используйте [контракт STG → DDS](STG_DDS_CONTRACT.md) и его [проверенный SQL](sql/STG_DDS_READ_QUERIES.sql). В `psql` выполните SQL-файл с `-v issue_id=<числовой GitHub ID> -v batch_id=` для последнего запуска или передайте UUID конкретной партии вместо пустого `batch_id`; замены для DBeaver описаны в контракте.

## 5. Распределение кода и интеграция

| Зона | Python | SQL | Тесты |
|---|---|---|---|
| STG | `src/stg/` | `sql/stg/` | `tests/stg/` |
| DDS | `src/dds/` | `sql/dds/` | `tests/dds/` |
| CDM | `src/cdm/` | `sql/cdm/` | `tests/cdm/` |
| Airflow | `dags/` и `src/stg/airflow_adapter.py` | — | `tests/dags/` |

DDS, миграции и тесты участника 2 реализованы; команды запуска и правила передачи приведены в [DDS_CONTRACT.md](DDS_CONTRACT.md), входы и правила показателей для CDM — в [памятке участнику 3](CDM_HANDOFF.md). Для разработки CDM без полной выгрузки используйте воспроизводимый тестовый DDS из памятки (`python -m src.dds.fixture` в отдельной базе `*_test`). `python -m src.dds.cli init-db` создаёт таблицы DDS, а `load --batch-id '<UUID>'` применяет миграции и публикует подтверждённые партии. Малую выборку можно проверить через `preview --batch-id '<UUID>'` без записи. CDM пока содержит заготовки. Не меняйте STG-таблицы и существующий DAG как побочный эффект DDS/CDM-разработки; точка включения в DAG описана в [AIRFLOW_INTEGRATION.md](AIRFLOW_INTEGRATION.md). Проверка на полной рабочей БД выполняется на компьютере с исходным volume после объединения изменений. Сначала там нужно подтвердить границу трёх партий полной выгрузки командой `python -m src.stg.cli attest-cohort`: они созданы до миграции STG `004` и имеют неизвестную границу. Порядок приёмки описан в [DDS_CONTRACT.md](DDS_CONTRACT.md#проверки-запросы-и-тестовый-dds).

Metabase должна читать аналитические данные **только из CDM**. Настройка отдельного пользователя PostgreSQL с правами чтения CDM и переключение подключения BI выполняются при интеграции CDM; текущий `analytics` не рассматривается как окончательная роль Metabase. Конфигурация и пароли Metabase сейчас не меняются.
