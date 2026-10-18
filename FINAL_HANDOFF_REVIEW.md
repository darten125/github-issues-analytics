# Финальная проверка передачи STG

Дата: 18.10.2026. Изменены только запрос № 6 в `docs/sql/STG_DDS_READ_QUERIES.sql` и согласованные инструкции в `docs/STG_DDS_CONTRACT.md` и `docs/TEAM_SETUP.md`. Загрузчик, миграции, DAG, Compose и данные PostgreSQL не изменялись.

## Исправленный запрос

Запрос № 6 принимает `batch_id` через `psql -v batch_id=<UUID>`; пустое значение выбирает последний запуск **каждого** репозитория. `row_number()` вычисляется по всем партиям с сортировкой `started_at DESC, batch_id DESC` до проверки статусов. Затем возвращается выбранная партия с `ready=true` только при `succeeded`, `verified`, двух или более контрольных проходах и отсутствии незавершённых загрузок истории в этой партии. Более новые `failed`, `running`, `sample` и `unverified` остаются видимыми с `ready=false`; старая успешная партия их не подменяет. Явный UUID позволяет независимо проверить историческую партию. Для числа уникальных Issues используется `issue_scan_issue_count`, не `issues_fetched`.

## Выполненные проверки

| Проверка | Результат |
|---|---|
| Реальная БД, текущие партии | Три последних запуска `succeeded/verified`, `ready=true`: Kotlin `71ab2fee-ba51-41e2-8b38-a13bc53d3e4d` (493), Ktor `3024e69b-041d-41bb-9cd8-ef979e11093f` (108), NowInAndroid `b6e45856-a30d-47fa-98c1-8f3cb2a6d5cc` (392). Независимый `count(DISTINCT issue_id)` дал те же 993 Issues. |
| Тестовые CTE, новый `failed` | Выбрана новая неуспешная партия, `ready=false`; старая подтверждённая не возвращена. |
| Тестовые CTE, новый `sample` | Выбрана новая выборочная партия, `ready=false`; старая подтверждённая не возвращена. Дополнительно проверены `unverified` и `running`. |
| Исторический `batch_id` | Тестовая прежняя партия возвращена с `ready=true` несмотря на более новый `failed`; явный UUID существующей партии также возвращён корректно. |
| SQL и конфигурация | Все восемь запросов контракта прошли в `BEGIN READ ONLY`; `docker compose config --quiet` завершился успешно. |
| Документация и публикация | Локальные ссылки шести исходных Markdown-документов проверены; README фиксирует завершённую полную STG-выгрузку. `AGENTS.md` отсутствует, `.env` игнорируется Git. |

В архиве `review_sql/query6_synthetic.sql` содержит тестовые CTE, а `results/query6_*.txt` и `results/unique_issues_real.txt` — результаты новых read-only проверок. Исторические результаты полной загрузки и тестов сохранены в остальных файлах `results/`, `review_sql/` и `quality_sql/`. GitHub API и повторная загрузка не запускались. Commit и push не выполнялись.

## Файлы для публикации в GitHub

- Корень: `.env.example`, `.gitignore`, `compose.yaml`, `README.md`, `STG_FULL_LOAD_REPORT.md`, `HANDOFF_REVIEW.md`, `FINAL_HANDOFF_REVIEW.md`.
- Документация: `docs/STG_DDS_CONTRACT.md`, `docs/TEAM_SETUP.md`, `docs/AIRFLOW_INTEGRATION.md`, `docs/sql/STG_DDS_READ_QUERIES.sql`.
- Инфраструктура и STG: `docker/postgres/init/10-init.sh`, `dags/github_issues_stg.py`, `sql/stg/001_create_tables.sql`, `sql/stg/002_observations_and_issue_loads.sql`, `sql/stg/003_audit_and_issue_scan.sql`, все файлы `src/stg/*.py` и `src/__init__.py`.
- Тесты и заготовки зон команды: `tests/stg/*.py`, `tests/dags/*.py`, существующие `__init__.py` и `.gitkeep` в `src/dds/`, `src/cdm/`, `sql/dds/`, `sql/cdm/`, `tests/dds/`, `tests/cdm/`, `dags/`, `sql/stg/`, `tests/stg/`.

Файлы `results/`, `review_sql/` и `quality_sql/` в ZIP являются материалами проверки; они не нужны для запуска проекта. Локальный `.env`, пароли, токены, дампы БД и Docker volumes в публикацию не входят. DDS/CDM остаются зоной других участников.
