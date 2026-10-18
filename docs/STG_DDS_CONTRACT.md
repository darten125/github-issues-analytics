# Контракт источника STG → DDS

Статус: описание источника для DDS. Контракт описывает таблицы STG и **диагностические** правила их чтения. Правила публикации DDS (выбор партий, граница когорты, переходы статуса, проверки) нормативно заданы в [контракте DDS](DDS_CONTRACT.md), правила показателей — в [памятке участнику 3](CDM_HANDOFF.md). При расхождении действуют эти документы. Подтверждённая полная выгрузка от 18.10.2026 содержит 993 уникальных Issues и 4 552 уникальных события. Подробные партии и проверки приведены в [отчёте](../STG_FULL_LOAD_REPORT.md).

## Граница данных и идентификаторы

STG получает настоящие Issues трёх репозиториев `Kotlin/kotlinx.coroutines`, `ktorio/ktor`, `android/nowinandroid` с `created_at >= 2023-01-01T00:00:00Z`. Pull Requests исключаются по наличию поля `pull_request` в ответе `/issues`. `issue_id` и `event_id` — глобальные GitHub ID; `issue_number` имеет смысл только вместе с `repository_id`. Одна строка `*_raw` — **версия JSON**, а не обязательно отдельный GitHub-объект. Для подсчёта Issues применяйте `count(DISTINCT issue_id)` или выбор одного снимка на `issue_id`.

## Физическая структура STG

Все перечисленные таблицы находятся в схеме `stg`. Физические первичные и уникальные ключи заданы миграциями `sql/stg/001`–`004`.

### `stg.load_batches`

`batch_id UUID` — первичный ключ партии; `repository_full_name TEXT` — запрошенный репозиторий. `started_at`, `finished_at TIMESTAMPTZ` описывают выполнение загрузки. `status` принимает `running`, `succeeded`, `failed`; `error_count` и `error_message` сохраняют ошибку партии. `load_mode` принимает `legacy`, `backfill`, `sync`, `audit`.

`issue_scan_status` принимает `legacy`, `sample`, `verified`, `unverified`. `sample` означает ограниченную выборку, `verified` — совпадение двух последовательных наборов Issue ID; `issue_scan_passes` и `issue_scan_issue_count` фиксируют число проходов и размер последнего набора. `verified` является практической проверкой на момент запроса, а не неизменяемым снимком GitHub.

`created_since TIMESTAMPTZ` — фактическая граница создания Issue, с которой выполнялась партия. `created_since_source` объясняет её происхождение:

- `recorded` — граница записана загрузчиком из параметра CLI/Airflow при создании партии;
- `attested` — граница подтверждена командой `python -m src.stg.cli attest-cohort`, ссылка на сохранённую конфигурацию запуска лежит в `created_since_evidence`, время подтверждения — в `created_since_attested_at`;
- `unknown` — граница не известна. Так помечены партии, созданные до миграции `004`.

Подтверждение не меняет статусы и счётчики партии. Минимальная дата найденной задачи границу не доказывает.

Счётчики `repositories_fetched`, `repository_versions_inserted`, `issues_fetched`, `issues_selected`, `issue_versions_inserted`, `events_fetched`, `event_versions_inserted` описывают работу **этой партии**. `issues_fetched` включает элементы страниц `/issues`, в том числе PR и контрольные проходы; `*_versions_inserted` считает новые JSON-версии, не уникальные GitHub ID и не наблюдения. Не используйте эти счётчики как размер DDS без проверки зерна.

### Таблицы JSON-снимков

| Таблица | Первичный ключ и уникальность | Специфичные поля |
|---|---|---|
| `stg.repositories_raw` | `snapshot_id BIGINT`; уникально `(repository_id, payload_sha256)` | `repository_id BIGINT` — GitHub ID; `full_name TEXT` — имя `owner/repo`. |
| `stg.issues_raw` | `snapshot_id BIGINT`; уникально `(issue_id, payload_sha256)` | `issue_id BIGINT` — GitHub ID; `repository_id BIGINT` — родительский репозиторий; `issue_number INTEGER` — номер внутри него. |
| `stg.issue_events_raw` | `snapshot_id BIGINT`; уникально `(event_id, payload_sha256)` | `event_id BIGINT` — GitHub ID события; `issue_id`, `repository_id BIGINT` — контекст запроса; `event_type TEXT` — тип события; `github_created_at TIMESTAMPTZ` — время события на GitHub. |

У всех трёх таблиц есть `raw_json JSONB`, `payload_sha256 CHAR(64)`, `first_seen_at`, `last_seen_at TIMESTAMPTZ`, `first_batch_id`, `last_batch_id UUID`. Оба поля `*_batch_id` физически ссылаются на `stg.load_batches(batch_id)`. `raw_json` хранит разобранный JSON-объект ответа GitHub; тип JSONB не сохраняет исходный порядок ключей и байтовое представление HTTP-ответа. Хеш вычисляется из канонического JSON с отсортированными ключами. Идентичный ответ использует старую строку и обновляет `last_seen_at`/`last_batch_id`; изменённый JSON создаёт новый `snapshot_id`. Старые версии не удаляются.

`repository_id` в `issues_raw`, а также `issue_id`/`repository_id` в `issue_events_raw` образуют **логические** связи с GitHub-объектами. В схеме нет FK непосредственно между версиями репозитория, Issue и события: один объект может иметь несколько снимков. Связь проверяется запросами целостности из отчёта. Для соединений используйте GitHub ID и проверяйте `repository_id`, а не соединяйте строки по `snapshot_id` разных объектов.

### Таблицы наблюдений

`stg.repository_observations`, `stg.issue_observations`, `stg.event_observations` имеют одинаковое зерно: `observation_id BIGINT` — первичный ключ, `snapshot_id BIGINT` — FK на соответствующую таблицу `*_raw`, `batch_id UUID` — FK на `load_batches`, `observed_at TIMESTAMPTZ` — время получения этой версии загрузчиком. Уникальная пара `(batch_id, snapshot_id)` допускает одно подтверждение версии за партию. Повтор одной JSON-версии в следующих партиях даёт новые наблюдения без новых снимков. При миграции старых данных известные первое и последнее наблюдения восстановлены; неизвестные промежуточные наблюдения восстановить нельзя. Для последнего реально наблюдавшегося состояния предпочитайте `observed_at, observation_id`, а не счётчики партии.

### `stg.issue_event_loads`

Первичный ключ `(batch_id, issue_id)`; `batch_id` — FK на `load_batches`. Поля `repository_id`, `issue_number` фиксируют контекст Issue; `issue_updated_at TIMESTAMPTZ` — значение GitHub `updated_at` в момент обхода истории. `mode` — `backfill`, `sync` или `audit`; `status` — `running`, `succeeded`, `failed`; `events_fetched` — число полученных событий в **этой попытке**; `started_at`, `finished_at`, `error_message` — исполнение и ошибка. `succeeded` означает успешное прохождение всех доступных страниц `/events` для данного Issue, в том числе когда страниц с событиями нет. При `backfill` история, успешно загруженная в предыдущей партии, может быть пропущена: полноту проверяют по **всем** успешным попыткам Issue, а не только по текущему `batch_id`.

## Правила чтения и исполняемые SQL-запросы

[STG_DDS_READ_QUERIES.sql](sql/STG_DDS_READ_QUERIES.sql) — часть этого контракта. В нём восемь read-only запросов; первые семь реализуют требуемые способы чтения, восьмой воспроизводит пограничный случай. Скрипт выполнен против текущего PostgreSQL в `BEGIN READ ONLY` с `psql -v issue_id=2159200356 -v batch_id=`; результаты с `raw_json` при проверке не экспортировались. Пустой `batch_id` означает проверку последнего запуска каждого репозитория; для конкретного запуска передайте его UUID из Airflow, например `-v batch_id=71ab2fee-ba51-41e2-8b38-a13bc53d3e4d`. Для DBeaver замените `:issue_id` на числовой ID, а `NULLIF(:'batch_id', '')::uuid` в запросе № 6 — на `NULL::uuid` или `'<batch_id>'::uuid`. Для `psql`:

```bash
docker compose exec -T analytics-db psql -X -q -v ON_ERROR_STOP=1 -v issue_id=2159200356 -v batch_id= -U analytics -d github_issues -o /dev/null -c 'BEGIN READ ONLY' -f - -c 'ROLLBACK' < docs/sql/STG_DDS_READ_QUERIES.sql
```

| № | Результат | Правило |
|---:|---|---|
| 1 | Уникальные репозитории | Последнее наблюдение каждого `repository_id`; `full_name` берётся из этого снимка. |
| 2 | Актуальный снимок каждого Issue | `DISTINCT ON (issue_id)` после соединения `issue_observations` и `issues_raw`; сортировка по времени и ID наблюдения. |
| 3 | Полный набор уникальных событий | Один последний снимок на глобальный `event_id`; сохраняйте `issue_id` и `repository_id` для связи. |
| 4 | Статусные события конкретного Issue | `closed`, `reopened` и отдельно `converted_to_discussion`; сортировка по `(github_created_at, event_id)`. |
| 5 | Полнота истории Issue | Последняя успешная попытка `/events`, сравнение её `issue_updated_at` с последним `updated_at`, статус последней попытки. |
| 6 | Диагностика готовности партии | При заданном `batch_id` выбирается именно он; иначе сначала определяется последний запуск каждого репозитория по `started_at`, включая неуспешные и выборочные. Затем `ready=true` только для `succeeded` + `verified`, минимум двух проходов и отсутствия незавершённых попыток в партии. Граница когорты здесь не проверяется. |
| 7 | Детерминированная последовательность | Уникальные события Issue в порядке `(github_created_at, event_id)` с порядковым номером. |
| 8 | Пограничный случай | Read-only выборка закрытого Issue с `converted_to_discussion` без `closed`. |

Запрос № 6 — диагностика: он возвращает партию вместе с её фактическими статусами и флагом `ready`. `ready=false` запрещает использовать партию как подтверждённую полную выгрузку, но `ready=true` ещё не разрешает публикацию. DDS не выбирает «последнюю» партию сам: он принимает явные `batch_id` из текущего `summarize`/`verify_*` и проверяет именно их, включая границу когорты. Пустой параметр предназначен для проверки **последнего запуска** каждого репозитория: более новые `failed`, `running`, `sample` и `unverified` не подменяются ранней успешной партией. Явный исторический `batch_id` проверяется независимо от последующих запусков; отсутствие строки означает, что такой партии нет. `issue_scan_issue_count` — число уникальных Issue ID в контрольном проходе, в отличие от счётчика запросов `issues_fetched`. Запрос № 5 даёт проверку свежести по `updated_at`, но сам `/events` может содержать активность, не отражённую этим полем; для нужного уровня доверия требуется отдельная политика `sync`/`audit`.

## Контракт времени для DDS и будущего SCD2

| Время | Источник | Применение |
|---|---|---|
| Создание Issue на GitHub | `issues_raw.raw_json->>'created_at'` | Начало существования Issue и отбор когорты с 01.01.2023; первый кандидат `valid_from`. |
| Изменение Issue на GitHub | `issues_raw.raw_json->>'updated_at'` | Признак необходимости `sync`; **не** время каждого изменения статуса. |
| Возникновение события на GitHub | `issue_events_raw.github_created_at` и `raw_json->>'created_at'` | Порядок статусных переходов и границы будущих интервалов состояния. |
| Наблюдение/загрузка в STG | `*_observations.observed_at`, `*_raw.first_seen_at`/`last_seen_at`, `load_batches.started_at`/`finished_at`, `issue_event_loads.started_at`/`finished_at` | Аудит происхождения и свежести загрузки, **не** дата бизнес-события GitHub. |

Времена `TIMESTAMPTZ` хранятся и сравниваются с явным часовым поясом, отчётные группировки — в UTC. Бизнес-даты Issue и события не могут быть позже `finished_at` выбранной партии. SCD2 реализован в DDS по правилам [контракта DDS](DDS_CONTRACT.md#история-статусов): начало — GitHub `created_at`, границы — `github_created_at` статусных событий в порядке `(github_created_at, event_id)`, интервалы `[valid_from, valid_to)`. Последний снимок `state`/`closed_at` служит проверкой реконструкции, но не заменяет исторические события.

## Пограничные случаи и ограничения источника

- **`converted_to_discussion`:** read-only запрос № 8 нашёл `android/nowinandroid` `issue_id=2159200356`, `issue_number=1232`. Последний снимок имеет `state=closed`, `closed_at=2024-03-11T22:47:34Z`; среди уникальных событий есть одно `converted_to_discussion` и нет `closed`. DDS считает конверсию закрытием по времени события и сохраняет исходный тип, исходный JSON не правится.
- **Повторные `closed`:** считать переходы по уникальным `event_id`, не принимать первый или последний `closed` за единственную историю. В текущих данных есть Issues, закрывавшиеся неоднократно.
- **`reopened`:** после него возможен новый `closed`; метрики «когда-либо закрыт» и «закрыт на дату» различаются. `reopened` без ранее известного закрытия (включая конверсию) означает неполную историю: DDS блокирует такую публикацию, закрытие не синтезируется.
- **Одинаковое время:** порядок `(github_created_at, event_id)`. Закрытие и открытие в один момент остаются двумя фактическими переходами событий, даже если закрытого интервала положительной длительности нет.
- **Нет событий:** успешная попытка `/events` с `events_fetched=0` допустима. В полной выборке 33 таких Issues; отсутствие строк в `issue_events_raw` само по себе не означает сбой загрузки.
- **Несколько снимков:** у одного `issue_id` или `event_id` может быть несколько `snapshot_id`; не размножайте объекты при JOIN наблюдений. Последний снимок — текущее *наблюдаемое* состояние, а не доказательство всех промежуточных состояний.
- **Граница `/events`:** загрузчик читает все доступные страницы [Issue Events API](https://docs.github.com/en/rest/issues/events) для выбранных Issues; отдельные [timeline events](https://docs.github.com/en/rest/issues/timeline) и комментарии не загружаются. Это не журнал каждой правки Issue. Страницы GitHub не образуют один атомарный снимок на момент времени; контрольные проходы сверяют наборы Issue ID, но не доказывают абсолютную полноту истории. `backfill` пропускает ранее успешные истории; `sync` ориентируется на `updated_at`, `audit` перечитывает события. Типы событий описаны в [официальном справочнике GitHub](https://docs.github.com/en/rest/using-the-rest-api/issue-event-types).

## Показатели

Нормативные правила шести показателей, медиан, долей, автора и текущих меток зафиксированы в [памятке участнику 3](CDM_HANDOFF.md), эталонные запросы — в [CDM_REFERENCE_METRICS.sql](sql/CDM_REFERENCE_METRICS.sql). Все показатели строятся по уникальным Issues когорты без Pull Requests. Исторический статус берётся из SCD2 DDS, а не из текущего `state`. Состояние на конец дня — состояние непосредственно перед следующей полуночью UTC, и только в пределах горизонта отчётности публикации. Запросы этого контракта к STG для расчёта показателей не используются.
