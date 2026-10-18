# GitHub Issues Analytics

Учебное аналитическое хранилище для жизненного цикла GitHub Issues из `Kotlin/kotlinx.coroutines`, `ktorio/ktor` и `android/nowinandroid`. Планируемая выборка: Issues, созданные с 1 января 2023 года, без Pull Requests, с доступной историей изменений. Целевой поток: GitHub REST API → STG → DDS → CDM → Metabase, под управлением Apache Airflow.

Сейчас в репозитории есть только инфраструктура. DAG, загрузка GitHub API, таблицы и преобразования STG/DDS/CDM будут добавляться отдельно.

## Состав окружения

| Компонент | Образ | Назначение |
| --- | --- | --- |
| PostgreSQL | `postgres:16.15` | Аналитическая база и отдельная служебная база Metabase |
| PostgreSQL | `postgres:16.15` | Отдельная база метаданных Airflow |
| Apache Airflow | `apache/airflow:3.3.2` | API server, scheduler, DAG processor, одноразовая инициализация; `LocalExecutor` |
| Metabase | `metabase/metabase:v0.63.18` | Веб-интерфейс аналитики |

Все сервисы подключены к сети Compose `internal`. PostgreSQL Airflow не опубликован на хосте. Аналитический PostgreSQL и веб-интерфейсы доступны только через `127.0.0.1`. Данные обоих PostgreSQL хранятся в именованных Docker volumes; логи Airflow также сохраняются в volume. Metabase хранит свои настройки в отдельной базе `metabaseapp` аналитического экземпляра PostgreSQL.

## Запуск на Windows

Нужны Docker Desktop с включёнными Linux containers и Git Bash. Для Airflow выделите Docker Desktop не менее 4 ГБ оперативной памяти; 8 ГБ удобнее для одновременного запуска Metabase. Используйте современный `docker compose` (V2 или новее), а не старый `docker-compose` V1. Команды ниже выполняются в Git Bash.

1. Клонируйте репозиторий и перейдите в его каталог:

   ```bash
   git clone https://github.com/darten125/github-issues-analytics.git
   cd github-issues-analytics
   ```

2. Создайте локальный файл настроек:

   ```bash
   cp .env.example .env
   ```

3. Сгенерируйте случайные значения для четырёх паролей и двух ключей одной командой. Команда использует уже выбранный образ Airflow, поэтому отдельная установка Python не нужна:

   ```bash
   docker run --rm apache/airflow:3.3.2 python -c 'import secrets; from cryptography.fernet import Fernet; print("ANALYTICS_DB_PASSWORD=" + secrets.token_hex(24)); print("AIRFLOW_DB_PASSWORD=" + secrets.token_hex(24)); print("AIRFLOW_ADMIN_PASSWORD=" + secrets.token_hex(24)); print("AIRFLOW_FERNET_KEY=" + Fernet.generate_key().decode()); print("AIRFLOW_JWT_SECRET=" + secrets.token_hex(32)); print("METABASE_DB_PASSWORD=" + secrets.token_hex(24))'
   ```

   Откройте `.env` через `notepad.exe .env` и заполните пустые значения соответствующими строками из вывода. Значения имён баз, пользователей и портов можно оставить как в примере. Пароли баз данных должны состоять из URL-безопасных символов; команда выше выдаёт шестнадцатеричные значения. Не добавляйте `.env` в Git и не публикуйте вывод генератора.

4. Проверьте конфигурацию и запустите окружение:

   ```bash
   docker compose config --quiet
   docker compose up -d --wait --wait-timeout 600
   docker compose ps --all
   ```

   `config --quiet` проверяет подстановку переменных и Compose-синтаксис, не выводя пароли. Первый запуск скачивает образы и инициализирует базы; Metabase может запускаться несколько минут. Сервис `airflow-init` должен завершиться с кодом `0`, остальные — быть `healthy`.

5. Откройте [Airflow](http://localhost:8080) и войдите с `AIRFLOW_ADMIN_USERNAME` / `AIRFLOW_ADMIN_PASSWORD` из `.env`. Откройте [Metabase](http://localhost:3000) и пройдите первоначальную настройку. Если во время настройки Metabase вы добавляете аналитическую базу, укажите хост `analytics-db`, порт `5432`, имя базы `github_issues`, пользователя `analytics` и пароль `ANALYTICS_DB_PASSWORD` из `.env`. Пока в схемах нет таблиц.

## Проверка и управление

Проверить схемы аналитической базы из Git Bash:

```bash
docker compose exec -T analytics-db psql -U analytics -d github_issues -Atc "SELECT schema_name FROM information_schema.schemata WHERE schema_name IN ('stg','dds','cdm') ORDER BY schema_name"
```

Ожидаются три строки: `cdm`, `dds`, `stg`. Также можно проверить сервисы:

```bash
docker compose exec -T airflow-db pg_isready -U airflow -d airflow
curl -fsS http://localhost:8080/api/v2/monitor/health
curl -fsS http://localhost:3000/api/health
```

При неполадках:

```bash
docker compose ps --all
docker compose logs --tail=100 analytics-db airflow-db airflow-init airflow-apiserver airflow-scheduler airflow-dag-processor metabase
```

Остановить контейнеры, сохранив базы:

```bash
docker compose down
```

Повторный запуск: `docker compose up -d --wait --wait-timeout 600`. Не используйте `docker compose down -v`, если хотите сохранить данные: эта команда удаляет volumes. Скрипт `docker/postgres/init/10-init.sh` запускается только при **первой** инициализации пустого volume аналитической базы. Изменение паролей баз в `.env` после первого запуска не меняет существующие роли PostgreSQL автоматически.

## Каталоги для следующей разработки

- `dags/` — определения DAG для Airflow.
- `src/stg/`, `src/dds/`, `src/cdm/` — Python-код этапов.
- `sql/stg/`, `sql/dds/`, `sql/cdm/` — SQL каждого этапа.
- `tests/stg/`, `tests/dds/`, `tests/cdm/` — тесты по зонам ответственности.

В контейнерах Airflow каталоги `dags`, `src` и `sql` смонтированы только для чтения. Python-код из `src` импортируется как `src.stg`, `src.dds` или `src.cdm`; SQL доступен по `/opt/airflow/sql`. Отдельные изменения DDS/CDM могут добавляться без изменения конфигурации Compose.

## Основа конфигурации

Конфигурация адаптирует [официальный пример Airflow для Docker Compose](https://airflow.apache.org/docs/apache-airflow/3.3.2/howto/docker-compose/) под [LocalExecutor](https://airflow.apache.org/docs/apache-airflow/3.3.2/core-concepts/executor/index.html): без Celery worker, брокера Redis и triggerer, который понадобится только для deferrable tasks. Порядок старта задают [healthcheck и `depends_on`](https://docs.docker.com/compose/how-tos/startup-order/). Служебная база Metabase следует [рекомендации Metabase использовать PostgreSQL](https://www.metabase.com/docs/latest/installation-and-operation/configuring-application-database).
