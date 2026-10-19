"""Idempotently provision the CDM data source, questions and dashboards.

Uses Metabase's ordinary HTTP API, available in the open-source edition.
Requires a running local Metabase and CDM schema. Reads secrets from untracked
.env without printing them. Run: python3 scripts/provision_metabase.py
"""

from __future__ import annotations

import json
import os
import sys
import time
import urllib.error
import urllib.request
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def settings():
    values = {}
    for line in (ROOT / ".env").read_text(encoding="utf-8").splitlines():
        if "=" in line and not line.lstrip().startswith("#"):
            key, value = line.split("=", 1)
            values[key.strip()] = value.strip().strip('"').strip("'")
    values.update({key: value for key, value in os.environ.items() if key in values})
    required = ("METABASE_ADMIN_EMAIL", "METABASE_ADMIN_PASSWORD", "CDM_READ_PASSWORD",
                "ANALYTICS_DB_NAME")
    missing = [key for key in required if not values.get(key)]
    if missing:
        raise RuntimeError("Missing local settings: " + ", ".join(missing))
    return values


class Metabase:
    def __init__(self, base):
        self.base = base.rstrip("/")
        self.session = None

    def call(self, method, path, body=None):
        data = None if body is None else json.dumps(body).encode("utf-8")
        headers = {"Content-Type": "application/json"}
        if self.session:
            headers["X-Metabase-Session"] = self.session
        request = urllib.request.Request(self.base + path, data=data, headers=headers, method=method)
        try:
            with urllib.request.urlopen(request, timeout=45) as response:
                raw = response.read()
                return json.loads(raw) if raw else None
        except urllib.error.HTTPError as exc:
            # API errors can contain connection details; never print a response body.
            raise RuntimeError(f"Metabase {method} {path} returned HTTP {exc.code}") from None

    def wait_healthy(self):
        for _ in range(90):
            try:
                if self.call("GET", "/api/health").get("status") == "ok":
                    return
            except (RuntimeError, urllib.error.URLError):
                pass
            time.sleep(5)
        raise RuntimeError("Metabase did not become healthy")

    def authenticate(self, values):
        properties = self.call("GET", "/api/session/properties")
        email = values["METABASE_ADMIN_EMAIL"]
        password = values["METABASE_ADMIN_PASSWORD"]
        if properties.get("has-user-setup"):
            result = self.call("POST", "/api/session", {"username": email, "password": password})
        else:
            token = properties.get("setup-token")
            if not token:
                raise RuntimeError("Metabase setup token is unavailable")
            result = self.call("POST", "/api/setup", {
                "token": token,
                "user": {"first_name": "Course", "last_name": "Admin",
                         "email": email, "password": password},
                "prefs": {"site_name": "GitHub Issues Analytics", "site_locale": "ru"},
            })
        self.session = result["id"]


def _list(result):
    if isinstance(result, list):
        return result
    if isinstance(result, dict):
        return result.get("data", [])
    return []


def ensure_database(api, values):
    name = "GitHub Issues CDM"
    databases = _list(api.call("GET", "/api/database"))
    existing = next((item for item in databases if item.get("name") == name), None)
    payload = {
        "name": name,
        "engine": "postgres",
        "details": {"host": "analytics-db", "port": 5432,
                    "dbname": values["ANALYTICS_DB_NAME"],
                    "user": "cdm_reader", "password": values["CDM_READ_PASSWORD"]},
    }
    if existing:
        database_id = existing["id"]
        api.call("PUT", f"/api/database/{database_id}", payload)
    else:
        database_id = api.call("POST", "/api/database", payload)["id"]
    api.call("POST", f"/api/database/{database_id}/sync_schema")
    api.call("POST", f"/api/database/{database_id}/rescan_values")
    return database_id


def field_ids(api, database_id):
    wanted = {
        "repository_snapshot": {"full_name"},
        "issue_day": {"report_day"},
        "issue_lifecycle": {"first_closed_day"},
    }
    for _ in range(60):
        tables = _list(api.call("GET", f"/api/table?db_id={database_id}"))
        found = {table["name"]: table for table in tables
                 if table.get("schema") == "cdm" and table["name"] in wanted}
        if len(found) == len(wanted):
            fields = {}
            for table_name, names in wanted.items():
                metadata = api.call("GET", f"/api/table/{found[table_name]['id']}/query_metadata")
                for field in metadata.get("fields", []):
                    if field["name"] in names:
                        fields[(table_name, field["name"])] = field["id"]
            if len(fields) == sum(map(len, wanted.values())):
                return fields
        time.sleep(3)
    raise RuntimeError("CDM fields did not appear in the Metabase schema sync")


def ensure_collection(api):
    name = "GitHub Issues — CDM"
    existing = next((item for item in _list(api.call("GET", "/api/collection"))
                     if item.get("name") == name), None)
    if existing:
        return existing["id"]
    return api.call("POST", "/api/collection", {
        "name": name, "color": "#509EE2",
        "description": "Воспроизводимые вопросы и дашборды жизненного цикла GitHub Issues",
    })["id"]


def _tag(name, field_id, base_type):
    return {"id": str(uuid.uuid5(uuid.NAMESPACE_URL, f"github-issues-cdm:{name}")),
            "name": name, "display-name": name.replace("_", " ").capitalize(),
            "type": "dimension", "dimension": ["field", field_id, {"base-type": base_type}],
            "widget-type": "date/all-options" if base_type == "type/Date" else "category"}


CARDS = (
    ("Новые Issues по месяцам", "bar", "Отчётная дата — дата создания Issue в UTC; крайние месяцы диапазона могут быть неполными.",
     """SELECT date_trunc('month', cdm.issue_day.report_day)::date AS month,
               count(*) FILTER (WHERE cdm.issue_day.is_new) AS new_issues
        FROM cdm.issue_day
        JOIN cdm.repository_snapshot ON cdm.repository_snapshot.repository_id = cdm.issue_day.repository_id
        WHERE 1=1 [[AND {{repository}}]] [[AND {{report_day}}]]
        GROUP BY 1 ORDER BY 1""", ("repository", "report_day")),
    ("Открытые Issues на конец дня", "line", "Исторический остаток непосредственно перед следующей полуночью UTC.",
     """SELECT cdm.issue_day.report_day,
               count(*) FILTER (WHERE cdm.issue_day.status_eod='open') AS open_issues
        FROM cdm.issue_day
        JOIN cdm.repository_snapshot ON cdm.repository_snapshot.repository_id = cdm.issue_day.repository_id
        WHERE 1=1 [[AND {{repository}}]] [[AND {{report_day}}]]
        GROUP BY 1 ORDER BY 1""", ("repository", "report_day")),
    ("Доля закрытых на конец дня", "line", "Закрытые / все созданные к этому дню; числители и знаменатели суммируются по выбранным репозиториям.",
     """SELECT cdm.issue_day.report_day,
               count(*) FILTER (WHERE cdm.issue_day.status_eod='closed')::numeric
                 / nullif(count(*), 0) AS closed_share
        FROM cdm.issue_day
        JOIN cdm.repository_snapshot ON cdm.repository_snapshot.repository_id = cdm.issue_day.repository_id
        WHERE 1=1 [[AND {{repository}}]] [[AND {{report_day}}]]
        GROUP BY 1 ORDER BY 1""", ("repository", "report_day")),
    ("Медиана времени первого закрытия", "line", "Накопительная медиана индивидуальных сроков в днях среди впервые закрытых к концу дня.",
     """SELECT cdm.issue_day.report_day,
               percentile_cont(0.5) WITHIN GROUP
                 (ORDER BY cdm.issue_day.first_close_days)
                 FILTER (WHERE cdm.issue_day.ever_closed) AS median_days
        FROM cdm.issue_day
        JOIN cdm.repository_snapshot ON cdm.repository_snapshot.repository_id = cdm.issue_day.repository_id
        WHERE 1=1 [[AND {{repository}}]] [[AND {{report_day}}]]
        GROUP BY 1 ORDER BY 1""", ("repository", "report_day")),
    ("Доля повторно открытых", "line", "Когда-либо повторно открытые / когда-либо закрытые к одному срезу; при нулевом знаменателе значения нет.",
     """SELECT cdm.issue_day.report_day,
               count(*) FILTER (WHERE cdm.issue_day.ever_reopened)::numeric
                 / nullif(count(*) FILTER (WHERE cdm.issue_day.ever_closed), 0) AS reopen_share
        FROM cdm.issue_day
        JOIN cdm.repository_snapshot ON cdm.repository_snapshot.repository_id = cdm.issue_day.repository_id
        WHERE 1=1 [[AND {{repository}}]] [[AND {{report_day}}]]
        GROUP BY 1 ORDER BY 1""", ("repository", "report_day")),
    ("Медиана впервые закрытых за месяц", "line", "Медиана Issues, впервые закрытых внутри месяца; дата фильтра — дата первого закрытия в UTC.",
     """SELECT date_trunc('month', cdm.issue_lifecycle.first_closed_day)::date AS month,
               percentile_cont(0.5) WITHIN GROUP
                 (ORDER BY cdm.issue_lifecycle.first_close_days) AS median_days
        FROM cdm.issue_lifecycle
        JOIN cdm.repository_snapshot ON cdm.repository_snapshot.repository_id = cdm.issue_lifecycle.repository_id
        WHERE cdm.issue_lifecycle.first_closed_day <= cdm.repository_snapshot.common_last_day
          [[AND {{repository}}]] [[AND {{first_closed_day}}]]
        GROUP BY 1 ORDER BY 1""", ("repository", "first_closed_day")),
    ("Топ-15 текущих меток", "row", "15 самых частых пар метка–репозиторий текущего снимка. Полный список ниже; один Issue может иметь несколько меток.",
     """SELECT cdm.current_issue_label.label_name || ' · ' || cdm.repository_snapshot.full_name
                  AS label_repository,
               count(DISTINCT cdm.current_issue_label.issue_id) AS issues
        FROM cdm.current_issue_label
        JOIN cdm.repository_snapshot ON cdm.repository_snapshot.repository_id = cdm.current_issue_label.repository_id
        WHERE 1=1 [[AND {{repository}}]]
        GROUP BY cdm.current_issue_label.label_name, cdm.repository_snapshot.full_name
        ORDER BY issues DESC, label_repository
        LIMIT 15""", ("repository",)),
    ("Текущие метки Issues", "table", "Все пары репозиторий–метка. Issue считается один раз внутри метки, но может иметь несколько меток, поэтому сумма строк может превышать число Issues.",
     """SELECT cdm.repository_snapshot.full_name AS repository,
               cdm.current_issue_label.label_name AS label,
               count(DISTINCT cdm.current_issue_label.issue_id) AS issues
        FROM cdm.current_issue_label
        JOIN cdm.repository_snapshot ON cdm.repository_snapshot.repository_id = cdm.current_issue_label.repository_id
        WHERE 1=1 [[AND {{repository}}]]
        GROUP BY 1, 2 ORDER BY issues DESC, 1, 2""", ("repository",)),
    ("Дата текущего снимка меток", "table", "Метки относятся к текущей выбранной публикации каждого репозитория, а не к прошлому отчётному дню.",
     """SELECT cdm.repository_snapshot.full_name AS repository,
               cdm.repository_snapshot.source_batch_finished_at AT TIME ZONE 'UTC' AS snapshot_utc,
               cdm.repository_snapshot.common_last_day
        FROM cdm.repository_snapshot
        WHERE 1=1 [[AND {{repository}}]]
        ORDER BY 1""", ("repository",)),
)

PERCENT_COLUMNS = {
    "Доля закрытых на конец дня": "closed_share",
    "Доля повторно открытых": "reopen_share",
}


def ensure_cards(api, database_id, collection_id, fields):
    items = _list(api.call("GET", f"/api/collection/{collection_id}/items?models=card"))
    existing = {item["name"]: item["id"] for item in items if item.get("model") == "card"}
    ids = {}
    for name, display, description, query, tags in CARDS:
        mapping = {}
        for tag_name in tags:
            table, column, base_type = {
                "repository": ("repository_snapshot", "full_name", "type/Text"),
                "report_day": ("issue_day", "report_day", "type/Date"),
                "first_closed_day": ("issue_lifecycle", "first_closed_day", "type/Date"),
            }[tag_name]
            mapping[tag_name] = _tag(tag_name, fields[(table, column)], base_type)
        percent_column = PERCENT_COLUMNS.get(name)
        visualization_settings = (
            {"column_settings": {
                json.dumps(["name", percent_column], ensure_ascii=False):
                    {"number_style": "percent", "decimals": 1}
            }} if percent_column else {}
        )
        payload = {
            "name": name, "description": description, "collection_id": collection_id,
            "type": "question", "display": display,
            "visualization_settings": visualization_settings,
            "dataset_query": {"database": database_id, "type": "native",
                              "native": {"query": query, "template-tags": mapping}},
        }
        if name in existing:
            api.call("PUT", f"/api/card/{existing[name]}", payload)
            ids[name] = existing[name]
        else:
            ids[name] = api.call("POST", "/api/card", payload)["id"]
    return ids


def _mapping(parameter_id, card_id, tag):
    return {"parameter_id": parameter_id, "card_id": card_id,
            "target": ["dimension", ["template-tag", tag]]}


def ensure_dashboards(api, collection_id, card_ids):
    known = {item["name"]: item["id"] for item in _list(api.call("GET", "/api/dashboard"))
             if item.get("collection_id") == collection_id}
    definitions = (
        ("Жизненный цикл Issues", [item[0] for item in CARDS[:5]],
         "Динамика по полным UTC-дням; временной фильтр относится к отчётной дате.", "report_day"),
        ("Первое закрытие по месяцам", [CARDS[5][0]],
         "Месячная медиана; временной фильтр относится к дате первого закрытия.", "first_closed_day"),
        ("Текущие метки Issues", [CARDS[6][0], CARDS[7][0], CARDS[8][0]],
         "Топ-15 и полный список меток текущего снимка. Один Issue может иметь несколько меток; исторический фильтр даты неприменим.", None),
    )
    result = {}
    for name, card_names, description, date_tag in definitions:
        if name in known:
            dashboard_id = known[name]
            before = api.call("GET", f"/api/dashboard/{dashboard_id}")
            old = {item.get("card_id"): item["id"] for item in before.get("dashcards", [])}
        else:
            dashboard_id = api.call("POST", "/api/dashboard", {
                "name": name, "description": description, "collection_id": collection_id})["id"]
            old = {}
        parameters = [{"id": "repository", "name": "Репозиторий",
                       "slug": "repository", "type": "string/="}]
        if date_tag:
            parameters.append({"id": "date", "name": "Период UTC",
                               "slug": "date", "type": "date/all-options"})
        dashcards = []
        for index, card_name in enumerate(card_names):
            card_id = card_ids[card_name]
            mappings = [_mapping("repository", card_id, "repository")]
            if date_tag:
                mappings.append(_mapping("date", card_id, date_tag))
            if name == "Текущие метки Issues":
                row, col, width, height = ((0, 0, 24, 12), (13, 0, 24, 12),
                                           (26, 0, 24, 5))[index]
            else:
                row, col, width, height = (index // 2) * 7, (index % 2) * 12, 12, 6
            dashcards.append({
                "id": old.get(card_id, -index - 1), "card_id": card_id,
                "row": row, "col": col,
                "size_x": width, "size_y": height, "parameter_mappings": mappings,
            })
        api.call("PUT", f"/api/dashboard/{dashboard_id}", {
            "name": name, "description": description, "collection_id": collection_id,
            "parameters": parameters, "dashcards": dashcards,
        })
        result[name] = dashboard_id
    return result


def main():
    values = settings()
    api = Metabase(f"http://127.0.0.1:{values.get('METABASE_PORT', '3000')}")
    api.wait_healthy()
    api.authenticate(values)
    database_id = ensure_database(api, values)
    fields = field_ids(api, database_id)
    collection_id = ensure_collection(api)
    cards = ensure_cards(api, database_id, collection_id, fields)
    dashboards = ensure_dashboards(api, collection_id, cards)
    print(json.dumps({"database_id": database_id, "collection_id": collection_id,
                      "dashboards": dashboards}, ensure_ascii=False))


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"Metabase provisioning failed: {exc}", file=sys.stderr)
        raise SystemExit(1)
