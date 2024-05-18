import os
import sqlite3
import requests
import shutil
import uuid
from datetime import date, datetime, timedelta
from dotenv import load_dotenv
from pathlib import Path


SQLITE_FILENAME = "stats.db"
STATS_DIR = "../hugo/content/stats"


def connect_to_db() -> sqlite3.Connection | None:
    print("Connecting to DB ...")
    connection = None

    try:
        connection = sqlite3.connect(SQLITE_FILENAME)
        print("Connected to DB.")

    except sqlite3.Error as e:
        print(e)

    return connection


def update_about_table(connection: sqlite3.Connection):
    sql_statements = [
        """
        CREATE TABLE IF NOT EXISTS about (
            name TEXT PRIMARY KEY NOT NULL, 
            value TEXT NOT NULL
        );""",
        "REPLACE INTO about (name, value) VALUES ('version', '1');",
        f"REPLACE INTO about (name, value) VALUES ('last_modified', '{
            datetime.now()}');"
    ]

    cursor = connection.cursor()

    for statement in sql_statements:
        cursor.execute(statement)

    connection.commit()


def create_repo_table(connection: sqlite3.Connection, repos: list[str]):
    sql_statements = [
        """
        CREATE TABLE IF NOT EXISTS repositories (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL
        );"""
    ]

    for repo in repos:
        if get_repo_id(connection, repo) == None:
            sql_statements.append(
                f"""
                INSERT INTO repositories (name)
                VALUES ('{repo}');
                """
            )

    cursor = connection.cursor()

    for statement in sql_statements:
        cursor.execute(statement)

    connection.commit()


def get_repo_id(connection: sqlite3.Connection, repo: str) -> int | None:
    cursor = connection.cursor()

    try:
        cursor.execute(
            f"""
            SELECT id
            FROM repositories
            WHERE name = '{repo}'
            LIMIT 1;
            """
        )
    except sqlite3.OperationalError:
        return None

    res = cursor.fetchone()

    if res == None:
        return None

    return res[0]


def fetch_all_data(connection: sqlite3.Connection, repos: list[str]):
    token = os.getenv("GITHUB_TOKEN")

    if token == None:
        print("Error: GitHub Access Token not found!")
        return

    for repo in repos:
        releases = fetch_releases(repo, token)
        print(releases)
        save_releases(connection, releases, repo,
                      get_current_day().isoformat())

        views = fetch_views(repo, token)
        save_views(connection, views, repo)


def fetch_releases(repo, token) -> list:
    print("Fetching releases for", repo)

    (owner, repo_name) = split_owner_and_repo(repo)
    query = f"{{repository(owner:\"{owner}\",name:\"{repo_name}\"){{\
                releases(last:99,orderBy:{{direction:DESC,field:CREATED_AT}}){{\
                nodes{{name isPrerelease createdAt author{{login}}\
                releaseAssets(last:99){{nodes{{name downloadCount}}}}}}}}}}}}"

    url = "https://api.github.com/graphql"
    headers = {"Accept": "application/vnd.github+json",
               "Authorization": 'Bearer ' + token}

    request = requests.post(url, headers=headers, json={"query": query})

    if not request.ok:
        print(request.status_code, request.text)
        return []

    return request.json()["data"]["repository"]["releases"]["nodes"]


def save_releases(connection: sqlite3.Connection, releases: dict, repo: str, day: str):
    if len(releases) == 0:
        return

    sql_statements = [
        "PRAGMA foreign_keys = ON;",
        """
        CREATE TABLE IF NOT EXISTS releases (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            is_prerelease INTEGER NOT NULL,
            author TEXT NOT NULL,
            created_at TEXT NOT NULL,
            repo_id INTEGER NOT NULL,
            FOREIGN KEY (repo_id) REFERENCES repositories (id)
            UNIQUE(created_at, repo_id)
        );""",
        """
        CREATE TABLE IF NOT EXISTS assets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            release_id INTEGER NOT NULL,
            FOREIGN KEY (release_id) REFERENCES releases (id)
            UNIQUE(name, release_id)
        );""",
        """
        CREATE TABLE IF NOT EXISTS downloads (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            asset_id INTEGER NOT NULL,
            count INTEGER NOT NULL,
            FOREIGN KEY (asset_id) REFERENCES assets (id)
            UNIQUE(timestamp, asset_id)
        );"""
    ]

    repo_id = get_repo_id(connection, repo)

    for release in releases:
        sql_statements.append(
            f"""
            INSERT INTO releases (name, is_prerelease, author, created_at, repo_id)
            VALUES(
                '{release["name"]}',
                {release["isPrerelease"]},
                '{release["author"]["login"]}',
                '{release["createdAt"]}',
                {repo_id})
            ON CONFLICT (created_at, repo_id) DO UPDATE SET
                name = excluded.name,
                is_prerelease = excluded.is_prerelease,
                author = excluded.author,
                created_at = excluded.created_at;
            """
        )

        for asset in release["releaseAssets"]["nodes"]:
            sql_statements.append(
                f"""
                INSERT INTO assets (name, release_id)
                VALUES(
                    '{asset["name"]}',
                    (SELECT id FROM releases WHERE created_at = '{release["createdAt"]}' AND repo_id = {repo_id}))
                ON CONFLICT (name, release_id) DO UPDATE SET
                    name = excluded.name;
                """
            )

            sql_statements.append(
                f"""
                INSERT INTO downloads (timestamp, asset_id, count)
                VALUES(
                    '{day}',
                    (SELECT id FROM assets WHERE name = '{asset["name"]}'
                        AND release_id = (SELECT id FROM releases WHERE created_at = '{release["createdAt"]}' AND repo_id = {repo_id})),
                    {asset["downloadCount"]})
                ON CONFLICT (timestamp, asset_id) DO UPDATE SET
                    count = excluded.count;
                """
            )

    cursor = connection.cursor()

    for statement in sql_statements:
        cursor.execute(statement)

    connection.commit()


def fetch_views(repo, token) -> dict:
    print("Fetching views for", repo)

    url = "https://api.github.com/repos/" + repo + "/traffic/views"
    params = {"per": "week"}
    headers = {"Accept": "application/vnd.github+json",
               "Authorization": 'Bearer ' + token}

    request = requests.get(url, params=params, headers=headers)

    if not request.ok:
        print(request.status_code, request.text)
        return {}

    print(request.json())
    return request.json()


def save_views(connection: sqlite3.Connection, views: dict, repo: str):
    sql_statements = [
        "PRAGMA foreign_keys = ON;",
        """
        CREATE TABLE IF NOT EXISTS views (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            repo_id INTEGER NOT NULL,
            count INTEGER NOT NULL,
            uniques INTEGER NOT NULL,
            FOREIGN KEY (repo_id) REFERENCES repositories (id),
            UNIQUE(timestamp, repo_id)
        );"""
    ]

    repo_id = get_repo_id(connection, repo)

    for data in views["views"]:
        sql_statements.append(
            f"""
            INSERT INTO views (timestamp, repo_id, count, uniques)
            VALUES(
                '{data["timestamp"]}',
                {repo_id}, {
                data["count"]},
                {data["uniques"]})
            ON CONFLICT(timestamp, repo_id) DO UPDATE SET
                count = excluded.count,
                uniques = excluded.uniques;
            """
        )

    if len(views["views"]) == 0:
        sql_statements.append(
            f"""
            INSERT OR IGNORE INTO views (timestamp, repo_id, count, uniques)
            VALUES('{get_start_of_week().isoformat()}', {repo_id}, 0, 0);
            """
        )

    cursor = connection.cursor()

    for statement in sql_statements:
        cursor.execute(statement)

    connection.commit()


def delete_stats_dir():
    shutil.rmtree(STATS_DIR)


def create_stats_dir():
    Path(STATS_DIR).mkdir(parents=True, exist_ok=True)


def create_owner_dir(owner: str, owner_clean: str) -> Path:
    dir_path = Path(STATS_DIR).joinpath(owner_clean)
    dir_path.mkdir(parents=True, exist_ok=True)

    index_path = dir_path.joinpath("_index.md")

    with open(index_path, "w") as index_file:
        index_file.write(f"+++\ntitle = \"{owner}\"\n+++\n")

    return dir_path


def generate_all_pages(connection: sqlite3.Connection, repos: list[str]):
    delete_stats_dir()

    for repo in repos:
        generate_page(connection, repo)


def generate_tabs(content: dict) -> str:
    res = f"{{{{< tabs \"{uuid.uuid4()}\" >}}}}\n"

    for key in content:
        res += generate_tab(key, content[key])

    res += "{{< /tabs >}}\n"
    return res


def generate_tab(title: str, content: str) -> str:
    return f"{{{{< tab \"{title}\" >}}}}\n{content}\n{{{{< /tab >}}}}\n"


def generate_hint(type: str, content: str) -> str:
    return f"{{{{< hint {type} >}}}}\n{content}\n{{{{< /hint >}}}}\n"


def generate_page(connection: sqlite3.Connection, repo: str):
    print(f"Generating page for {repo} ...")

    create_stats_dir()

    (owner, repo_name) = split_owner_and_repo(repo)
    owner_dir_name = owner.replace("-", "_")
    owner_path = create_owner_dir(owner, owner_dir_name)

    repo_id = get_repo_id(connection, repo)

    with open(owner_path.joinpath(repo_name + ".md"), "w") as file:
        file.write(f"""
+++
title = \"{repo_name}\"
+++

# {repo_name}

[![total downloads](https://img.shields.io/github/downloads/{repo}/total.svg?style=flat-square)](https://github.com/{repo}/releases/)
[![forks](https://img.shields.io/github/forks/{repo}.svg?style=flat-square)](https://github.com/{repo}/network/)
[![stars](https://img.shields.io/github/stars/{repo}.svg?style=flat-square)](https://github.com/{repo}/stargazers/)
[![watchers](https://img.shields.io/github/watchers/{repo}.svg?style=flat-square)](https://github.com/{repo}/watchers/)

## Views
{generate_view_chart(connection, repo_id)}

## Releases
{generate_release_charts(connection, repo_id)}
""")


def generate_view_chart(connection: sqlite3.Connection, repo_id: int) -> str:
    cursor = connection.cursor()
    cursor.execute(
        f"""
        SELECT timestamp, count, uniques
        FROM views
        WHERE repo_id = {repo_id}
        ORDER BY id ASC;
        """
    )

    res = cursor.fetchall()

    labels = ",".join(map(lambda d: f"\"{d[0]}\"", res))
    data_count = ",".join(map(lambda d: f"{d[1]}", res))
    data_unique = ",".join(map(lambda d: f"{d[2]}", res))

    return generate_line_chart(labels, {"Count": data_count, "Unique": data_unique})


def generate_line_chart(labels: str, datasets: dict) -> str:
    chart = f"""
{{{{< chart >}}}}
{{
    "type": "line",
    "data": {{
        "labels": [{labels}],
        "datasets": ["""

    for key in datasets.keys():
        chart += f"{{\"label\": \"{
            key}\", \"pointStyle\": false, \"data\": [{datasets[key]}] }},"

    if len(datasets) > 0:
        chart = chart[:-1]

    chart += "]"
    chart += """
    },
    "options": {
        "animation": false,
        "interaction": { "intersect": false, "mode": "index" },
        "plugins": { "decimation": { "enabled": true, "algorithm": "min-max" } },
        "maintainAspectRatio": false,
        "scales": {
            "x": { "type": "time", "time": { "unit": "day" } },
            "y": { "suggestedMin": 0 }
        }
    }
}
{{< /chart >}}"""
    return chart


def generate_release_charts(connection: sqlite3.Connection, repo_id: int) -> str:
    cursor = connection.cursor()
    cursor.execute(
        f"""
        SELECT id, name, created_at, author
        FROM releases
        WHERE repo_id = {repo_id}
        ORDER BY created_at DESC;
        """
    )
    releases = cursor.fetchall()

    if len(releases) == 0:
        return generate_hint("warning", "This repository contains no releases.\n\n")

    charts = ""

    for release in releases:
        charts += f"### {release[1]}\nDate: {release[2]
                                             }  \nAuthor: {release[3]}\n"

        cursor.execute(
            f"""
            SELECT id, name
            FROM assets
            WHERE release_id = {release[0]}
            ORDER BY id ASC;
            """
        )
        assets = cursor.fetchall()

        asset_labels = ""

        for asset in assets:
            asset_labels += f"\"{asset[1]}\","

        asset_labels = asset_labels[:-1]

        charts += generate_tabs({
            "Over Time": generate_release_line_chart(connection, assets),
            "Total": generate_release_bar_chart(connection, release[0], assets, asset_labels)
        })

    return charts


def generate_release_line_chart(connection: sqlite3.Connection, assets: list) -> str:
    cursor = connection.cursor()

    asset_ids = ",".join(map(lambda a: str(a[0]), assets))

    cursor.execute(
        f"""
        SELECT DISTINCT timestamp
        FROM downloads
        WHERE asset_id IN ({asset_ids})
        ORDER BY timestamp ASC;
        """
    )

    timestamps = cursor.fetchall()
    timestamp_labels = ",".join(map(lambda t: f"\"{t[0]}\"", timestamps))

    data = {}

    for asset in assets:
        cursor.execute(
            f"""
            SELECT count
            FROM downloads
            WHERE asset_id = {asset[0]}
            ORDER BY timestamp ASC;
            """
        )
        counts = cursor.fetchall()
        counts_clean = ",".join(map(lambda c: str(c[0]), counts))
        data[asset[1]] = counts_clean

    return generate_line_chart(timestamp_labels, data)


def generate_release_bar_chart(connection: sqlite3.Connection, release_id: int, assets: list, asset_labels: str) -> str:
    cursor = connection.cursor()
    asset_ids = ",".join(map(lambda a: str(a[0]), assets))
    cursor.execute(
        f"""
        SELECT timestamp
        FROM downloads
        WHERE asset_id IN ({asset_ids})
        ORDER BY timestamp DESC
        LIMIT 1;
        """
    )
    newest_timestamp = cursor.fetchone()[0]

    cursor.execute(
        f"""
        SELECT l.count
        FROM downloads l
        INNER JOIN assets r ON r.id = l.asset_id
        WHERE l.timestamp = '{newest_timestamp}' AND r.release_id = {release_id}
        ORDER BY r.id ASC;
        """
    )
    downloads = cursor.fetchall()
    download_counts = ",".join(map(lambda d: str(d[0]), downloads))

    return f"""
{{{{< chart >}}}}
{{
    "type": "bar",
    "data": {{
        "labels": [{asset_labels}],
        "datasets": [
            {{
                "label": "Downloads",
                "data": [{download_counts}]
            }}
        ]
    }},
    "options": {{
        "maintainAspectRatio": false,
        "scales": {{ "y": {{ "suggestedMin": 0 }} }}
    }}
}}
{{{{< /chart >}}}}\n\n"""


def optimize_db(connection: sqlite3.Connection):
    print("Optimizing Database Tables ...")

    release_ids = get_all_release_ids(connection)

    cursor = connection.cursor()

    for release_id in release_ids:
        print(f"\tOptimizing Release with ID {release_id} ...")

        cursor.execute(
            f"""
            SELECT timestamp, sum(count)
            FROM downloads
            WHERE asset_id IN (SELECT id FROM assets WHERE release_id = {release_id})
            GROUP BY timestamp ORDER BY timestamp ASC;
            """
        )
        res = cursor.fetchall()
        timestamps_to_remove = []

        for i in range(1, len(res)):
            count = res[i][1]
            if i+1 < len(res) and count == res[i+1][1] and count == res[i-1][1]:
                timestamps_to_remove.append(res[i][0])

        cursor.execute(
            f"""
            DELETE FROM downloads
            WHERE timestamp IN ({",".join(map(lambda t: f"'{t}'", timestamps_to_remove))})
                AND asset_id IN (SELECT id FROM assets WHERE release_id = {release_id});
            """
        )

    connection.commit()


def get_all_release_ids(connection: sqlite3.Connection) -> list[int]:
    cursor = connection.cursor()
    cursor.execute("SELECT id FROM releases;")
    res = cursor.fetchall()
    return list(map(lambda r: r[0], res))


def get_current_day():
    current = datetime.now()
    return date(current.year, current.month, current.day)


def get_start_of_week():
    current_day = get_current_day()
    return current_day - timedelta(days=current_day.weekday())


def split_owner_and_repo(repo) -> tuple[str, str]:
    sep = repo.split("/")
    return (sep[0], sep[1])


def main():
    print("Updating statistics ...")

    load_dotenv()

    repos = os.getenv("REPOSITORIES")
    repos = repos.split(",")

    if repos == None or len(repos) == 0:
        print("No repositories configured.")
        return

    print("Repositories:", repos)

    connection = connect_to_db()

    if connection == None:
        return

    update_about_table(connection)

    create_repo_table(connection, repos)
    get_repo_id(connection, "test")
    get_repo_id(connection, "philinthegaps/gm-companion")

    fetch_all_data(connection, repos)
    optimize_db(connection)
    generate_all_pages(connection, repos)

    connection.close()
    print("Done.")


if __name__ == '__main__':
    main()
