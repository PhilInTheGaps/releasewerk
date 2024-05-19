from datetime import date, datetime, timedelta
from github import GitHubRepo
import sqlite3

CURRENT_VERSION = 2


class Database():
    def __enter__(self):
        self._connection = None
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.disconnect()

    def connect(self, db_file: str) -> bool:
        print(f"Connecting to DB {db_file} ...")

        try:
            self._connection = sqlite3.connect(db_file)
            print("Connected to DB.")
            return True

        except sqlite3.Error as e:
            print(e)
            return False

    def disconnect(self):
        if self._connection == None:
            return

        print("Closing DB connection ...")
        self._connection.close()

    def update_tables(self):
        version = self._get_version()

        if version == 1:
            self._update_table_repositories_to_v2()

        self._create_tables()
        self._update_about()

    def _update_about(self):
        cursor = self._connection.cursor()
        cursor.execute(
            "REPLACE INTO about (name, value) VALUES ('version', ?);", [str(CURRENT_VERSION)])
        cursor.execute(
            "REPLACE INTO about (name, value) VALUES ('last_modified', ?);", [str(datetime.now())])
        self._connection.commit()

    def _get_version(self) -> int:
        try:
            cursor = self._connection.execute(
                "SELECT value FROM about WHERE name = 'version' LIMIT 1;")
            return int(cursor.fetchone()[0])
        except Exception:
            return CURRENT_VERSION

    def _create_tables(self):
        self._enable_foreign_keys()

        self._create_table_about()
        self._create_table_repositories()
        self._create_table_views()
        self._create_table_releases()
        self._create_table_assets()
        self._create_table_downloads()

        self._connection.commit()

    def _enable_foreign_keys(self):
        self._connection.execute("PRAGMA foreign_keys = ON;")
        self._connection.commit()

    def _create_table_about(self):
        self._connection.execute("""
            CREATE TABLE IF NOT EXISTS about (
                name TEXT PRIMARY KEY NOT NULL, 
                value TEXT NOT NULL
            );""")

    def _create_table_repositories(self):
        self._connection.execute("""
            CREATE TABLE IF NOT EXISTS repositories (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE
            );""")

    def _update_table_repositories_to_v2(self):
        print("Updating table 'repositories' ...")
        cursor = self._connection.cursor()

        cursor.execute(
            "CREATE TEMP TABLE temp_repos AS SELECT * FROM repositories;")
        cursor.execute("DROP TABLE repositories;")
        self._create_table_repositories()
        cursor.execute(
            "UPDATE repositories SET id = (SELECT id FROM temp_repos), name = (SELECT name FROM temp_repos);")

        self._connection.commit()

    def add_repositories(self, repos: list[GitHubRepo]):
        self._connection.executemany(
            "INSERT OR IGNORE INTO repositories (name) VALUES (?);", [(str(repo), ) for repo in repos])
        self._connection.commit()

    def get_repo_id(self, repo: GitHubRepo) -> int | None:
        try:
            cursor = self._connection.execute(f"""
                SELECT id
                FROM repositories
                WHERE name = '{str(repo)}'
                LIMIT 1;
                """)
            return cursor.fetchone()[0]

        except sqlite3.OperationalError:
            return None

    def set_repo_ids(self, repos: list[GitHubRepo]):
        for repo in repos:
            repo.db_id = self.get_repo_id(repo)

    def _create_table_views(self):
        self._connection.execute("""
            CREATE TABLE IF NOT EXISTS views (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                repo_id INTEGER NOT NULL,
                count INTEGER NOT NULL,
                uniques INTEGER NOT NULL,
                FOREIGN KEY (repo_id) REFERENCES repositories (id),
                UNIQUE(timestamp, repo_id)
            );""")

    def add_views(self, repo: GitHubRepo, data: dict):
        data["repoId"] = repo.db_id
        self._connection.execute("""
            INSERT INTO views (timestamp, repo_id, count, uniques)
            VALUES(:timestamp, :repoId, :count, :uniques)
            ON CONFLICT(timestamp, repo_id) DO UPDATE SET
                count = excluded.count,
                uniques = excluded.uniques;
            """, data)
        self._connection.commit()

    def add_views_zero(self, repo: GitHubRepo, day: str):
        self._connection.execute("""
            INSERT OR IGNORE INTO views (timestamp, repo_id, count, uniques)
            VALUES(?, ?, 0, 0);
            """, [day, repo.db_id])
        self._connection.commit()

    def get_views(self, repo: GitHubRepo) -> list[sqlite3.Row]:
        cursor = self._connection.cursor()
        cursor.row_factory = sqlite3.Row
        cursor.execute("""
            SELECT timestamp, count, uniques
            FROM views
            WHERE repo_id = ?
            ORDER BY id ASC;
            """, [repo.db_id])
        return cursor.fetchall()

    def _create_table_releases(self):
        self._connection.execute("""
            CREATE TABLE IF NOT EXISTS releases (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                is_prerelease INTEGER NOT NULL,
                author TEXT NOT NULL,
                created_at TEXT NOT NULL,
                repo_id INTEGER NOT NULL,
                FOREIGN KEY (repo_id) REFERENCES repositories (id)
                UNIQUE(created_at, repo_id)
            );""")

    def get_releases(self, repo: GitHubRepo) -> list[sqlite3.Row]:
        cursor = self._connection.cursor()
        cursor.row_factory = sqlite3.Row
        cursor.execute("""
            SELECT id, name, created_at, author
            FROM releases
            WHERE repo_id = ?
            ORDER BY created_at DESC;
            """, [repo.db_id])
        return cursor.fetchall()

    def add_release(self, repo: GitHubRepo, release: dict, day: str):
        release["username"] = release["author"]["login"]
        release["repoId"] = repo.db_id

        self._connection.execute("""
            INSERT INTO releases (name, is_prerelease, author, created_at, repo_id)
            VALUES(:name, :isPrerelease, :username, :createdAt, :repoId)
            ON CONFLICT (created_at, repo_id) DO UPDATE SET
                name = excluded.name,
                is_prerelease = excluded.is_prerelease,
                author = excluded.author,
                created_at = excluded.created_at;
            """, release)

        for asset in release["releaseAssets"]["nodes"]:
            self._add_asset(repo, release, asset)
            self._add_download_data(repo, release, asset, day)

        self._connection.commit()

    def _get_all_release_ids(self) -> list[int]:
        cursor = self._connection.execute("SELECT id FROM releases;")
        res = cursor.fetchall()
        return list(map(lambda r: r[0], res))

    def _create_table_assets(self):
        self._connection.execute("""
            CREATE TABLE IF NOT EXISTS assets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                release_id INTEGER NOT NULL,
                FOREIGN KEY (release_id) REFERENCES releases (id)
                UNIQUE(name, release_id)
            );""")

    def _add_asset(self, repo: GitHubRepo, release: dict, asset: dict):
        self._connection.execute("""
            INSERT INTO assets (name, release_id)
            VALUES(?, (SELECT id FROM releases WHERE created_at = ? AND repo_id = ?))
            ON CONFLICT (name, release_id) DO UPDATE SET name = excluded.name;
            """, [asset["name"], release["createdAt"], repo.db_id])

    def get_assets(self, release_id: int) -> list[sqlite3.Row]:
        cursor = self._connection.cursor()
        cursor.row_factory = sqlite3.Row
        cursor.execute("""
            SELECT id, name
            FROM assets
            WHERE release_id = ?
            ORDER BY id ASC;
            """, [release_id])
        return cursor.fetchall()

    def _create_table_downloads(self):
        self._connection.execute("""
            CREATE TABLE IF NOT EXISTS downloads (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                asset_id INTEGER NOT NULL,
                count INTEGER NOT NULL,
                FOREIGN KEY (asset_id) REFERENCES assets (id)
                UNIQUE(timestamp, asset_id)
            );""")

    def _add_download_data(self, repo: GitHubRepo, release: dict, asset: dict, day: str):
        self._connection.execute("""
            INSERT INTO downloads (timestamp, asset_id, count)
            VALUES (?, 
                (SELECT id FROM assets WHERE name = ? AND 
                    release_id = (SELECT id FROM releases WHERE created_at = ? AND repo_id = ?)),
                ?)
            ON CONFLICT (timestamp, asset_id) DO UPDATE SET count = excluded.count;
            """, [day, asset["name"], release["createdAt"], repo.db_id, asset["downloadCount"]])

    def get_all_download_timestamps(self, assets: list[sqlite3.Row]) -> list[sqlite3.Row]:
        cursor = self._connection.cursor()
        cursor.row_factory = sqlite3.Row
        cursor.execute(f"""
            SELECT DISTINCT timestamp
            FROM downloads
            WHERE asset_id IN ({",".join(map(lambda a: str(a["id"]), assets))})
            ORDER BY timestamp ASC;
            """)
        return cursor.fetchall()

    def _get_newest_download_timestamp(self, assets: list[sqlite3.Row]) -> str | None:
        cursor = self._connection.cursor()
        cursor.row_factory = sqlite3.Row
        cursor.execute(f"""
            SELECT timestamp
            FROM downloads
            WHERE asset_id IN ({",".join(map(lambda a: str(a["id"]), assets))})
            ORDER BY timestamp DESC
            LIMIT 1;
            """)
        res = cursor.fetchone()
        return res["timestamp"] if res != None else None

    def get_newest_download_counts(self, release_id: int, assets: list[sqlite3.Row]):
        newest_timestamp = self._get_newest_download_timestamp(assets)
        if newest_timestamp == None:
            return []

        cursor = self._connection.cursor()
        cursor.row_factory = sqlite3.Row
        cursor.execute("""
            SELECT l.count
            FROM downloads l
            INNER JOIN assets r ON r.id = l.asset_id
            WHERE l.timestamp = ? AND r.release_id = ?
            ORDER BY r.id ASC;
            """, [newest_timestamp, release_id])
        return cursor.fetchall()

    def get_download_counts(self, asset: sqlite3.Row) -> list[sqlite3.Row]:
        cursor = self._connection.cursor()
        cursor.row_factory = sqlite3.Row
        cursor.execute("""
            SELECT count
            FROM downloads
            WHERE asset_id = ?
            ORDER BY timestamp ASC;
            """, [asset["id"]])
        return cursor.fetchall()

    def optimize(self):
        print("Optimizing DB tables ...")

        release_ids = self._get_all_release_ids()

        cursor = self._connection.cursor()
        cursor.row_factory = sqlite3.Row

        for release_id in release_ids:
            print(f"\tOptimizing release with ID {release_id} ...")

            cursor.execute("""
                SELECT timestamp, sum(count) AS count_sum
                FROM downloads
                WHERE asset_id IN (SELECT id FROM assets WHERE release_id = ?)
                GROUP BY timestamp ORDER BY timestamp ASC;
                """, [release_id])

            timestamps_to_remove = self._find_obsolete_download_count_timestamps(
                cursor.fetchall())

            cursor.execute(f"""
                DELETE FROM downloads
                WHERE timestamp IN ({",".join(map(lambda t: f"'{t}'", timestamps_to_remove))}) AND asset_id IN (SELECT id FROM assets WHERE release_id = ?);
                """, [release_id])

        self._connection.commit()

    def _find_obsolete_download_count_timestamps(self, data: list[sqlite3.Row]) -> list[str]:
        obsolete_timestamps = []

        for i in range(1, len(data)):
            count = data[i]["count_sum"]
            if i+1 < len(data) and count == data[i+1]["count_sum"] and count == data[i-1]["count_sum"]:
                obsolete_timestamps.append(data[i]["timestamp"])

        return obsolete_timestamps
