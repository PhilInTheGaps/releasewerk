import inspect
import os
import sqlite3
import shutil
from datetime import date, datetime, timedelta
from dotenv import load_dotenv
from pathlib import Path
import markdown as md
from database import Database
from github import GitHubRepo, GitHubConnector

SQLITE_FILENAME = "stats.db"
STATS_DIR = "../hugo/content/stats"


def delete_stats_dir():
    shutil.rmtree(STATS_DIR)


def create_stats_dir():
    Path(STATS_DIR).mkdir(parents=True, exist_ok=True)


def create_owner_dir(repo: GitHubRepo) -> Path:
    owner_clean = repo.owner.replace("-", "_")
    dir_path = Path(STATS_DIR).joinpath(owner_clean)
    dir_path.mkdir(parents=True, exist_ok=True)

    index_path = dir_path.joinpath("_index.md")

    with open(index_path, "w") as index_file:
        index_file.write(f"+++\ntitle = \"{repo.owner}\"\n+++\n")

    return dir_path


def generate_all_pages(db: Database, repos: list[GitHubRepo]):
    delete_stats_dir()
    create_stats_dir()

    for repo in repos:
        generate_page(db, repo)


def generate_page(db: Database, repo: GitHubRepo):
    print(f"Generating page for {repo} ...")

    owner_path = create_owner_dir(repo)

    content = inspect.cleandoc(f"""
+++
title = \"{repo.name}\"
+++

# {repo.name}
{md.generate_repo_badges(repo)}

## Views
{generate_view_chart(db, repo)}

## Releases
{generate_release_charts(db, repo)}
""")

    with open(owner_path.joinpath(f"{repo.name}.md"), "w") as file:
        file.write(content)


def generate_view_chart(db: Database, repo: GitHubRepo) -> str:
    views = db.get_views(repo)

    return md.generate_line_chart(views, "timestamp",
                                  {"Count": md.make_db_list_str(views, "count", False),
                                   "Unique": md.make_db_list_str(views, "uniques", False)})


def generate_release_charts(db: Database, repo: GitHubRepo) -> str:
    releases = db.get_releases(repo)

    if len(releases) == 0:
        return md.generate_hint("warning", "This repository contains no releases.")

    charts = ""

    for release in releases:
        charts += md.generate_charts_header(release)

        assets = db.get_assets(release["id"])

        charts += md.generate_tabs({
            "Over Time": generate_release_line_chart(db, assets),
            "Total": generate_release_bar_chart(db, release["id"], assets)
        })

    return charts


def generate_release_line_chart(db: Database, assets: list[sqlite3.Row]) -> str:
    timestamps = db.get_all_download_timestamps(assets)

    data = {}

    for asset in assets:
        counts = db.get_download_counts(asset)
        counts_clean = ",".join(map(lambda c: str(c["count"]), counts))
        data[asset["name"]] = counts_clean

    return md.generate_line_chart(timestamps, "timestamp", data)


def generate_release_bar_chart(db: Database, release_id: int, assets: list[sqlite3.Row]) -> str:
    downloads = db.get_newest_download_counts(release_id, assets)
    return md.generate_bar_chart(assets, "name", downloads)


def get_current_day():
    current = datetime.now()
    return date(current.year, current.month, current.day)


def get_start_of_week():
    current_day = get_current_day()
    return current_day - timedelta(days=current_day.weekday())


def main():
    print("Updating statistics ...")
    load_dotenv()

    repos = os.getenv("REPOSITORIES").split(",")
    repos = [GitHubRepo(repo) for repo in repos]

    if len(repos) == 0:
        print("No repositories configured.")
        exit(1)

    print("Watching the following repositories:")
    for repo in repos:
        print(f"\t{repo}")

    gh_token = os.getenv("GITHUB_TOKEN")

    if gh_token == None:
        print("Error: GitHub Access Token not found!")
        exit(1)

    with Database() as db:
        if not db.connect(SQLITE_FILENAME):
            exit(1)

        db.update_tables()
        db.add_repositories(repos)
        db.set_repo_ids(repos)

        gh = GitHubConnector(gh_token)
        today = get_current_day().isoformat()

        # for repo in repos:
        #     releases = gh.get_releases(repo)

        #     for release in releases:
        #         db.add_release(repo, release, today)

        #     views = gh.get_views(repo)

        #     for data in views:
        #         db.add_views(repo, data)

        #     if len(views) == 0:
        #         db.add_views_zero(repo, get_start_of_week().isoformat())

        db.optimize()

        generate_all_pages(db, repos)

    print("Done.")


if __name__ == '__main__':
    main()
