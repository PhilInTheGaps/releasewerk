import argparse
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


def init_argparse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        usage="%(prog)s [option] ...",
        description="Convert data from json files to the new db based approach."
    )

    parser.add_argument("--repos", action="store",
                        nargs="+", help="specify repositories")
    parser.add_argument("--users", action="store",
                        nargs="+", help="specify users to get repositories from")
    parser.add_argument("--orgs", action="store",
                        nargs="+", help="specify organisations to get repositories from")
    parser.add_argument("--fetch-only", action="store_true",
                        help="don't generate the web page")
    parser.add_argument("--generate-only",
                        action="store_true", help="don't fetch data")

    return parser


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


def make_gh_connector() -> GitHubConnector:
    gh_token = os.getenv("RW_GITHUB_TOKEN")

    if gh_token == None:
        print("Error: GitHub Access Token not found!")
        exit(1)

    return GitHubConnector(gh_token)


def fetch_data(db: Database, gh: GitHubConnector, repos: list[GitHubRepo]):
    today = get_current_day().isoformat()

    releases = gh.get_releases(repos)

    for repo in releases:
        for release in releases[repo]:
            db.add_release(repo, release, today)

        views = gh.get_views(repo)

        for data in views:
            db.add_views(repo, data)

        if len(views) == 0:
            db.add_views_zero(repo, get_start_of_week().isoformat())


def get_from_args_or_env(args_value: list[str], env_key: str) -> list[str]:
    if args_value and len(args_value) > 0:
        return args_value

    values = os.getenv(env_key)

    if not values or len(values) == 0:
        return []

    return [value.strip() for value in values.split(",")]


def get_repos(args: argparse.Namespace, gh: GitHubConnector) -> list[GitHubRepo]:
    users = get_from_args_or_env(args.users, "RW_USERS")
    orgs = get_from_args_or_env(args.orgs, "RW_ORGS")
    repos = gh.get_repos(users, orgs)

    repos += [GitHubRepo(repo.lower())
              for repo in get_from_args_or_env(args.repos, "RW_REPOS")]

    if len(repos) == 0:
        print("No repositories configured.")
        exit(1)

    # remove duplicates
    repos = list(set(repos))

    print("Watching the following repositories:")
    for repo in repos:
        print(f"\t{repo}")

    return repos


def main():
    parser = init_argparse()
    args = parser.parse_args()

    print("Updating statistics ...")
    load_dotenv()

    gh = make_gh_connector()
    repos = get_repos(args, gh)

    with Database() as db:
        if not db.connect(SQLITE_FILENAME):
            exit(1)

        db.update_tables()
        db.add_repositories(repos)
        db.set_repo_ids(repos)

        if not args.generate_only:
            fetch_data(db, gh, repos)

        db.optimize()

        if not args.fetch_only:
            generate_all_pages(db, repos)

    print("Done.")


if __name__ == '__main__':
    main()
