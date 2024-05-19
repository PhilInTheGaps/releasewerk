import requests
import shortuuid


class GitHubRepo:
    def __init__(self, repo: str) -> None:
        sep = repo.split("/")
        self._owner = sep[0]
        self._name = sep[1]
        self._db_id = 0

    def __str__(self) -> str:
        return f"{self._owner}/{self._name}"

    def __eq__(self, __value: object) -> bool:
        return str(self) == str(__value)

    def __hash__(self) -> int:
        return hash(tuple(sorted(self.__dict__.items())))

    @property
    def owner(self) -> str:
        return self._owner

    @property
    def name(self) -> str:
        return self._name

    @property
    def db_id(self) -> int:
        return self._db_id

    @db_id.setter
    def db_id(self, id: int):
        self._db_id = id


class GitHubConnector:
    def __init__(self, access_token: str) -> None:
        self._access_token = access_token
        self._short_uuid = shortuuid.ShortUUID(
            alphabet="abcdefghijklmnopqrstuvwxyz")

    def _make_headers(self) -> dict:
        return {
            "Authorization": f"Bearer {self._access_token}",
            "Accept": "application/vnd.github+json"
        }

    def get_releases(self, repos: list[GitHubRepo]) -> dict:
        print("Fetching releases ...")

        ids = {}
        subqueries = ""

        for repo in repos:
            uuid = self._short_uuid.uuid()
            ids[uuid] = repo
            subqueries += self._make_releases_query(repo, uuid)

        query = f"{{{subqueries}}}"

        url = "https://api.github.com/graphql"
        request = requests.post(
            url, headers=self._make_headers(), json={"query": query})

        if not request.ok:
            print(request.status_code, request.text)
            return {}

        res = request.json()["data"]

        releases = {}

        for uuid in res:
            releases[ids[uuid]] = res[uuid]["releases"]["nodes"]

        return releases

    @staticmethod
    def _make_releases_query(repo: GitHubRepo, uuid: str) -> str:
        return f"{uuid}:repository(owner:\"{repo.owner}\",name:\"{repo.name}\"){{\
                    releases(last:99,orderBy:{{direction:DESC,field:CREATED_AT}}){{\
                    nodes{{name isPrerelease createdAt author{{login}}\
                    releaseAssets(last:99){{nodes{{name downloadCount}}}}}}}}}}"

    def get_views(self, repo: GitHubRepo) -> list[dict]:
        print("Fetching views for", repo, "...")

        url = f"https://api.github.com/repos/{repo}/traffic/views"
        params = {"per": "week"}
        request = requests.get(
            url, params=params, headers=self._make_headers())

        if not request.ok:
            print(request.status_code, request.text)
            return []

        return request.json()["views"]

    def get_repos(self, users: list[str], organisations: list[str]) -> list[GitHubRepo]:
        print("Loading repositories for users and organisations ...")

        subqueries = ""

        for org in organisations:
            subqueries += self._make_org_repos_query(org, self._short_uuid.uuid())

        for user in users:
            subqueries += self._make_user_repos_query(user, self._short_uuid.uuid())

        if len(subqueries) == 0:
            return []

        query = f"{{{subqueries}}}"
        url = "https://api.github.com/graphql"
        request = requests.post(
            url, headers=self._make_headers(), json={"query": query})

        if not request.ok:
            print(request.status_code, request.text)
            return []

        res = request.json()

        repos = []
        for uuid in res["data"]:
            repos += [GitHubRepo(n["nameWithOwner"].lower())
                      for n in res["data"][uuid]["repositories"]["nodes"]]
        return repos

    @staticmethod
    def _make_org_repos_query(org: str, uuid: str) -> str:
        return f"{uuid}:organization(login:\"{org}\"){{repositories(first:99,visibility:PUBLIC){{nodes{{nameWithOwner}}}}}}"

    @staticmethod
    def _make_user_repos_query(user: str, uuid: str) -> str:
        return f"{uuid}:user(login:\"{user}\"){{repositories(first:99,visibility:PUBLIC){{nodes{{nameWithOwner}}}}}}"
