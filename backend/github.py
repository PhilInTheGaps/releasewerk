import requests


class GitHubRepo:
    def __init__(self, repo: str) -> None:
        sep = repo.split("/")
        self._owner = sep[0]
        self._name = sep[1]
        self._dbId = 0

    def __str__(self) -> str:
        return f"{self._owner}/{self._name}"

    @property
    def owner(self) -> str:
        return self._owner

    @property
    def name(self) -> str:
        return self._name

    @property
    def db_id(self) -> int:
        return self._dbId

    @db_id.setter
    def db_id(self, id: int):
        self._dbId = id


class GitHubConnector:
    def __init__(self, access_token: str) -> None:
        self._access_token = access_token

    def _make_headers(self) -> dict:
        return {
            "Authorization": f"Bearer {self._access_token}",
            "Accept": "application/vnd.github+json"
        }

    def get_releases(self, repo: GitHubRepo) -> list[dict]:
        print("Fetching releases for", repo)

        query = f"{{repository(owner:\"{repo.owner}\",name:\"{repo.name}\"){{\
                    releases(last:99,orderBy:{{direction:DESC,field:CREATED_AT}}){{\
                    nodes{{name isPrerelease createdAt author{{login}}\
                    releaseAssets(last:99){{nodes{{name downloadCount}}}}}}}}}}}}"
        url = "https://api.github.com/graphql"
        request = requests.post(
            url, headers=self._make_headers(), json={"query": query})

        if not request.ok:
            print(request.status_code, request.text)
            return []

        return request.json()["data"]["repository"]["releases"]["nodes"]

    def get_views(self, repo: GitHubRepo) -> list[dict]:
        print("Fetching views for", repo)

        url = f"https://api.github.com/repos/{repo}/traffic/views"
        params = {"per": "week"}
        request = requests.get(
            url, params=params, headers=self._make_headers())

        if not request.ok:
            print(request.status_code, request.text)
            return {}

        return request.json()["views"]
