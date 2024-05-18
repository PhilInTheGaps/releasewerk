import datetime
import inspect
import uuid
from github import GitHubRepo


def make_db_list_str(data: list[list], index: int | str, with_quotation_marks: bool) -> str:
    if with_quotation_marks:
        return ",".join(map(lambda d: f"\"{d[index]}\"", data))

    return ",".join(map(lambda d: str(d[index]), data))


def _make_db_list_labels_str(data: list[list], index: int | str) -> str:
    return make_db_list_str(data, index, True)


def generate_repo_badges(repo: GitHubRepo) -> str:
    return inspect.cleandoc(f"""
        [![total downloads](https://img.shields.io/github/downloads/{repo}/total.svg?style=flat-square)](https://github.com/{repo}/releases/)
        [![forks](https://img.shields.io/github/forks/{repo}.svg?style=flat-square)](https://github.com/{repo}/network/)
        [![stars](https://img.shields.io/github/stars/{repo}.svg?style=flat-square)](https://github.com/{repo}/stargazers/)
        [![watchers](https://img.shields.io/github/watchers/{repo}.svg?style=flat-square)](https://github.com/{repo}/watchers/)
        """)


def generate_hint(type: str, content: str) -> str:
    return f"{{{{< hint {type} >}}}}\n{content}\n{{{{< /hint >}}}}\n"


def generate_charts_header(release: dict) -> str:
    return inspect.cleandoc(f"""
        ### {release["name"]}
        Date: {datetime.datetime.fromisoformat(release["created_at"]).date().isoformat()}  
        Author: {release["author"]}
        """)


def generate_bar_chart(labels: list, label_index: int | str, data: list) -> str:
    return inspect.cleandoc(f"""
        {{{{< chart >}}}}
        {{
            "type": "bar",
            "data": {{
                "labels": [{_make_db_list_labels_str(labels, label_index)}],
                "datasets": [
                    {{
                        "label": "Downloads",
                        "data": [{make_db_list_str(data, 0, False)}]
                    }}
                ]
            }},
            "options": {{
                "maintainAspectRatio": false,
                "scales": {{ "y": {{ "suggestedMin": 0 }} }}
            }}
        }}
        {{{{< /chart >}}}}\n\n
        """)


def generate_line_chart(labels: list, label_index: int | str, data: dict) -> str:
    return inspect.cleandoc(f"""
        {{{{< chart >}}}}
        {{
            "type": "line",
            "data": {{
                "labels": [{_make_db_list_labels_str(labels, label_index)}],
                "datasets": [{_generate_line_chart_datasets(data)}]
            }},
            "options": {{
                "animation": false,
                "interaction": {{ "intersect": false, "mode": "index" }},
                "plugins": {{ "decimation": {{ "enabled": true, "algorithm": "min-max" }} }},
                "maintainAspectRatio": false,
                "scales": {{
                    "x": {{ "type": "time", "time": {{ "unit": "day" }} }},
                    "y": {{ "suggestedMin": 0 }}
                }}
            }}
        }}
        {{{{< /chart >}}}}
        """)


def _generate_line_chart_datasets(data: dict) -> str:
    if len(data) == 0:
        return ""

    sets = []

    for key in data:
        sets.append(
            f"{{\"label\": \"{key}\", \"pointStyle\": false, \"data\": [{data[key]}]}}")

    return ",".join(sets)


def generate_tabs(content: dict) -> str:
    res = f"{{{{< tabs \"{uuid.uuid4()}\" >}}}}\n"

    for key in content:
        res += _generate_tab(key, content[key])

    res += "{{< /tabs >}}\n"
    return res


def _generate_tab(title: str, content: str) -> str:
    return f"{{{{< tab \"{title}\" >}}}}\n{content}\n{{{{< /tab >}}}}\n"
