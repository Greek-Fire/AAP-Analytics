import csv
from pathlib import Path
from typing import Iterable, Dict, Any, Union

HEADERS = [
    "id","name","playbook","description","inventory","project","date","time",
    "organization","exe_user","total_hosts","success","unreachable","failed",
    "skipped","rescued","ignored","canceled","inventory_failed",
    "project_failed","ansible_failed"
]

class ReportWriter:
    def __init__(self, path: Union[str, Path]):
        self.path = Path(path).expanduser()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(",".join(HEADERS) + "\n", encoding="utf-8")

    def append(self, rows: Iterable[Dict[str, Any]]) -> None:
        with self.path.open("a", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=HEADERS)
            for r in rows:
                w.writerow(r)
