import re
from typing import Dict, Any

# One compiled regex is ~10× faster than many .split() calls
LINE_RE = re.compile(
    r"^(?P<host>\S+)\s*:.*ok=(?P<ok>\d+)\s+changed=\d+"
    r"\s+unreachable=(?P<unr>\d+)\s+failed=(?P<fail>\d+)"
    r"\s+skipped=(?P<skip>\d+)\s+rescued=(?P<resc>\d+)"
    r"\s+ignored=(?P<ign>\d+)"
)

def summarize(job: Dict[str, Any], stdout: str) -> Dict[str, Any]:
    """Return one flat record ready for CSV."""
    meta = job["summary_fields"]
    inv  = meta.get("inventory", {}).get("name")
    proj = meta.get("project", {}).get("name")
    org  = meta["organization"]["name"]
    finished = job["finished"] or job["started"]
    date_, time_ = finished.split("T")
    time_ = time_[:8]
    user = (job.get("launched_by") or {}).get("name") \
        or (job["summary_fields"].get("user") or {}).get("username") \
        or "N/A"

    counts = dict(total_hosts=0, success=0, unreachable=0,
                  failed=0, skipped=0, rescued=0, ignored=0)

    if stdout:
        for m in LINE_RE.finditer(stdout):
            counts["total_hosts"] += 1
            ok, unr, fail, skip, resc, ign = map(int, m.groups()[1:])
            if unr:   counts["unreachable"] += 1
            elif fail:counts["failed"]      += 1
            elif ok:  counts["success"]     += 1
            if skip and not (ok or unr or fail):
                counts["skipped"] += 1
            if resc:  counts["rescued"] += 1
            if ign:   counts["ignored"] += 1

    return {
        "id": job["id"],
        "name": job["name"],
        "playbook": job["playbook"],
        "description": job["description"],
        "inventory": inv,
        "project": proj,
        "date": date_,
        "time": time_,
        "organization": org,
        "exe_user": user,
        **counts,
        "canceled": job["status"] == "canceled",
        "inventory_failed": "inventory_update" in job["job_explanation"],
        "project_failed":   "project_update"   in job["job_explanation"],
        "ansible_failed":   not bool(stdout),
    }
