import argparse, asyncio, yaml
from aap_client import AAPClient, AAPConfig
from job_parser import summarize
from report_writer import ReportWriter

def load_cfg(path: str) -> AAPConfig:
    with open(path, "r") as f:
        y = yaml.safe_load(f)
    return AAPConfig(
        base_url=y["aap_url"].rstrip("/"),
        verify_ssl=y.get("verify_ssl", True),
        token=y["token"],
    )

def main() -> None:
    ap = argparse.ArgumentParser(description="Generate AAP job report")
    ap.add_argument("--config", default="~/.config.yml")
    ap.add_argument("--report", default="report.csv")
    ap.add_argument("--start-id", type=int, help="First job id to include")
    ap.add_argument("--end-id",   type=int, help="Last job id to include")
    ap.add_argument("--batch",    type=int, default=20,
                    help="Concurrent stdout downloads")

    args = ap.parse_args()
    cfg = load_cfg(args.config)
    writer = ReportWriter(args.report)
    client = AAPClient(cfg)

    batch, ids, rows = args.batch, [], []

    def _run_async(coro):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            return loop.run_until_complete(coro)
        finally:
            loop.close()
            asyncio.set_event_loop(None)

    async def flush():
        if not ids:
            return
        stdout_map = await client.get_stdout_bulk(ids)
        rows.extend(summarize(j, stdout_map[j["id"]]) for j in jobs)
        writer.append(rows)
        ids.clear(); rows.clear(); jobs.clear()

    jobs = []
    for job in client.iter_jobs(args.start_id, args.end_id):
        jobs.append(job)
        ids.append(job["id"])
        if len(ids) >= batch:
            _run_async(flush())

    _run_async(flush())                # final partial batch
    print(f"✅ Report written to {writer.path}")

if __name__ == "__main__":
    main()
