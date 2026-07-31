"""Command-line entrypoint: ``filemig run`` (and friends)."""

from __future__ import annotations

import argparse
import asyncio

from filemig.config import LOOPBACK_HOST, get_settings


def _loopback_host(value: str) -> str:
    if value != LOOPBACK_HOST:
        raise argparse.ArgumentTypeError(
            "filemig only supports --host 127.0.0.1"
        )
    return value


def main() -> None:
    parser = argparse.ArgumentParser(prog="filemig")
    sub = parser.add_subparsers(dest="command")

    run_p = sub.add_parser("run", help="start the web dashboard + workers")
    run_p.add_argument(
        "--host", type=_loopback_host, default=None,
        help="listen address (security-restricted to 127.0.0.1)",
    )
    run_p.add_argument("--port", type=int, default=None)

    sub.add_parser("reconcile", help="run a reconciliation pass and print a report")
    sub.add_parser("status", help="print a one-shot migration status summary")

    args = parser.parse_args()
    command = args.command or "run"

    if command == "run":
        _run_server(args)
    elif command == "reconcile":
        asyncio.run(_reconcile())
    elif command == "status":
        asyncio.run(_status())


def _run_server(args) -> None:
    import uvicorn

    settings = get_settings()
    host = args.host or settings.host
    port = args.port or settings.port
    # Pass the app via factory string so uvicorn owns the event loop.
    uvicorn.run("filemig.app:create_app", factory=True, host=host, port=port)


async def _reconcile() -> None:
    from filemig.service import MigrationService

    service = MigrationService(get_settings())
    await service.startup()
    try:
        report = await service.reconcile()
        print(f"checked={report.checked} confirmed={report.confirmed} "
              f"missing={len(report.missing_on_drive)} "
              f"mismatch={len(report.checksum_mismatch)} dead={len(report.dead)}")
        for path in report.missing_on_drive:
            print(f"  MISSING:  {path}")
        for path in report.checksum_mismatch:
            print(f"  MISMATCH: {path}")
        print("OK" if report.ok else "DISCREPANCIES FOUND")
    finally:
        await service.shutdown()


async def _status() -> None:
    from filemig.service import MigrationService

    service = MigrationService(get_settings())
    await service.startup()
    try:
        counts = await service.state_counts()
        quota = await service.quota_status()
        print("file states:")
        for state, n in sorted(counts.items()):
            print(f"  {state:14s} {n}")
        gib = 1024**3
        print(f"quota: {quota.used_24h_bytes / gib:.1f} / "
              f"{quota.effective_cap_bytes / gib:.1f} GiB used (24h)")
    finally:
        await service.shutdown()


if __name__ == "__main__":
    main()
