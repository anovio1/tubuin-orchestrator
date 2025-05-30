#!/usr/bin/env python3
import asyncio
import argparse

from prefect import get_client
from prefect.client.schemas.filters import FlowRunFilter
from prefect.states import State, StateType


async def cancel_all_late_runs(dry_run: bool = False):
    """
    Find all flow runs stuck in the Late state and request cancellation for each.
    If dry_run=True, only print which runs would be cancelled.
    """
    async with get_client() as client:
        late_runs = await client.read_flow_runs(
            flow_run_filter=FlowRunFilter(
                state={"name": {"any_": ["Late"]}}
            )
        )

        if not late_runs:
            print("No late flow runs found.")
            return

        for run in late_runs:
            print(f"{'[DRY RUN] Would cancel' if dry_run else 'Cancelling'} flow run: {run.name!r} ({run.id})")
            if not dry_run:
                # transition into Cancelling state so agents will pick it up
                result = await client.set_flow_run_state(
                    flow_run_id=run.id,
                    state=State(type=StateType.CANCELLING)
                )

                # The SetStateResult tells us if the transition was accepted
                if result.status == "ABORT":
                    reason = getattr(result.details, "reason", None)
                    print(f"  Could not cancel {run.id!r}: {reason}")
                else:
                    print(f"  Cancellation scheduled for {run.id!r}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Cancel all Prefect flow runs in the Late state."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only list which runs would be cancelled, without actually cancelling them."
    )
    args = parser.parse_args()

    asyncio.run(cancel_all_late_runs(dry_run=args.dry_run))
