# @@@SNIPSTART data-pipeline-run-worker-python
import asyncio
import concurrent.futures
from typing import Optional

from temporalio.client import Client
from temporalio.worker import Worker

from activities import (
    TASK_QUEUE_NAME,
    PostIDsGetter,
    PostFetcher,
    TopPostsGetter,
    TopTagsGetter,
)
from your_workflow import TemporalCommunityWorkflow


async def main():
    client = await Client.connect("localhost:7233")
    with concurrent.futures.ThreadPoolExecutor(max_workers=100) as activity_executor:
        worker = new_worker(client, TASK_QUEUE_NAME, activity_executor)
        await worker.run()


def new_worker(
    client: Client,
    task_queue: str,
    activity_executor: Optional[concurrent.futures.Executor] = None,
) -> Worker:
    return Worker(
        client,
        task_queue=task_queue,
        workflows=[TemporalCommunityWorkflow],
        activities=[
            PostIDsGetter(),
            PostFetcher(),
            TopPostsGetter(),
            TopTagsGetter(),
        ],
        activity_executor=activity_executor,
    )


if __name__ == "__main__":
    try:
        print("Worker started")
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Worker stopped")


# @@@SNIPEND
