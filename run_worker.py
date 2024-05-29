# @@@SNIPSTART data-pipeline-run-worker-python
import asyncio
import concurrent.futures

from temporalio.client import Client
from temporalio.worker import Worker

from activities import (
    TASK_QUEUE_NAME,
    fetch_post,
    get_post_ids,
    get_top_posts,
    TopTagsGetter,
)
from your_workflow import TemporalCommunityWorkflow


async def main():
    client = await Client.connect("localhost:7233")
    with concurrent.futures.ThreadPoolExecutor(max_workers=100) as activity_executor:
        worker = Worker(
            client,
            task_queue=TASK_QUEUE_NAME,
            workflows=[TemporalCommunityWorkflow],
            activities=[
                fetch_post,
                get_post_ids,
                get_top_posts,
                TopTagsGetter(),
            ],
            activity_executor=activity_executor,
        )
        await worker.run()


if __name__ == "__main__":
    try:
        print("Worker started")
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Worker stopped")


# @@@SNIPEND
