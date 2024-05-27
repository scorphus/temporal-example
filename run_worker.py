# @@@SNIPSTART data-pipeline-run-worker-python
import asyncio

from temporalio.client import Client
from temporalio.worker import Worker

from activities import TASK_QUEUE_NAME, get_post_ids, get_top_posts
from your_workflow import TemporalCommunityWorkflow


async def main():
    client = await Client.connect("localhost:7233")
    worker = Worker(
        client,
        task_queue=TASK_QUEUE_NAME,
        workflows=[TemporalCommunityWorkflow],
        activities=[get_top_posts, get_post_ids],
    )
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
# @@@SNIPEND
