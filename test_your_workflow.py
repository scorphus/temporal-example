import concurrent.futures
import random
import uuid

import pytest
from temporalio import activity
from temporalio.testing import WorkflowEnvironment

import activities
import run_worker
import your_workflow


@activity.defn(name="PostIDsGetter")
async def post_ids_getter_mock() -> list[str]:
    return list(map(str, range(10)))


@activity.defn(name="PostFetcher")
async def post_fetcher_mock(item_id: str) -> activities.TemporalCommunityPost:
    tags = ["tag1", "tag2", "tag3", "tag4", "tag5", "tag6"]
    return activities.TemporalCommunityPost(
        title=f"{item_id}_title",
        url=f"{item_id}_url",
        tags=random.choices(tags, k=random.randint(1, 3)),
        views=random.randint(0, 1000),
    )


@pytest.fixture(autouse=True)
def fixture_post_ids_getter(session_mocker):
    session_mocker.patch("activities.PostIDsGetter", return_value=post_ids_getter_mock)
    session_mocker.patch("run_worker.PostIDsGetter", return_value=post_ids_getter_mock)
    session_mocker.patch(
        "your_workflow.PostIDsGetter", return_value=post_ids_getter_mock
    )


@pytest.fixture(autouse=True)
def fixture_post_fetcher(session_mocker):
    session_mocker.patch("activities.PostFetcher", return_value=post_fetcher_mock)
    session_mocker.patch("run_worker.PostFetcher", return_value=post_fetcher_mock)
    session_mocker.patch("your_workflow.PostFetcher", return_value=post_fetcher_mock)


@pytest.mark.asyncio
async def test_execute_workflow():
    task_queue_name = str(uuid.uuid4())
    with concurrent.futures.ThreadPoolExecutor(max_workers=100) as activity_executor:
        async with await WorkflowEnvironment.start_time_skipping() as env:
            async with run_worker.new_worker(
                env.client, task_queue_name, activity_executor
            ):
                top_posts, top_tags = await env.client.execute_workflow(
                    your_workflow.TemporalCommunityWorkflow.run,
                    id=str(uuid.uuid4()),
                    task_queue=task_queue_name,
                )
                assert top_posts is not None
                assert top_tags is not None
                assert len(top_posts) == 10
                assert 1 <= len(top_tags) <= 6
