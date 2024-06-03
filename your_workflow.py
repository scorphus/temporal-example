# @@@SNIPSTART data-pipeline-your-workflow-python
import asyncio
from datetime import timedelta
from typing import List, Tuple

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from activities import (
        PostFetcher,
        PostIDsGetter,
        TemporalCommunityPost,
        TopPostsGetter,
        TopTagsGetter,
    )


@workflow.defn
class TemporalCommunityWorkflow:
    @workflow.run
    async def run(self) -> Tuple[List[TemporalCommunityPost], List[Tuple[str, int]]]:
        news_ids = await workflow.execute_activity(
            PostIDsGetter(), start_to_close_timeout=timedelta(seconds=15)
        )
        activities = [
            workflow.execute_activity(
                PostFetcher(), news_id, start_to_close_timeout=timedelta(seconds=15)
            )
            for news_id in news_ids
        ]
        posts = await asyncio.gather(*activities)
        top_posts = await workflow.execute_activity(
            TopPostsGetter(), posts, start_to_close_timeout=timedelta(seconds=15)
        )
        top_tags = await workflow.execute_activity(
            TopTagsGetter(), posts, start_to_close_timeout=timedelta(seconds=15)
        )
        return top_posts, top_tags


# @@@SNIPEND
