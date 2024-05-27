# @@@SNIPSTART data-pipeline-your-workflow-python
import asyncio
from datetime import timedelta
from typing import List

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from activities import (
        TemporalCommunityPost,
        fetch_post,
        get_post_ids,
        get_top_posts,
    )


@workflow.defn
class TemporalCommunityWorkflow:
    @workflow.run
    async def run(self) -> List[TemporalCommunityPost]:
        news_ids = await workflow.execute_activity(
            get_post_ids,
            start_to_close_timeout=timedelta(seconds=15),
        )
        activities = [
            workflow.execute_activity(
                fetch_post, news_id, start_to_close_timeout=timedelta(seconds=15)
            )
            for news_id in news_ids
        ]
        posts = await asyncio.gather(*activities)
        return await workflow.execute_activity(
            get_top_posts, posts, start_to_close_timeout=timedelta(seconds=15)
        )


# @@@SNIPEND
