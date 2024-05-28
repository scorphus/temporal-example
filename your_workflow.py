# @@@SNIPSTART data-pipeline-your-workflow-python
import asyncio
from datetime import timedelta
from typing import List, Tuple

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from activities import (
        TemporalCommunityPost,
        fetch_post,
        get_post_ids,
        get_top_posts,
        get_top_tags,
    )


@workflow.defn
class TemporalCommunityWorkflow:
    @workflow.run
    async def run(self) -> Tuple[List[TemporalCommunityPost], List[Tuple[str, int]]]:
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
        stories = await workflow.execute_activity(
            get_top_posts, posts, start_to_close_timeout=timedelta(seconds=15)
        )
        top_tags = await workflow.execute_activity(
            get_top_tags, posts, start_to_close_timeout=timedelta(seconds=15)
        )
        return stories, top_tags


# @@@SNIPEND
