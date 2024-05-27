# @@@SNIPSTART data-pipeline-your-workflow-python
from datetime import timedelta
from typing import List

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from activities import TemporalCommunityPost, get_post_ids, get_top_posts


@workflow.defn
class TemporalCommunityWorkflow:
    @workflow.run
    async def run(self) -> List[TemporalCommunityPost]:
        news_ids = await workflow.execute_activity(
            get_post_ids,
            start_to_close_timeout=timedelta(seconds=15),
        )
        return await workflow.execute_activity(
            get_top_posts,
            news_ids,
            start_to_close_timeout=timedelta(seconds=15),
        )


# @@@SNIPEND
