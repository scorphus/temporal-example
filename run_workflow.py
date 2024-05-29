# @@@SNIPSTART data-pipeline-run-workflow-python
import asyncio

import pandas as pd
from temporalio.client import Client

from activities import TASK_QUEUE_NAME
from your_workflow import TemporalCommunityWorkflow


async def main():
    client = await Client.connect("localhost:7233")

    top_posts, top_tags = await client.execute_workflow(
        TemporalCommunityWorkflow.run,
        id="temporal-community-workflow",
        task_queue=TASK_QUEUE_NAME,
    )
    df = pd.DataFrame(top_posts)
    df.columns = ["Title", "URL", "Tags", "Views"]
    print("Top 10 posts on Temporal Community:")
    print(df)
    df2 = pd.DataFrame(top_tags)
    df2.columns = ["Tag", "Posts"]
    print("Top 10 tags on Temporal Community:")
    print(df2)
    return df, df2


if __name__ == "__main__":
    asyncio.run(main())


# @@@SNIPEND
