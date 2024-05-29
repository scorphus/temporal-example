# @@@SNIPSTART data-pipeline-activity-python
import logging
from collections import Counter
from dataclasses import dataclass
from typing import List, Tuple

import aiohttp
from temporalio import activity

TASK_QUEUE_NAME = "temporal-community-task-queue"

# Logging configuration
logging.basicConfig(level=logging.INFO)


@dataclass
class TemporalCommunityPost:
    title: str
    url: str
    tags: List[str]
    views: int


@activity.defn
async def get_post_ids() -> List[str]:
    async with aiohttp.ClientSession() as session, session.get(
        "https://community.temporal.io/latest.json"
    ) as response:
        if not 200 <= int(response.status) < 300:
            raise RuntimeError(f"Status: {response.status}")
        post_ids = await response.json()
    return [str(topic["id"]) for topic in post_ids["topic_list"]["topics"]]


@activity.defn
async def fetch_post(item_id: str) -> TemporalCommunityPost:
    logging.info("Fetching post %s ...", item_id)
    async with aiohttp.ClientSession() as session, session.get(
        f"https://community.temporal.io/t/{item_id}.json"
    ) as response:
        if response.status < 200 or response.status >= 300:
            raise RuntimeError(f"Status: {response.status}")
        item = await response.json()
        slug = item["slug"]
        url = f"https://community.temporal.io/t/{slug}/{item_id}"
        post = TemporalCommunityPost(
            title=item["title"], url=url, tags=item.get("tags", []), views=item["views"]
        )
        logging.info("Fetched post %s", post.url)
    return post


@activity.defn
def get_top_posts(posts: List[TemporalCommunityPost]) -> List[TemporalCommunityPost]:
    return sorted(posts, key=lambda x: x.views, reverse=True)[:10]


@activity.defn
class TopTagsGetter:

    def __call__(self, posts: List[TemporalCommunityPost]) -> List[Tuple[str, int]]:
        tag_counter: Counter[str] = Counter()
        for post in posts:
            tag_counter.update(post.tags)
        return tag_counter.most_common(10)


# @@@SNIPEND
