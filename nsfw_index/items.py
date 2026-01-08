from scrapy.item import Item, Field
from typing import Optional, Any
from urllib.parse import urlparse
import datetime
import re


class Video(Item):
    """
    Video metadata item for nsfw-index database

    Core fields:
        - source_url: Video URL (required, unique)
        - domain: Source domain (required)

    All other fields are optional (nullable in database).
    Rating is normalized to 0-100 scale where possible.
    """

    # Identity (required)
    source_url: str = Field()
    domain: str = Field()

    # Content metadata (optional)
    thumbnail_url: Optional[str] = Field(default=None)
    uploader_url: Optional[str] = Field(default=None)
    uploader_name: Optional[str] = Field(default=None)
    title: Optional[str] = Field(default=None)
    description: Optional[str] = Field(default=None)
    tags: list[str] = Field(default_factory=list)
    duration: Optional[int] = Field(default=None)

    # Engagement metrics (optional)
    views: Optional[int] = Field(default=None)
    likes: Optional[int] = Field(default=None)
    dislikes: Optional[int] = Field(default=None)
    comments: Optional[int] = Field(default=None)

    # Rating: 0-100 scale
    # Formula: likes/(likes+dislikes)*100
    # Or converted from alternative scales (e.g., 5-star -> 0-100)
    # Alternative scale formula: (score/max_score)*100
    rating: Optional[int] = Field(default=None)

    # Date (optional)
    upload_date: Optional[datetime.datetime] = Field(default=None)

    # Credits to Matt Savage <spatialtime>
    # Found and adapted from https://gist.github.com/spatialtime/c1924a3b178b4fe721fe406e0bf1a1dc
    @staticmethod
    def _parse_iso_duration(iso_duration: str) -> int:
        """Parses an ISO 8601 duration string into a datetime.timedelta instance.
        Args:
            iso_duration: an ISO 8601 duration string.
        Returns:
            a datetime.timedelta instance
        """
        m = re.match(
            r"^P(?:(\d+)Y)?(?:(\d+)M)?(?:(\d+)D)?T(?:(\d+)H)?(?:(\d+)M)?(?:(\d+(?:.\d+)?)S)?$",
            iso_duration,
        )
        if m is None:
            raise ValueError("invalid ISO 8601 duration string")

        days = 0
        hours = 0
        minutes = 0
        seconds = 0

        if m[3]:
            days = int(m[3])
        if m[4]:
            hours = int(m[4])
        if m[5]:
            minutes = int(m[5])
        if m[6]:
            seconds = float(m[6])

        return int(
            datetime.timedelta(
                days=days, hours=hours, minutes=minutes, seconds=seconds
            ).total_seconds()
        )

    @classmethod
    def from_schema(cls, schema: dict[str, Any], source_url: str) -> "Video":
        """
        Factory method: Creates a Video item from a Schema.org VideoObject dictionary.
        Use it initially, if it is supported, then collect left metadata from page

        Args:
            schema: The dictionary containing JSON-LD data.
            source_url: The URL of the page where the schema was found.
        """
        item = cls()

        item["source_url"] = source_url
        if source_url:
            item["domain"] = urlparse(source_url).netloc.replace("www.", "")

        item["title"] = schema.get("name")
        item["description"] = schema.get("description")
        item["thumbnail_url"] = schema.get("thumbnailUrl")

        if duration_str := schema.get("duration"):
            item["duration"] = cls._parse_iso_duration(duration_str)

        if date_str := schema.get("uploadDate"):
            try:
                item["upload_date"] = datetime.datetime.fromisoformat(date_str)
            except ValueError:
                pass

        # Normalize interactionStatistic
        stats = schema.get("interactionStatistic", [])
        if isinstance(stats, dict):
            stats = [stats]

        # Collect all possible interactions
        for stat in stats:
            int_type = stat.get("interactionType", "")
            try:
                count = int(stat.get("userInteractionCount", 0))
            except (ValueError, TypeError):
                continue

            if "WatchAction" in int_type:
                item["views"] = count
            elif "LikeAction" in int_type:
                item["likes"] = count
            elif "DislikeAction" in int_type:
                item["dislikes"] = count

        # Rating calculation
        likes = item.get("likes")
        dislikes = item.get("dislikes")

        if likes is not None and dislikes is not None:
            item["rating"] = int((likes / (likes + dislikes)) * 100)

        return item
