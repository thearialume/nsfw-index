import psycopg

from .items import Video
from .settings import DATABASE_URL


class VideoPipeline:
    def __init__(self):
        self.connection = psycopg.connect(DATABASE_URL)
        self.cursor = self.connection.cursor()

        # Create video table if not exists
        self.cursor.execute("""
        create table if not exists videos (
            source_url text primary key,
            domain text not null,
            content_url text,
            thumbnail_url text,
            uploader_url text,
            uploader_name text,
            title text,
            description text,
            tags text[],
            tags_n text[],
            duration int,
            views int,
            likes int,
            dislikes int,
            comments int,
            rating int check (rating between 0 and 100),
            upload_date timestamp
        )
        """)

    # Lots of code, but one query handles both inserts and updates, so it should be screamingly fast! :3
    def process_item(self, item: Video, spider):
        query = """
        insert into videos (
            source_url,
            domain,
            content_url,
            thumbnail_url,
            uploader_url,
            uploader_name,
            title,
            description,
            tags,
            duration,
            views,
            likes,
            dislikes,
            comments,
            rating,
            upload_date
        ) values (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        ) on conflict (source_url) do update set
        content_url = excluded.content_url,
        thumbnail_url = excluded.thumbnail_url,
        uploader_url = excluded.uploader_url,
        uploader_name = excluded.uploader_name,
        title = excluded.title,
        description = excluded.description,
        tags = excluded.tags,
        duration = excluded.duration,
        views = excluded.views,
        likes = excluded.likes,
        dislikes = excluded.dislikes,
        comments = excluded.comments,
        rating = excluded.rating,
        upload_date = excluded.upload_date
        """

        self.connection.execute(
            query,
            (
                item["source_url"],
                item["domain"],
                item.get("content_url"),
                item.get("thumbnail_url"),
                item.get("uploader_url"),
                item.get("uploader_name"),
                item.get("title"),
                item.get("description"),
                item.get("tags", []),
                item.get("duration"),
                item.get("views"),
                item.get("likes"),
                item.get("dislikes"),
                item.get("comments"),
                item.get("rating"),
                item.get("upload_date"),
            ),
        )

        self.connection.commit()
        return item

    def close_spider(self, spider):
        self.cursor.close()
        self.connection.close()
