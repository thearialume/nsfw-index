from datetime import datetime
from typing import Any, cast

from scrapy.http import Request, Response
from scrapy.link import Link
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule

from ..db import connection, cursor


# I know this code might look crazy, but hear me out! XD
# Since scrapers in this project might be really really long-running
# I needed something equally reliable in terms of spiders state saving
# In my case, built-in JOBDIR is not reliable at all! Because it was designed for grace shutdown only
# So I designed a Spider over built-in CrawlSpider so I can switch between them any time
# Actually, before this spider I made a middleware and it worked great
# But real problem lies in how CrawlSpider works, start_urls are never affected by rules
# So I had an option to write even more insane things in middleware
# Or just to modify CrawlSpider! So I stuck with it! :3
class TrackedCrawlSpider(CrawlSpider):
    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        self.connection = connection
        self.cursor = cursor

        self.cursor.execute("""
        create table if not exists tasks (
            source_url text primary key,
            spider text,
            status bool,
            time timestamp
        )
        """)
        self.connection.commit()

    async def start(self):
        # This is an absolute hack
        # To make base domain being processed through rule system
        self.rules = list(self.rules)
        self.rules.append(
            Rule(
                LinkExtractor(
                    allow=r"^https?://[^/]+/?$", allow_domains=self.allowed_domains
                )
            )
        )
        self._compile_rules()

        # Load urls which were found in previous run, but never completed
        incompleted = [
            i[0]
            for i in self.cursor.execute(
                "select source_url from tasks where spider = %s and status = %s",
                (self.crawler.spider.name, False),
            ).fetchall()
        ]

        # Refer to CrawlSpider._requests_to_follow
        # In short, just map all start urls to rules
        # And let spider process them as if they been found
        links = self.start_urls + incompleted
        for rule_index, rule in enumerate(self._rules):
            matches = [i for i in links if rule.link_extractor.matches(i)]
            for url in matches:
                yield self._build_request(rule_index, Link(url))

    # Slightly modified callback to track url completion
    def _callback(self, response: Response, **cb_kwargs: Any) -> Any:
        self.cursor.execute(
            "update tasks set status=%s where source_url=%s and spider=%s",
            (True, response.url, self.name),
        )
        self.connection.commit()

        rule = self._rules[cast("int", response.meta["rule"])]
        return self.parse_with_rules(
            response,
            rule.callback,
            {**rule.cb_kwargs, **cb_kwargs},
            rule.follow,
        )

    # Slightly modified build to track url queue
    def _build_request(self, rule_index: int, link: Link) -> Request:
        existed = self.cursor.execute(
            """
            insert into tasks (source_url, spider, status, time)
            values (%s, %s, %s, %s)
            on conflict (source_url) do update
            set status = tasks.status
            where tasks.status = false
            returning source_url;
            """,
            (link.url, self.name, False, datetime.now()),
        ).fetchall()
        self.connection.commit()

        # SQL query above returns True result in two cases
        # 1. source_url was not in the database
        # 2. source_url was in database, but not completed
        # So if source_url was presented and completed it returns [](False)
        if not existed:
            return None

        return Request(
            url=link.url,
            callback=self._callback,
            errback=self._errback,
            meta={"rule": rule_index, "link_text": link.text},
        )
