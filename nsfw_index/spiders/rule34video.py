import json

from scrapy.http import Response
from scrapy.linkextractors import LinkExtractor
from .crawlspider import TrackedCrawlSpider, Rule

from ..items import Video


class Rule34videoSpider(TrackedCrawlSpider):
    name = "rule34video"
    allowed_domains = ["rule34video.com"]
    start_urls = ["https://rule34video.com/"]
    custom_settings = {
        "ITEM_PIPELINES": {
            "nsfw_index.pipelines.VideoPipeline": 300,
        },
    }

    rules = (
        Rule(LinkExtractor(allow="/latest-updates/")),
        Rule(LinkExtractor(allow="/video/"), callback="parse_item"),
    )

    def parse_item(self, response: Response):
        json_ld_script = response.xpath(
            '//script[@type="application/ld+json"]/text()'
        ).get()
        data = json.loads(json_ld_script, strict=False)
        video = Video.from_schema(data, response.url)

        # Metrics
        video["comments"] = int(
            response.xpath('//a[@href="#tab_comments"]/text()')
            .get()
            .split("(")[1]
            .replace(")", "")
        )
        # Try Except looked too spooky here, so I dediced to replace it!
        video["rating"] = (
            int(
                response.css(".voters.count::text").get().split("%")[0],
            )
            if response.css(".voters.count::text").get().split("%")[0]
            else 0
        )
        video["dislikes"] = (
            int(
                round(video.get("likes") / video.get("rating") * 100)
                - video.get("likes")
            )
            if video.get("rating") and video.get("likes")
            else 0
        )

        # Uploader
        video["uploader_url"] = response.xpath(
            '//div[text()="Uploaded by"]/../a/@href'
        ).get()
        video["uploader_name"] = (
            response.xpath('string(//div[text()="Uploaded by"]/../a)').get().strip()
        )

        # Tags
        tags = response.xpath(
            '//a[@class="tag_item" and contains(@href,"tags")]/text()'
        ).getall()
        categories = response.xpath(
            '//div[text()="Categories"]/../a//span/text()'
        ).getall()
        artists = response.xpath('//div[text()="Artist"]/../a//span/text()').getall()

        # Overlap in tags and categories taken properly now
        summary = tags + categories + artists
        summary = set([i.lower().strip() for i in summary])

        video["tags"] = list(summary)

        yield video
