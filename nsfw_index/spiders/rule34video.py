import json

from scrapy.http import Response
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule

from ..items import Video


class Rule34videoSpider(CrawlSpider):
    name = "rule34video"
    allowed_domains = ["rule34video.com"]
    start_urls = ["https://rule34video.com"]
    custom_settings = {
        "ITEM_PIPELINES": {
            "nsfw_index.pipelines.VideoPipeline": 300,
        }
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

        # Not sure, if rating appearence is persistent, so I'll leave it like this for now
        try:
            video["rating"] = int(
                response.css(".voters.count::text").get().split("%")[0]
            )
            video["dislikes"] = int(
                round(video.get("likes") / video.get("rating") * 100)
                - video.get("likes")
            )
        except:  # noqa: E722
            pass

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

        video["tags"] = tags + categories + artists

        yield video
