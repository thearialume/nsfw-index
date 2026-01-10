import json
from urllib.parse import urljoin

from scrapy.http import Response
from scrapy.linkextractors import LinkExtractor

from ..items import Video
from .crawlspider import Rule, TrackedCrawlSpider


class XnxxSpider(TrackedCrawlSpider):
    name = "xnxx"
    allowed_domains = ["www.xnxx.com"]
    # Tags url is used, because index page needs js
    start_urls = ["https://www.xnxx.com/tags/"]
    custom_settings = {
        "ITEM_PIPELINES": {
            "nsfw_index.pipelines.VideoPipeline": 300,
        },
    }

    rules = (
        Rule(LinkExtractor(allow="/tags/")),
        Rule(LinkExtractor(allow="/search/")),
        Rule(LinkExtractor(allow="/video"), callback="parse_item"),
    )

    def parse_item(self, response: Response):
        json_ld_script = response.xpath(
            '//script[@type="application/ld+json"]/text()'
        ).get()
        data = json.loads(json_ld_script, strict=False)
        video = Video.from_schema(data, response.url)

        # Metrics
        video["comments"] = int(
            response.xpath(
                '//div[@class="tab-buttons"]//a[@title="Comments"]//span[@class="value"]/text()'
            )
            .get()
            .replace(",", "")
            if response.xpath(
                '//div[@class="tab-buttons"]//a[@title="Comments"]//span[@class="value"]/text()'
            ).get()
            else 0
        )
        video["likes"] = int(
            response.xpath(
                '//div[@id="video-votes"]//a[contains(@class,"vote-action-good")]//span[@class="value"]/text()'
            )
            .get()
            .replace(",", "")
        )
        video["dislikes"] = int(
            response.xpath(
                '//div[@id="video-votes"]//a[contains(@class,"vote-action-bad")]//span[@class="value"]/text()'
            )
            .get()
            .replace(",", "")
        )
        video["rating"] = int(
            video["likes"] / (video["likes"] + video["dislikes"]) * 100
        )

        # Custom description extraction, because the one in schema just copies title
        video["description"] = (
            response.xpath('//p[contains(@class,"video-description")]/text()').get()
            if response.xpath('//p[contains(@class,"video-description")]/text()')
            else video["description"]
        )

        # Uploader
        # Looks like xnxx allow to post anonymously
        # So ability to extract uploader should be checked
        video["uploader_url"] = (
            urljoin(
                "https://www.xnxx.com/",
                response.xpath('//div[@class="video-title-container"]//a/@href').get(),
            )
            if response.xpath('//div[@class="video-title-container"]//a/@href').get()
            else None
        )
        video["uploader_name"] = (
            response.xpath('//div[@class="video-title-container"]//a/text()')
            .get()
            .strip()
            if response.xpath('//div[@class="video-title-container"]//a/text()').get()
            else None
        )

        # Tags
        tags = response.xpath(
            '//div[contains(@class,"video-tags")]//a[@class="is-keyword"]/text()'
        ).getall()

        summary = tags
        summary = set([i.lower().strip() for i in summary])

        video["tags"] = list(summary)

        yield video
