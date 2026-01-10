import json
from urllib.parse import urljoin

from scrapy.http import Response
from scrapy.linkextractors import LinkExtractor

from ..items import Video
from .crawlspider import Rule, TrackedCrawlSpider


class XvideosSpider(TrackedCrawlSpider):
    name = "xvideos"
    allowed_domains = ["www.xvideos.com"]
    start_urls = ["https://www.xvideos.com/"]
    custom_settings = {
        "ITEM_PIPELINES": {
            "nsfw_index.pipelines.VideoPipeline": 300,
        },
    }

    rules = (
        Rule(LinkExtractor(allow="/new/")),
        Rule(LinkExtractor(allow="/video"), callback="parse_item"),
    )

    # Normalize values rounded up by Xvideos
    # We're getting some inaccuracy because of that
    # But doesn't seem they can be obtained other way
    def votes_to_int(self, text: str) -> int:
        text = text.strip().lower()

        if text.endswith("k"):
            number = float(text[:-1])
            return int(number * 1000)

        return int(text)

    def parse_item(self, response: Response):
        json_ld_script = response.xpath(
            '//script[@type="application/ld+json"]/text()'
        ).get()
        data = json.loads(json_ld_script, strict=False)
        video = Video.from_schema(data, response.url)

        # Metrics
        video["comments"] = int(
            response.xpath(
                '//button[@class="comments tab-button"]//span[@class="badge"]/text()'
            ).get()
        )
        video["likes"] = self.votes_to_int(
            response.xpath(
                '//div[@class="rate-infos"]//span[@class="rating-good-nbr"]/text()'
            ).get()
        )
        video["dislikes"] = self.votes_to_int(
            response.xpath(
                '//div[@class="rate-infos"]//span[@class="rating-bad-nbr"]/text()'
            ).get()
        )
        video["rating"] = int(
            video["likes"] / (video["likes"] + video["dislikes"]) * 100
        )

        # Uploader
        video["uploader_url"] = urljoin(
            "https://www.xvideos.com/",
            response.xpath('//li[@class="main-uploader"]/a//@href').get(),
        )
        video["uploader_name"] = (
            response.xpath('//li[@class="main-uploader"]//span[@class="name"]/text()')
            .get()
            .strip()
        )

        # Tags
        tags = response.xpath(
            '//div[@class="video-metadata video-tags-list ordered-label-list cropped"]//a[@class="is-keyword btn btn-default"]/text()'
        ).getall()
        models = response.xpath(
            '//li[@class="model"]//span[@class="name"]/text()'
        ).getall()

        # In case we have any overlaps
        summary = tags + models
        summary = set([i.lower().strip() for i in summary])

        video["tags"] = list(summary)

        yield video
