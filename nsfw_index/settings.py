import os

from dotenv import load_dotenv

load_dotenv()

BOT_NAME = "nsfw_index"

SPIDER_MODULES = ["nsfw_index.spiders"]
NEWSPIDER_MODULE = "nsfw_index.spiders"

# Crawl responsibly by identifying yourself (and your website) on the user-agent
USER_AGENT = os.getenv("CRAWLER_USER_AGENT")
if USER_AGENT is None:
    raise Exception(
        "CRAWLER_USER_AGENT is not set in environment variables: Crawl responsibly by identifying yourself"
    )

DATABASE_URL = os.getenv("DATABASE_URL")
if DATABASE_URL is None:
    raise Exception(
        "DATABASE_URL is not set in environment variables: Where will you store data?"
    )

# Obey robots.txt rules
ROBOTSTXT_OBEY = True

# Concurrency and throttling settings
CONCURRENT_REQUESTS_PER_DOMAIN = os.getenv("CONCURRENT_REQUESTS_PER_DOMAIN", 1)
DOWNLOAD_DELAY = os.getenv("DOWNLOAD_DELAY", 1)

FEED_EXPORT_ENCODING = "utf-8"
