
import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule
from alloscrap.items import AllocineItem 






class AllocineSpiderSpider(CrawlSpider):
    name = "allocine_spider"
    allowed_domains = ["allocine.fr"]
    start_urls = [f"https://www.allocine.fr/film/meilleurs/?page={n}" for n in range(1,15)]

    rules = [Rule(LinkExtractor(restrict_xpaths="//h2/a"), callback='parse_item', follow=False),]
    #rules = (Rule(LinkExtractor(allow=('/film/fichefilm_gen_cfilm=.*.html',)), callback='parse_item', follow=True),
    def parse_item(self, response):
        item = AllocineItem()  # Instanciation de AllocineItem
        # Extraire les données et les assigner à l'objet item
        item['title'] = response.xpath('//div[@class="titlebar-title titlebar-title-xl"]/text()').get()
        item['original_title'] = response.xpath('//div[@class="meta-body-item"]/span[@class="dark-grey"]/text()').get()
        item['score'] =response.xpath('//div[@class="rating-mdl n40 stareval-stars"]/following-sibling::span/text()').get()
        item['genre'] =response.xpath('//span[@class="spacer"][2]/following-sibling::*/text()').get()
        item['year'] = response.xpath('//div[@class="meta-body-item meta-body-info"]/span/text()').get()
        item['duration'] =response.xpath('//div[@class="meta-body-item meta-body-info"]/text()').getall()
        item['description'] =response.xpath('//p[@class="bo-p"]/text()').get()
        item['actors'] = response.xpath('//span[contains(text(),"Avec")]/following-sibling::span/text()').getall()
        item['director'] =response.xpath('//span[contains(text(),"De")]/following-sibling::span/text()|//span[contains(text(),"Par")]/following-sibling::span/text()').getall()
        yield item