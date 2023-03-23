import scrapy
from scrapy import Request
from scrapy.http import HtmlResponse
from datetime import datetime
from bs4 import BeautifulSoup as bs
from re import sub


class AptekaSpider(scrapy.Spider):
    name = "apteka"
    allowed_domains = ["apteka-ot-sklada.ru"]

    start_urls = [
        'https://apteka-ot-sklada.ru/catalog/medikamenty-i-bady/vitaminy-i-mikroelementy/vitaminy-kompleksnye-_multivitaminy_?start=0',
        'https://apteka-ot-sklada.ru/catalog/medikamenty-i-bady/antistressovoe-deystvie/uspokoitelnye',
        'https://apteka-ot-sklada.ru/catalog/medikamenty-i-bady/antistressovoe-deystvie/antidepressanty'
        ]

    cookies = {'city': 92}

    def start_requests(self):
        for url in self.start_urls:
            yield Request(url=url, callback=self.parse_urls, cookies=self.cookies)

    def parse_urls(self, response: HtmlResponse):
        urls = response.xpath(self.GOODS_HREFS).getall()

        for url in urls:
            yield Request(url=f'https://{self.allowed_domains[0]}{url}', callback=self.parse, cookies=self.cookies)
        next_page_url = self.__get_next_page_url(response)

        if next_page_url:
            yield Request(url=next_page_url, callback=self.parse_urls, cookies=self.cookies)

    def __get_next_page_url(self, response):
        next_page_url = response.xpath("//li[contains(@class, 'item_next')]/a/@href").get()
        if next_page_url:
            url = f'https://{self.allowed_domains[0]}{next_page_url}'
            return url

    def parse(self, response, **kwargs):
        yield {
            'timestamp': datetime.timestamp(datetime.now()),
            'RPC': response.url.split('_')[-1],
            'url': response.url,
            "title": response.xpath(self.TITLE).get(),
            "marketing_tags": self.__clear_text(response.xpath(self.MARKETING_TAGS).getall()),
            "brand": response.xpath(self.BRAND).get(),
            "section": response.xpath(self.SECTION).getall(),
            "price_data": self.get_price_data(response),
            "stock": {
                "in_stock": self.__check_in_stock(response),
                "count": 0
            },
            "assets": self.get_assets(response),
            "metadata": self.get_metadata(response),
            "variants": 1,
        }

    @staticmethod
    def __clear_text(dirty_text):
        # чистим все лишние символы и колличество пробелов больше двух
        dirty_char = '[\r\t\n\xa0­ ​₽]|\s{2,}'
        text_list = [sub(dirty_char, '', item) for item in dirty_text]

        return text_list

    def __check_in_stock(self, response):
        goods_offer_block = response.xpath(self.GOODS_OFFER_BLOCK).getall()
        goods_offer_block = self.__clear_text(goods_offer_block)

        if goods_offer_block[0] == 'Временно нет на складе':
            return False
        else:
            return True

    def get_price_data(self, response):
        # обработать вариант когда товара нет в наличии
        in_stock = self.__check_in_stock(response)

        if in_stock:
            price_items = response.xpath(self.PRICE_ITEMS).getall()
            price_items = (self.__clear_text(price_items))

            # чистим лишний пробел в цене от тысячи
            if len(price_items[0]) > 8 or len(price_items[-1]) > 8:
                price_items = [item.replace(' ', '') for item in price_items]

            price_items = list(map(float, price_items))

            if len(price_items) > 1:
                current_cost = price_items[0]
                original_cost = price_items[1]
                sale_tag = f"Скидка {100.0 - (current_cost / original_cost) * 100}%"
            else:
                current_cost = price_items[0]
                original_cost = price_items[0]
                sale_tag = ''
        else:
            current_cost = float(0)
            original_cost = float(0)
            sale_tag = ''

        return {
            "current": current_cost,
            "original": original_cost,
            "sale_tag": sale_tag
        }

    def get_assets(self, response):
        images_container = response.xpath("//ul[@class='goods-gallery__preview-list']/li//img/@src").getall()
        main_img = response.xpath("//ul[@class='goods-gallery__preview-list']/li//img/@src").get()
        return {
            "main_image": f'https://{self.allowed_domains[0]}{main_img}',
            "set_images": [f'https://{self.allowed_domains[0]}{img}' for img in images_container],
            "view360": [],
            "video": []
        }

    def get_metadata(self, response):
        description = response.xpath("//div[@itemprop='description']/*").getall()

        description = [bs(string, 'lxml').get_text() for string in description]
        description = self.__clear_text(description)
        description = ' '.join(description)

        return {
            "__description": description,
            "АРТИКУЛ": response.url.split('_')[-1],
            "СТРАНА ПРОИЗВОДИТЕЛЬ": response.xpath("//span[@itemtype='location']/text()").get()
        }

    GOODS_HREFS = "//div[@class='goods-grid__inner']/div//a[@class='goods-card__link']/@href"
    MARKETING_TAGS = "//span[@class='ui-tag text text_weight_medium ui-tag_theme_secondary']/text()"
    TITLE = "//h1//span/text()"
    BRAND = "//span[@itemtype='legalName']/text()"
    SECTION = "//ul[@class='ui-breadcrumbs__list']/li//span[@itemprop='name']/text()"

    GOODS_OFFER_BLOCK = "//div[@class='goods-offer-panel']/div/text()"
    PRICE_ITEMS = "//div[@class='goods-offer-panel__price']/span/text()"
