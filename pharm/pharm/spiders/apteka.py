import scrapy
from scrapy import Request
from scrapy.http import HtmlResponse
from datetime import datetime
from bs4 import BeautifulSoup as bs


class AptekaSpider(scrapy.Spider):
    name = "apteka"
    allowed_domains = ["apteka-ot-sklada.ru"]

    start_urls = ['https://apteka-ot-sklada.ru/catalog/medikamenty-i-bady/vitaminy-i-mikroelementy/vitaminy-kompleksnye-_multivitaminy_?start=0',
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
            yield Request(url=f'https://{self.allowed_domains[0]}{url}', callback=self.parse)
        next_page_url = self.get_next_page_url(response)

        if next_page_url:
            yield Request(url=next_page_url, callback=self.parse_urls)

    def get_next_page_url(self, response):
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
            "marketing_tags": self.__clear_text(response, self.MARKETING_TAGS),
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
    def __clear_text(response, path: str):
        text_list = response.xpath(path).getall()

        for item in text_list:
            text_list[text_list.index(item)] = item.replace('\n', '').replace('  ', '').replace('₽', '')

        return text_list

    def __check_in_stock(self, response):
        goods_offer_block = self.__clear_text(response, self.GOODS_OFFER_BLOCK)
        if goods_offer_block[0] == 'Временно нет на складе':
            return False
        else:
            return True

    def get_price_data(self, response):
        # обработать вариант когда товара нет в наличии
        in_stock = self.__check_in_stock(response)

        if in_stock:
            price_items = (self.__clear_text(response, self.PRICE_ITEMS))

            for item in price_items:
                price_items[price_items.index(item)] = item.replace(' ', '')

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

    @staticmethod
    def get_metadata(response):
        description = response.xpath("//div[@itemprop='description']/*").getall()

        for string in description:
            soup = bs(string, features='lxml').get_text()
            description[description.index(string)] = soup.replace('\r', '').replace('\n', '').replace('\t', '').replace('\xa0', '').replace('­', '').replace(' ', '').replace('​', '')

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
