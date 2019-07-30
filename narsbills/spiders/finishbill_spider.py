import scrapy
import time
import re
from scrapy import signals
from scrapy import Spider



TARGET_URL_FINISHBILL = "http://likms.assembly.go.kr/bill/FinishBill.do"
TARGET_URL_MOORINGBILL = "http://likms.assembly.go.kr/bill/MooringBill.do"
TARGET_URL_DETAIL = "http://likms.assembly.go.kr/bill/billDetail.do?billId="
TARGET_URL_DETAIL_COACTORLIST= "http://likms.assembly.go.kr/bill/coactorListPopup.do?billId="

FORMREQUEST_PAGE_SIZE = "100"


class MooringBillSpider(scrapy.Spider):

    name = "MooringBill"
    start_urls = [
        TARGET_URL_MOORINGBILL,
    ]
    custom_settings = {
        'ITEM_PIPELINES': {
            'narsbills.pipelines.MooringBillPipeline': 300
        }
    }

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.i = 1
        self.count = 0
        self.DroppedItem = set()

    def start_requests(self):
        yield scrapy.FormRequest(
            url=TARGET_URL_MOORINGBILL,
            formdata={
                'strPage': f'{self.i}',
                'pageSize': FORMREQUEST_PAGE_SIZE,
            }
        )

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(MooringBillSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.item_dropped, signal=signals.item_dropped, **kwargs)
        return spider

    def item_dropped(self, **kwargs):
        self.count += 1
        try:
            self.DroppedItem.add(kwargs['item']['BillLink'])
        except Exception as error:
            print(error)

    def parse(self, response):
        for tr in response.css('div.tableCol01 > table > tbody > tr'):
            yield {
                'BillNo': tr.xpath('td[1]/text()').get(),
                'Finished': False,
                'BillName': tr.xpath('td[2]/a/text()').get().strip(),
                'BillLink': tr.css('a::attr(href)').re('(?:ARC|PRC)\w+')[0],
                'ProposerKind': tr.xpath('td[3]/text()').get(),
                'ProposerDt': tr.xpath('td[4]/text()').get(),
                'SubMitDt': tr.xpath('td[5]/text()').get(),
                'CommitteeName': tr.xpath('td[6]/@title').get().strip(),
            }

        totalItems = int(''.join(response.css('div.subContents > div > p > span::text').re(r'[0-9]')))

        # if self.i < totalItems/FORMREQUEST_PAGE_SIZE:
        #     self.i += 1
        #     yield scrapy.FormRequest(
        #         url=TARGET_URL_FINISHBILL,
        #         formdata={
        #             'strPage': f'{self.i}',
        #             'pageSize': FORMREQUEST_PAGE_SIZE,
        #         },
        #         callback=self.parse
        #     )

        urls = response.css('div.tableCol01 > table > tbody > tr').css('a::attr(href)').re('(?:ARC|PRC)\w+')
        for url in urls:
            if url in self.DroppedItem:
                print(f"url: {url}")
                continue
            print("Folling Links")
            time.sleep(1)
            yield scrapy.Request(
                TARGET_URL_DETAIL + url,
                callback=self.parse_following_urls1,
                dont_filter=True)
            yield scrapy.Request(
                TARGET_URL_DETAIL_COACTORLIST + url,
                callback=self.parse_following_urls2,
                dont_filter=True)

    def parse_following_urls1(self, response):
        yield {
            'BillDetailParsing': True,
            'BillNo': response.css('div.tableCol01 > table > tbody > tr').xpath('td[1]/text()').get(),
            'BillStep': response.css('div.boxType01 > div > span.on::text').get(),
            'SummaryContent': response.css('#summaryContentDiv::text').getall()
        }

    def parse_following_urls2(self, response):
        yield {
            'BillDetailCoActorParsing': True,
            'BillNo': response.css('div.layerInScroll.coaTxtScroll > p::text').re(r'[0-9]{6,9}')[0],
            'BillCoActorList': response.css('div.layerInScroll.coaTxtScroll > div > a::text').getall()
        }


class FinishBillSpider(scrapy.Spider):

    name = "FinishBill"
    start_urls = [
        TARGET_URL_FINISHBILL,
    ]

    custom_settings = {
        'ITEM_PIPELINES': {
            'narsbills.pipelines.FinishBillPipeline': 300
        }
    }

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.i = 1
        self.count = 0
        self.DroppedItem = set()

    def start_requests(self):
        yield scrapy.FormRequest(
            url=TARGET_URL_FINISHBILL,
            formdata={
                'strPage': f'{self.i}',
                'pageSize': "10",
            }
        )

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(FinishBillSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.item_dropped, signal=signals.item_dropped, **kwargs)
        return spider

    def item_dropped(self, **kwargs):
        self.count += 1
        try:
            self.DroppedItem.add(kwargs['item']['BillLink'])
        except Exception as error:
            print(error)


    def parse(self, response):
        for tr in response.css('div.tableCol01 > table > tbody > tr'):
            yield {
                'BillNo': tr.xpath('td[1]/text()').get(),
                'Finished': True,
                'BillName': tr.xpath('td[2]/a/text()').get().strip(),
                'BillLink': tr.css('a::attr(href)').re('(?:ARC|PRC)\w+')[0],
                'ProposerKind': tr.xpath('td[3]/text()').get(),
                'ProposerDt': tr.xpath('td[4]/text()').get(),
                'SubMitDt': tr.xpath('td[5]/text()').get(),
                'CommitteeName': tr.xpath('td[6]/@title').get().strip(),
                'ProcDt': tr.xpath('td[7]/text()').get(),
                'GeneralResult': tr.xpath('td[8]/text()').get(),
            }

        totalItems = int(''.join(response.css('div.subContents > div > p > span::text').re(r'[0-9]')))

        if self.i < 1:
            self.i += 1
            yield scrapy.FormRequest(
                url=TARGET_URL_FINISHBILL,
                formdata={
                    'strPage': f'{self.i}',
                    'pageSize': FORMREQUEST_PAGE_SIZE,
                },
                callback=self.parse
            )

        urls = response.css('div.tableCol01 > table > tbody > tr').css('a::attr(href)').re('(?:ARC|PRC)\w+')
        for url in urls:
            if url in self.DroppedItem:
                print(f"url: {url}")
                continue
            print("Folling Links")
            time.sleep(1)
            yield scrapy.Request(
                TARGET_URL_DETAIL + url,
                callback=self.parse_following_urls,
                dont_filter=True)

    def parse_following_urls(self, response):
        print(f"In parse_following_urls: ", response)
        yield {
            'BillDetailParsing': True,
            'BillNo': response.css('div.tableCol01 > table > tbody > tr').xpath('td[1]/text()').get(),
            'BillStep': response.css('div.boxType01 > div > span.on::text').get(),
            'SummaryContent': response.css('#summaryContentDiv::text').getall()
        }
