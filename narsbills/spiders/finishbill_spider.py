import scrapy
import time
import re
from scrapy import signals
from scrapy import Spider
from scrapy.exceptions import CloseSpider


TARGET_URL_FINISHBILL = "http://likms.assembly.go.kr/bill/FinishBill.do"
TARGET_URL_MOORINGBILL = "http://likms.assembly.go.kr/bill/MooringBill.do"
TARGET_URL_DETAIL = "http://likms.assembly.go.kr/bill/billDetail.do?billId="
TARGET_URL_DETAIL_COACTORLIST= "http://likms.assembly.go.kr/bill/coactorListPopup.do?billId="
# scrapy shell "http://likms.assembly.go.kr/bill/MooringBill.do"


FORMREQUEST_PAGE_SIZE = "100"
ALLOWED_COMMITTEE = [
    '과학기술정보방송통신위원회',
    '문화체육관광위원회',
    '산업통상자원중소벤처기업위원회',
    '산업통상자원위원회',
    '미래창조과학방송통신위원회'
]

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
        self.sendingPage = 1
        self.count = 0
        self.DroppedItem = set()

    def start_requests(self):
        yield scrapy.FormRequest(
            url=TARGET_URL_MOORINGBILL,
            formdata={
                'strPage': f'{self.sendingPage}',
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
        url_list = []
        for tr in response.css('div.tableCol01 > table > tbody > tr'):
            if tr.xpath('td[6]/@title').get().strip() in ALLOWED_COMMITTEE:
                url_list.append(tr.css('a::attr(href)').re('(?:ARC|PRC)\w+')[0])
                billName_raw = tr.xpath('td[2]/a/text()').get().strip()
                billName= billName_raw[:len(billName_raw) - billName_raw[::-1].find('(') - 1]
                alternative = '(대안)' in billName_raw
                yield {
                    'Parsing': 'BillTable',
                    'BillNo': int(tr.xpath('td[1]/text()').get()),
                    'Finished': False,
                    'BillName': billName,
                    'Bill': billName.replace(' 일부개정법률안', '').replace(' 전부개정법률안', ''),
                    'BillLink': tr.css('a::attr(href)').re('(?:ARC|PRC)\w+')[0],
                    'ProposerKind': tr.xpath('td[3]/text()').get(),
                    'ProposerDt': tr.xpath('td[4]/text()').get(),
                    'SubMitDt': tr.xpath('td[5]/text()').get(),
                    'Committee': tr.xpath('td[6]/@title').get().strip(),
                    'Alternative': alternative,
                    'Status': 'NORM',
                }

        DropCount = 0

        # urls = response.css('div.tableCol01 > table > tbody > tr').css('a::attr(href)').re('(?:ARC|PRC)\w+')
        for url in url_list:
            if url in self.DroppedItem:
                print(f"url: {url}")
                DropCount += 1
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

        if DropCount == len(url_list) and self.sendingPage > 5:
            raise CloseSpider('Stop Spider')

        totalItems = int(''.join(response.css('div.subContents > div > p > span::text').re(r'[0-9]')))

        if self.sendingPage < totalItems/int(FORMREQUEST_PAGE_SIZE):
            self.sendingPage += 1
            yield scrapy.FormRequest(
                url=TARGET_URL_MOORINGBILL,
                formdata={
                    'strPage': f'{self.sendingPage}',
                    'pageSize': FORMREQUEST_PAGE_SIZE,
                },
                callback=self.parse
            )


    def parse_following_urls1(self, response):
        yield {
            'Parsing': 'BillDetail',
            'BillNo': int(response.css('div.tableCol01 > table > tbody > tr').xpath('td[1]/text()').get()),
            'IsGovernment': response.css('div.tableCol01 > table > tbody > tr').xpath('td[3]')[0].re(r'정부'),
            'BillStep': response.css('div.boxType01 > div > span.on::text').get(),
            'SummaryContent': ''.join(response.css('#summaryContentDiv::text').getall())
        }

    def parse_following_urls2(self, response):
        yield {
            'Parsing': 'BillDetailCoActor',
            'BillNo': int(response.css('div.layerInScroll.coaTxtScroll > p::text').re(r'[0-9]{6,9}')[0]),
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
        self.sendingPage = 1
        self.count = 0
        self.DroppedItem = set()

    def start_requests(self):
        yield scrapy.FormRequest(
            url=TARGET_URL_FINISHBILL,
            formdata={
                'strPage': f'{self.sendingPage}',
                'pageSize': FORMREQUEST_PAGE_SIZE,
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
        url_list = []
        for tr in response.css('div.tableCol01 > table > tbody > tr'):
            if tr.xpath('td[6]/@title').get().strip() in ALLOWED_COMMITTEE:
                url_list.append(tr.css('a::attr(href)').re('(?:ARC|PRC)\w+')[0])
                billName_raw = tr.xpath('td[2]/a/text()').get().strip()
                billName = billName_raw[:len(billName_raw) - billName_raw[::-1].find('(') - 1]
                alternative = '(대안)' in billName_raw
                yield {
                    'Parsing': 'BillTable',
                    'BillNo': int(tr.xpath('td[1]/text()').get()),
                    'Finished': True,
                    'Done': False,
                    'BillName': billName,
                    'Bill': billName.replace(' 일부개정법률안', '').replace(' 전부개정법률안', ''),
                    'BillLink': tr.css('a::attr(href)').re('(?:ARC|PRC)\w+')[0],
                    'ProposerKind': tr.xpath('td[3]/text()').get(),
                    'ProposerDt': tr.xpath('td[4]/text()').get(),
                    'SubMitDt': tr.xpath('td[5]/text()').get(),
                    'Committee': tr.xpath('td[6]/@title').get().strip(),
                    'ProcDt': tr.xpath('td[7]/text()').get(),
                    'GeneralResult': tr.xpath('td[8]/text()').get(),
                    'Alternative': alternative,
                    'Status': 'NORM',
                }

        DropCount = 0

        for url in url_list:
            if url in self.DroppedItem:
                print(f"url: {url}")
                DropCount += 1
                continue
            time.sleep(1)

            yield scrapy.Request(
                TARGET_URL_DETAIL + url,
                callback=self.parse_following_urls1,
                dont_filter=True)
            yield scrapy.Request(
                TARGET_URL_DETAIL_COACTORLIST + url,
                callback=self.parse_following_urls2,
                dont_filter=True)

        # if DropCount == int(FORMREQUEST_PAGE_SIZE):
        #     raise CloseSpider('Stop Spider')

        totalItems = int(''.join(response.css('body > div > div.contentWrap > div.subContents > p > span').re(r'[0-9]')))
        if self.sendingPage < totalItems/int(FORMREQUEST_PAGE_SIZE):
            self.sendingPage += 1
            yield scrapy.FormRequest(
                url=TARGET_URL_FINISHBILL,
                formdata={
                    'strPage': f'{self.sendingPage}',
                    'pageSize': FORMREQUEST_PAGE_SIZE,
                },
                callback=self.parse
            )

    def parse_following_urls1(self, response):
        status = 'NORM'

        billfile = response.css('div > table > tbody > tr > td:nth-child(4) > a::attr(href)').get()
        if billfile:
            billfile = billfile.split("'")
            billfile = billfile[1] + '?bookId=' + billfile[3] + '&type=1'

        revised = response.css('table > tbody > tr > td:nth-child(6) > a:nth-child(8)::attr(href)').get()
        if revised:
            revised = revised.split("'")
            revised = revised[1] + '?bookId=' + revised[3] + '&type=1'

        divs = response.xpath('//div')
        discarded_list = None
        alternative = None
        for div in divs:
            if div.css('h5::text').getall() and '대안반영폐기' in ''.join(div.css('h5::text').getall()):
                discarded_list = div.css('div > p > a::text').getall()
            elif div.css('h5 ::text').getall() and '대안' in ''.join(div.css('h5 ::text').getall()):
                alternative = div.css('div > a::text').getall()

        if discarded_list:
            discarded_list = [int(item[1:8]) for item in discarded_list if re.search(r'\[[0-9]{7}', item)]
            status = 'ALT'

        if alternative:
            alternative = [int(item[1:8]) for item in alternative if re.search(r'\[[0-9]{7}', item)][0]
            status = 'DISC'

        yield {
            'Parsing': 'BillDetail',
            'BillNo': int(response.css('div.tableCol01 > table > tbody > tr').xpath('td[1]/text()').get()),
            'BillStep': response.css('div.boxType01 > div > span.on::text').get(),
            'BillFile': billfile,
            'SummaryContent': ''.join(response.css('#summaryContentDiv::text').getall()),
            'DiscardedList': discarded_list,
            'Alternative': alternative,
            'Revised': revised,
            'Status': status,
        }

    def parse_following_urls2(self, response):
        yield {
            'Parsing': 'BillDetailCoActor',
            'BillNo': int(response.css('div.layerInScroll.coaTxtScroll > p::text').re(r'[0-9]{6,9}')[0]),
            'BillCoActorList': response.css('div.layerInScroll.coaTxtScroll > div > a::text').getall()
        }
