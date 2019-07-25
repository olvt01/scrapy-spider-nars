import scrapy
import time
import re


class FinishBillSpider(scrapy.Spider):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.i = 1

    name = "FinishBill"
    start_urls = [
        'http://likms.assembly.go.kr/bill/FinishBill.do',
    ]

    def start_requests(self):
        yield scrapy.FormRequest(
            url='http://likms.assembly.go.kr/bill/FinishBill.do',
            formdata={
                'strPage': f'{self.i}',
                'pageSize': "10",
            }
        )

    def parse(self, response):
        for tr in response.xpath('/html/body/div/div[2]/div[2]/div/div[3]/table/tbody/tr'):
            yield {
                'BillNo': tr.xpath('td[1]/text()').get(),
                'BillName': tr.xpath('td[2]/a/text()').get().strip(),
                'BillLink': tr.css('a::attr(href)').re(r'P\w+')[0],
                'ProposerKind': tr.xpath('td[3]/text()').get(),
                'ProposerDt': tr.xpath('td[4]/text()').get(),
                'SubMitDt': tr.xpath('td[5]/text()').get(),
                'CommittieeName': tr.xpath('td[6]/@title').get().strip(),
                'ProcDt': tr.xpath('td[7]/text()').get(),
                'GeneralResult': tr.xpath('td[8]/text()').get(),
            }

        if self.i < 1:
            self.i += 1
            yield scrapy.FormRequest(
                url='http://likms.assembly.go.kr/bill/FinishBill.do',
                formdata={
                    'strPage': f'{self.i}',
                    'pageSize': "10",
                },
                callback=self.parse
            )
            time.sleep(10)

        # urls = response.xpath('/html/body/div/div[2]/div[2]/div/div[3]/table/tbody/tr').css('a::attr(href)').re(r'P\w+')
        # for url in urls:
        #     yield scrapy.FormRequest(
        #         'http://likms.assembly.go.kr/bill/billDetail.do?billId=' + url, callback=self.parse_following_urls, dont_filter=True)

    def parse_following_urls(self, response):
        yield {
            'summaryContent': response.css('div.textType02::text').getall()
        }