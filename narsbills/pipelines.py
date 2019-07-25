# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html

import psycopg2
from scrapy.exceptions import DropItem
from narsbills.config import config


class NarsbillsPipeline(object):
    def process_item(self, item, spider):
        return item


class FinishBillPipeline(object):
    def __init__(self):
        self.billNo = []

        params = config()
        self.conn = None

        try:
            self.conn = psycopg2.connect(**params)
            self.cur = self.conn.cursor()

        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

    def open_spider(self, spider):
        try:
            self.cur.execute("select billno from finishbill;")
            rows = self.cur.fetchall()
            self.billNo = [row[0] for row in rows]
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

    def close_spider(self, spider):
        self.conn.commit()
        self.cur.close()
        if self.conn is not None:
            self.conn.close()

    def process_item(self, item, spider):
        if item['BillNo']:
            if item['BillNo'] in self.billNo:
                raise DropItem("A duplicate item : %s" % item['BillNo'])

        # SQL
        sql = """INSERT INTO FinishBill VALUES(
                        %(BillNo)s, 
                        %(BillName)s, 
                        %(BillLink)s, 
                        %(ProposerKind)s, 
                        %(ProposerDt)s, 
                        %(SubMitDt)s, 
                        %(CommittieeName)s, 
                        %(ProcDt)s, 
                        %(GeneralResult)s)
        ;"""
        try:
            self.cur.execute(sql, item)
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        return item
