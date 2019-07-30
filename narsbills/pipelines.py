# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html

import psycopg2
# from psycopg2 import sql
from scrapy.exceptions import DropItem
from narsbills.config import config


class MooringBillPipeline(object):
    def __init__(self):
        self.billNo = []
        self.conn = None
        params = config()

        try:
            self.conn = psycopg2.connect(**params)
            self.cur = self.conn.cursor()

        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

    def open_spider(self, spider):
        # Get the BillNo and Done from the DB to check duplicate items
        try:
            self.cur.execute("SELECT BillNo, Done FROM FinishBill;")
            rows = self.cur.fetchall()
            self.billNo = {row[0]: row[1] for row in rows}
            print(self.billNo)
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

    def close_spider(self, spider):
        self.conn.commit()
        self.cur.close()
        if self.conn is not None:
            self.conn.close()

    def process_item(self, item, spider):
        # Parsing 'http://likms.assembly.go.kr/bill/billDetail.do?billId='
        if item.get('BillDetailParsing'):
            print(f"In BillDetailParsing: {item}")
            sql = """UPDATE FinishBill
                        SET SummaryContent = (%s), BillStep = (%s)
                        WHERE BillNo = (%s)"""
            try:
                self.cur.execute(sql, (''.join(item['SummaryContent']), item['BillStep'], item['BillNo'],))
            except (Exception, psycopg2.DatabaseError) as error:
                print(error)

        # Parsing 'http://likms.assembly.go.kr/bill/coactorListPopup.do?billId='
        if item.get('BillDetailCoActorParsing'):
            print(f"In BillDetailCoActorParsing: {item}")
            sql = """UPDATE FinishBill
                        SET SummaryContent = (%s), BillStep = (%s)
                        WHERE BillNo = (%s)"""
            try:
                self.cur.execute(sql, (''.join(item['SummaryContent']), item['BillStep'], item['BillNo'],))
            except (Exception, psycopg2.DatabaseError) as error:
                print(error)

        # Parsing 'http://likms.assembly.go.kr/bill/MooringBill.do'
        else:
            # Check If the item is already scraped
            if item['BillNo'] and self.billNo.get(item['BillNo']):
                raise DropItem("Duplicate item : %s" % item['BillNo'])

            # Check if the item is only missing the SubMitDt and CommitteeName
            if item['BillNo'] and self.billNo.get(item['BillNo']) == False:
                if item['CommitteeName']:
                    sql = """UPDATE FinishBill
                                SET SubMitDt = (%s), CommitteeName = (%s), Done = (%s)
                                WHERE BillNo = (%s)
                    """
                    try:
                        self.cur.execute(sql, (item['SubMitDt'], item['CommitteeName'], True,))
                    except (Exception, psycopg2.DatabaseError) as error:
                        print(error)
                    raise DropItem("Item is just done: %s" % item['BillNo'])
                else:
                    raise DropItem("Item is not ready: %s" % item['BillNo'])

            sql = """INSERT INTO FinishBill(
                        BillNo, BillName, BillLink, ProposerKind,
                        ProposerDt, SubMitDt, CommitteeName, Finished
                        )
                     VALUES(
                        %(BillNo)s, %(BillName)s,  %(BillLink)s,  %(ProposerKind)s,
                        %(ProposerDt)s,  %(SubMitDt)s,  %(CommitteeName)s,  %(Finished)s
                        );
            """
            try:
                self.cur.execute(sql, item)
            except (Exception, psycopg2.DatabaseError) as error:
                print(error)

            # Check if the bill is done
            if item['BillNo'] and item.get('CommitteeName'):
                sql = """UPDATE FinishBill
                            SET Done = (%s)
                            WHERE BillNo = (%s)"""
                try:
                    self.cur.execute(sql, (True, item['BillNo'],))
                except (Exception, psycopg2.DatabaseError) as error:
                    print(error)
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
            self.cur.execute("SELECT BillNo FROM FinishBill;")
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
        # Parsing 'http://likms.assembly.go.kr/bill/billDetail.do'
        if item.get('BillDetailParsing'):
            sql = """UPDATE FinishBill
                        SET SummaryContent = (%s), BillStep = (%s)
                        WHERE BillNo = (%s)"""
            try:
                self.cur.execute(sql,
                                 (''.join(item['SummaryContent']), item['BillStep'], item['BillNo'],)
                )
            except (Exception, psycopg2.DatabaseError) as error:
                print(error)

        # Parsing 'http://likms.assembly.go.kr/bill/FinishBill.do'
        else:
            # Duplicate filter
            if item['BillNo']:
                if self.billNo.get(item['BillNo']):
                    raise DropItem("A duplicate item : %s" % item['BillNo'])

            # Check if item is done
            Done = False
            if item.get('CommitteeName'):
                Done = True
            item.update(Done=Done)

            # INSERT items
            sql = """INSERT INTO FinishBill(
                        BillNo, BillName, BillLink, ProposerKind, ProposerDt,
                        SubMitDt, CommitteeName, Finished, ProcDt, GeneralResult, Finished, Done
                        )
                     VALUES(
                        %(BillNo)s, %(BillName)s,  %(BillLink)s,  %(ProposerKind)s, %(ProposerDt)s
                        %(SubMitDt)s,  %(CommitteeName)s,  %(ProcDt)s, %(GeneralResult)s, %(Finished)s, %(Done)s
                        );
            """
            try:
                self.cur.execute(sql, item)
            except (Exception, psycopg2.DatabaseError) as error:
                print(error)
        return item
