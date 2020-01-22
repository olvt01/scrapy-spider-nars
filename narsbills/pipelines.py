# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html

import psycopg2
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
            self.cur.execute("SELECT BillNo, Done FROM billview;")
            rows = self.cur.fetchall()
            # self.billNo = {'200000': True, '200001': False, ...}
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
        # Parsing 'http://likms.assembly.go.kr/bill/MooringBill.do'
        if item.get('Parsing') == 'BillTable':
            if item['Alternative']:
                raise DropItem("We don't scrape Alternative : %s" % item['BillNo'])
            # Check If the item is already scraped
            if item['BillNo'] and self.billNo.get(item['BillNo']):
                raise DropItem("Duplicate item : %s" % item['BillNo'])

            # Check if the item is only missing the SubMitDt and CommitteeName
            if item['BillNo'] and self.billNo.get(item['BillNo']) is False:
                if item['Committee']:
                    self.update_committee(item)
                    raise DropItem("Item is just done: %s" % item['BillNo'])
                else:
                    raise DropItem("Item is not ready: %s" % item['BillNo'])

            self.insert_billview(item)

            # Check if the bill is done
            if item['BillNo'] and item.get('Committee') != '':
                self.update_billview_item_is_done(item)

        if item.get('Parsing') == 'BillDetail':
            self.update_bill_detail(item)

        if item.get('Parsing') == 'BillDetailCoActor':
            self.update_bill_detail_coactor(item)

        return item

    def insert_bill(self, item):
        sql = """INSERT INTO bill(bill, committee_id)
                    VALUES ((%s), (SELECT id FROM core_committee WHERE committee = (%s)))
        """
        try:
            self.cur.execute(sql, (item['Bill'], item['Committee'],))
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        self.conn.commit()

    def insert_billview(self, item):
        try:
            sql = "SELECT id FROM bill WHERE bill = (%s);"
            self.cur.execute(sql, (item['Bill'],))
            rows = self.cur.fetchall()

            # If there is no bill in the database, update it first.
            if len(rows) == 0:
                self.insert_bill(item)
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

        sql = """INSERT INTO billview(
                    BillNo, BillName, BillLink, ProposerKind,
                    ProposerDt, SubMitDt, Finished, status, Committee_id, bill_id
                    )
                 VALUES(
                    %(BillNo)s, %(BillName)s,  %(BillLink)s,  %(ProposerKind)s,
                    %(ProposerDt)s, %(SubMitDt)s, %(Finished)s, %(Status)s, 
                    (SELECT id FROM core_committee WHERE committee = %(Committee)s), 
                    (SELECT id FROM bill WHERE bill = %(Bill)s)
                    );
        """
        try:
            self.cur.execute(sql, item)
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

        self.conn.commit()

    def update_committee(self, item):
        sql = """UPDATE billview
                    SET SubMitDt = (%s), Committee = (SELECT id FROM core_committee WHERE committee = (%s)), Done = (%s)
                    WHERE BillNo = (%s)
        """
        try:
            self.cur.execute(sql, (item['SubMitDt'], item['Committee'], True, item['BillNo'],))
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

        self.conn.commit()

    def update_billview_item_is_done(self, item):
        sql = """UPDATE billview
                    SET Done = (%s)
                    WHERE BillNo = (%s)"""
        try:
            self.cur.execute(sql, (True, item['BillNo'],))
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

        self.conn.commit()

    def update_bill_detail(self, item):
        # Parsing 'http://likms.assembly.go.kr/bill/billDetail.do?billId='
        sql = """UPDATE billview
                    SET SummaryContent = (%s), BillStep = (%s)
                    WHERE BillNo = (%s)"""
        try:
            self.cur.execute(sql, (item['SummaryContent'], item['BillStep'], item['BillNo'],))
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

        self.conn.commit()

    def update_bill_detail_coactor(self, item):
        # Parsing 'http://likms.assembly.go.kr/bill/coactorListPopup.do?billId='
        sql = """UPDATE billview
                    SET CoActors = (%s)
                    WHERE BillNo = (%s)"""
        try:
            self.cur.execute(sql, (' '.join(item['BillCoActorList']), item['BillNo'],))
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

        self.conn.commit()


class FinishBillPipeline(object):
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
            self.cur.execute("SELECT BillNo, Finished, Done FROM billview;")
            rows = self.cur.fetchall()
            # self.billNo = {'200000': True, '200001': False, ...}
            self.billNo = {row[0]: (row[1], row[2]) for row in rows}
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

    def close_spider(self, spider):
        self.conn.commit()

        # Update LastUpdated Field
        # Select billno, proposerdt, submitdt, procdt from billview
        # Update bill set lastupdated where billno

        try:
            self.cur.execute("SELECT count(*), bill_id FROM billview group by bill_id;")
            rows = self.cur.fetchall()
            bills = {x[1]: {'count': x[0], 'lastupdated': ''} for x in rows}

            self.cur.execute("SELECT billno, proposerdt, submitdt, procdt, bill_id FROM billview;")
            rows = self.cur.fetchall()

            billviews = {x[0]: {'lastupdated': x[3] or x[2] or x[1]} for x in rows}

            for row in rows:
                if row[3]:
                    if row[3] > bills[row[4]]['lastupdated']:
                        bills[row[4]]['lastupdated'] = row[3]
                        continue
                if row[2]:
                    if row[2] > bills[row[4]]['lastupdated']:
                        bills[row[4]]['lastupdated'] = row[2]
                        continue
                if row[1]:
                    if row[1] > bills[row[4]]['lastupdated']:
                        bills[row[4]]['lastupdated'] = row[1]
                        continue
            print(f'bills2: {bills}')

            for bill in bills:
                # print(bill)
                # print(bills[bill]['count'])
                # print(bills[bill]['lastupdated'])
                sql = """UPDATE bill
                            SET count = (%s), lastupdated = (%s)
                            WHERE id = (%s)
                """
                try:
                    self.cur.execute(sql, (bills[bill]['count'], bills[bill]['lastupdated'], bill,))
                except (Exception, psycopg2.DatabaseError) as error:
                    print(error)

                self.conn.commit()

            for billview in billviews:
                sql = """UPDATE billview
                            SET lastupdated = (%s)
                            WHERE billno = (%s)
                """

                try:
                    self.cur.execute(sql, (billviews[billview]['lastupdated'], billview,))
                except (Exception, psycopg2.DatabaseError) as error:
                    print(error)

                self.conn.commit()


        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

        self.cur.close()
        if self.conn is not None:
            self.conn.close()

    def process_item(self, item, spider):
        """
        1. 계류의안 (Scraped) -> 처리의안 (Finished=False, Done=True) => update bill + update bill detail (+ update bill detail coactor)
        2. 처리의안 (Scraped), 대안 (Scraped) (Finished=True, Done=True) => Skip
        3. 처리의안 Not yet => insert bill + update bill detail + update bill detail coactor
        4. 대안 Not yet => insert bill + update bill detail (+ update bill detail coactor)

        insert bill, update bill => Set Finished = True, Done = False
        update bill detail => Set Done = True
        """
        # Parsing 'http://likms.assembly.go.kr/bill/FinishBill.do'
        if item.get('Parsing') == 'BillTable':
            if item.get('BillNo') and self.billNo.get(item['BillNo']):
                if self.billNo.get(item['BillNo'])[0] and self.billNo.get(item['BillNo'])[1]:
                    raise DropItem("The item is already scraped : %s" % item['BillNo'])
                self.finish_update_billview(item)
            else:
                self.finish_insert_billview(item)

        if item.get('Parsing') == 'BillDetail':
            self.finish_update_bill_detail(item)

        if item.get('Parsing') == 'BillDetailCoActor':
            self.finish_update_bill_detail_coactor(item)

        return item

    def finish_insert_bill(self, item):
        sql = """INSERT INTO bill(bill, committee_id)
                    VALUES ((%s), (SELECT id FROM core_committee WHERE committee = (%s)))
        """
        try:
            self.cur.execute(sql, (item['Bill'], item['Committee'],))
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        self.conn.commit()

    def finish_insert_billview(self, item):
        try:
            sql = "SELECT id FROM bill WHERE bill = (%s);"
            self.cur.execute(sql, (item['Bill'],))
            rows = self.cur.fetchall()
            # If there is no bill in the database, update it first.
            if len(rows) == 0:
                self.finish_insert_bill(item)
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

        sql = """INSERT INTO billview(
                    BillNo, BillName, BillLink, ProposerKind,
                    ProposerDt, SubMitDt, Finished, Done, status, ProcDt, GeneralResult, 
                    Committee_id, bill_id
                    )
                 VALUES(
                    %(BillNo)s, %(BillName)s,  %(BillLink)s,  %(ProposerKind)s,
                    %(ProposerDt)s, %(SubMitDt)s, %(Finished)s, %(Done)s, %(Status)s,
                    %(ProcDt)s, %(GeneralResult)s,
                    (SELECT id FROM core_committee WHERE committee = %(Committee)s), 
                    (SELECT id FROM bill WHERE bill = %(Bill)s)
                    );
        """
        try:
            self.cur.execute(sql, item)
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

        self.conn.commit()

    def finish_update_billview(self, item):
        sql = """UPDATE billview
                    SET ProposerKind = (%s), Status = (%s), ProcDt = (%s), GeneralResult = (%s), Finished= (%s), Done = (%s)
                    WHERE BillNo = (%s)"""
        try:
            self.cur.execute(sql, (item['ProposerKind'], item['Status'], item['ProcDt'], item['GeneralResult'], item['Finished'], item['Done'], item['BillNo'],))
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

        self.conn.commit()

    def finish_update_bill_detail(self, item):
        sql = """UPDATE billview
                    SET BillStep = (%s), SummaryContent = (%s), Status = (%s), Done = (%s)
                    WHERE BillNo = (%s)
        """
        Done = False
        if item['BillStep'] in ['공포', '대안반영폐기', '철회', '폐기', '부결']:
            Done = True
        try:
            self.cur.execute(sql, (item['BillStep'], item['SummaryContent'], item['Status'], Done, item['BillNo'],))
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

        self.conn.commit()

        if item['DiscardedList']:
            for discarded in item['DiscardedList']:
                self.finish_update_alternative_ALT(discarded, item['BillNo'])

        if item['Alternative']:
            self.finish_update_alternative_DISC(item['Alternative'], item['BillNo'])

    def finish_update_alternative_ALT(self, discarded, billno):
        sql = """UPDATE billview
                    SET alternative_id = (%s), Status = (%s)
                    WHERE BillNo = (%s)
                """
        try:
            self.cur.execute(sql, (billno, 'DISC', discarded,))
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        self.conn.commit()

    def finish_update_alternative_DISC(self, alternative, billno):
        sql = """UPDATE billview
                    SET alternative_id = (%s), Status = (%s)
                    WHERE BillNo = (%s)
                """
        try:
            self.cur.execute(sql, (alternative, 'DISC', billno,))
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        self.conn.commit()

    def finish_update_bill_detail_coactor(self, item):
        sql = """UPDATE billview
                    SET CoActors = (%s)
                    WHERE BillNo = (%s)"""
        try:
            self.cur.execute(sql, (' '.join(item['BillCoActorList']), item['BillNo'],))
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        self.conn.commit()
