# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

import fdb

class ClimPipeline(object):
	sqls = []

	def process_item(self, item, spider):
		self.sqls.append(item['sql'])
		return item

	def close_spider(self, spider):
		con = fdb.connect(dsn='C:/AppServer/SHARED.FDB', user='sysdba', password='masterkey')
		cur = con.cursor()
		insertStatement = cur.prep("update or insert into historicos (id_estacion, fecha, prec, tmax, tmin, tmed, vmax, vdir, vmed, dmed, rad, hum, eto, eva) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) matching (id_estacion,fecha)")
		cur.executemany(insertStatement,self.sqls)
		con.commit()
		con.close()
