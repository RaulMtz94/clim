# -*- coding: utf-8 -*-
from scrapy.spiders import Spider
from scrapy.http import Request, FormRequest
from datetime import date
from scrapy.selector import Selector
from glob import glob
from clim.items import ClimItem

class SpiderSpider(Spider):
	name = "spider"
	allowed_domains = ["clima.inifap.gob.mx"]
	start_urls = []
	login_url = 'http://clima.inifap.gob.mx/lnmysr/Account/Login?ReturnUrl=/lnmysr/Historicos/Datos'
	custom_settings = {
		'RETRY_TIMES': 999,
	}

	def __init__(self):
		prefix = 'http://clima.inifap.gob.mx/lnmysr/Historicos/Datos?Estado={}&Estacion={}&Mes='
		for estado_file in glob('*.txt'):
			file = open(estado_file, 'r')
			idEstado = estado_file.replace('.txt', '')
			for line in file:
				idEstacion = line.split('"')[1]
				for year in range (2018,date.today().year + 1):
					for month in range (9,date.today().month + 1):
						self.start_urls.append(prefix.format(idEstado,idEstacion) + str(month) + "&Anio=" + str(year))

	def start_requests(self):
		yield Request(url=self.login_url,callback=self.login)

	def login(self,response):
		yield FormRequest.from_response(response,formdata={'UserName': 'danielasv', 'Password': 'renzokuken'},callback=self.after_login)

	def after_login(self, response):
		for url in self.start_urls:
			self.log(url)
			yield Request(url=url,cookies=response.headers.getlist('Set-Cookie'),callback=self.parse)

	def parse(self, response):
		if 'Lo sentimos. No tenemos datos' in response.body:
			self.log('Sin datos')
		else:
			sel = Selector(response)
			id = sel.xpath("//option[@selected]").re("\d+")
			rows = sel.xpath("//table[@class='table']/tr")
			for row in rows:
				fecha = row.xpath('td[1]').re_first(r'\d+/\d+/\d+')
				if fecha: 
					item = ClimItem()
					item['sql'] = ((
						id[1],
						fecha.replace('/','.'),
						row.xpath('td[2]').re_first(r'\d+\.\d+'),
						row.xpath('td[3]').re_first(r'\d+\.\d+'),
						row.xpath('td[4]').re_first(r'\d+\.\d+'),
						row.xpath('td[5]').re_first(r'\d+\.\d+'),
						row.xpath('td[6]').re_first(r'\d+\.\d+'),
						row.xpath('td[7]').re_first(r'\d+\.\d+'),
						row.xpath('td[8]').re_first(r'\d+\.\d+'),
						row.xpath('td[9]').re_first(r'\d+\.\d+'),
						row.xpath('td[10]').re_first(r'\d+\.\d+'),
						row.xpath('td[11]').re_first(r'\d+\.\d+'),
						row.xpath('td[12]').re_first(r'\d+\.\d+'),
						row.xpath('td[13]').re_first(r'\d+\.\d+')
					))
					yield item 