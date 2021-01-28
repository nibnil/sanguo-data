import logging
from time import sleep

import requests
from lxml import etree

from mongodb_helper import MongodbHelper

file_handler = logging.FileHandler('sanguo-data.log')
stream_handler = logging.StreamHandler()
logging.basicConfig(level=logging.INFO,
                    format='%(levelname)-s - %(asctime)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    handlers=[file_handler, stream_handler])

GW_URL = 'http://so.17173.com/monster/gw{0}.shtml'
BOSS_URL = 'http://so.17173.com/monster/boss{0}.shtml'


def get_data(url, html_type=0):
    logging.info(f'GetData, url: {url}')
    data = list()
    resp = requests.get(url)
    text = resp.text
    html = etree.HTML(text)
    if html_type == 0:
        elements = html.xpath(
            '//html/body/table[2]/tr[2]/td[2]/table/tr[2]/td/table/tr[1]/td/table/tr[3]/td/table/tbody/tr[2]/td/table/tbody/tr[1]/td/table/tr')
    else:
        elements = html.xpath(
            '//html/body/table[2]/tr[2]/td[2]/table/tr[2]/td/table/tr[1]/td/table/tr[3]/td/table/tbody/tr[2]/td/table/tr')
    first = True
    for element in elements:
        if first is True:
            name = element.xpath('td/table/tbody/tr[2]/td[1]/text()')[0]
            level = element.xpath('td/table/tbody/tr[2]/td[2]/text()')[0]
            try:
                level = int(level)
            except Exception as ex:
                pass
            maps = element.xpath('td/table/tbody/tr[2]/td[6]/text()')[0]
            maps = maps.split('，')
            products = element.xpath('td/table/tbody/tr[2]/td[7]/text()')[0]
            products = products.split(',')
            for product in products:
                if product == '':
                    products.remove(product)
            first = False
        else:
            name = element.xpath('td/table/tbody/tr[3]/td[1]/text()')[0]
            level = element.xpath('td/table/tbody/tr[3]/td[2]/text()')[0]
            try:
                level = int(level)
            except Exception as ex:
                pass
            maps = element.xpath('td/table/tbody/tr[3]/td[6]/text()')[0]
            maps = maps.split('，')
            products = element.xpath('td/table/tbody/tr[3]/td[7]/text()')[0]
            products = products.split(',')
            for product in products:
                if product == '':
                    products.remove(product)
        data.append({'name': name,
                     'level': level,
                     'maps': maps,
                     'products': products})
    return data


def get_boss_data(url, html_type=0):
    logging.info(f'GetData, url: {url}')
    data = list()
    resp = requests.get(url)
    text = resp.text
    html = etree.HTML(text)
    elements = html.xpath(
        '//html/body/table[2]/tr[2]/td[2]/table/tr[2]/td/table/tr[1]/td/table/tr[3]/td/table/tbody/tr[2]/td/table/tr')
    first = True
    for element in elements:
        if first is True:
            name = element.xpath('td/table/tbody/tr[2]/td[1]/text()')[0]
            level = element.xpath('td/table/tbody/tr[2]/td[2]/text()')[0]
            try:
                level = int(level)
            except Exception as ex:
                pass
            maps = None
            products = element.xpath('td/table/tbody/tr[2]/td[7]/text()')[0]
            products = products.split(',')
            for product in products:
                if product == '':
                    products.remove(product)
            first = False
        else:
            name = element.xpath('td/table/tbody/tr[3]/td[1]/text()')[0]
            level = element.xpath('td/table/tbody/tr[3]/td[2]/text()')[0]
            try:
                level = int(level)
            except Exception as ex:
                pass
            # maps = element.xpath('td/table/tbody/tr[3]/td[6]/text()')[0]
            # maps = maps.split('，')
            maps = None
            products = element.xpath('td/table/tbody/tr[3]/td[7]/text()')[0]
            products = products.split(',')
            for product in products:
                if product == '':
                    products.remove(product)
        print({'name': name,
                     'level': level,
                     'maps': maps,
                     'products': products})
        data.append({'name': name,
                     'level': level,
                     'maps': maps,
                     'products': products})
    return data


def main():
    db = MongodbHelper()
    # for i in range(1, 62):
    #     index = "{:0>2d}".format(i)
    #     url = GW_URL.format(index)
    #     if i == 1:
    #         data = get_data(url, html_type=0)
    #     else:
    #         data = get_data(url, html_type=1)
    #     result = db.do_bulk_upsert(col='monster',
    #                                data=data,
    #                                filter_key=['name', 'level', 'maps', 'products'],
    #                                set_on_insert_key=['name', 'level', 'maps', 'products'])
    #     print(result)
    for i in range(1, 56):
        index = "{:0>2d}".format(i)
        url = BOSS_URL.format(index)
        if i == 1:
            data = get_boss_data(url, html_type=0)
        else:
            data = get_boss_data(url, html_type=1)
        result = db.do_bulk_upsert(col='monster',
                                   data=data,
                                   set_key=[],
                                   filter_key=['name'],
                                   set_on_insert_key=['name', 'level', 'maps', 'products'])
        print(result)
        sleep(5)


if __name__ == '__main__':
    main()
