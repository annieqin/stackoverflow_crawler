# coding: utf-8


import re

from peewee import *
from pyquery import PyQuery as pq
import requests
from selenium import webdriver

mysql_db = MySQLDatabase('github', host='127.0.0.1', port=3306, user='root')


class BaseModel(Model):
    class Meta:
        database = mysql_db


class Proxy(BaseModel):
    INVALID = 0
    ANONYMOUS = 1
    NON_ANONYMOUS = 2

    STATUS_CHOICE = (
        (INVALID, '不可用的'),
        (ANONYMOUS, '匿名的'),
        (NON_ANONYMOUS, '非匿名的')
    )

    type = CharField(max_length=11, default='')
    ip = CharField(max_length=15, default='')
    port = IntegerField(default=0)
    status = IntegerField(choices=STATUS_CHOICE, default=INVALID)
    out_ip = CharField(max_length=255, default='')


class ProxyList(BaseModel):
    REQUEST = 1
    DRIVER = 2

    CRAWLING_METHOD_CHOICE = (
        (REQUEST, 'request'),
        (DRIVER, 'driver')
    )

    url = CharField(max_length=255, default='')
    crawling_method = IntegerField(choices=CRAWLING_METHOD_CHOICE,
                                   default=REQUEST)


def create_tables():
    mysql_db.connect()
    mysql_db.create_tables([Proxy, ProxyList], safe=True)


def pro_status(my_ip, pro):
    try:
        r = requests.get(
            'http://httpbin.org/ip',
            timeout=2,
            proxies={pro['type']: '%s://%s:%s' % (pro['type'], pro['ip'], pro['port'])},
        )
        out_ip = r.json()['origin']
        pro['out_ip'] = out_ip
        if my_ip in out_ip:
            pro['status'] = Proxy.NON_ANONYMOUS
        else:
            pro['status'] = Proxy.ANONYMOUS

    except:
        pro['status'] = Proxy.INVALID
        pro['out_ip'] = ''
    return pro


def save_pro(pro):
    p = Proxy.get_or_create(ip=pro['ip'], type=pro['type'])[0]
    p.type = pro['type']
    p.port = pro['port']
    p.status = pro['status']
    p.out_ip = pro['out_ip']
    p.save()


def parse_page(ths, rows, column):
    type_index = None
    port_index = None

    for i in range(len(ths)):
        if ths.eq(i).text():
            if re.match('https', ths.eq(i).text(), re.IGNORECASE):
                type_index = i
            if re.match('port|proxy port', ths.eq(i).text(), re.IGNORECASE):
                port_index = i

    pro = []
    for i in range(len(rows)):
        ip = None
        port = None
        type = None

        cols = rows.eq(i).find(column)
        for j in range(len(cols)):
            col = cols.eq(j)
            # td.children().remove()
            col.find('script').remove()
            # value = td.text().strip()
            value = col.text().replace(' ', '')

            if value:
                re_ip = re.match(r'^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$', value)
                if re_ip:
                    ip = value
                    continue

                re_type = re.match('http|https|socks4|socks5', value, re.IGNORECASE)
                if re_type:
                    type = value.lower()
                    continue

                re_ip_port = re.search(r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}:\d+', value)
                if re_ip_port:
                    ip = re_ip_port.group().split(':')[0]
                    port = re_ip_port.group().split(':')[1]
                    continue

        if port_index:
            port = cols.eq(port_index).text()

        if type_index and not type:
            yn = cols.eq(type_index).text()
            type = 'https' if yn == 'yes' else 'http'

        if ip and port:
            pro.append({'ip': ip, 'port': port, 'type': type})
    return pro


def check_pros(pros, my_ip):
    for i in pros:
        if i['type']:
            proxy = pro_status(my_ip, i)
            save_pro(proxy)

        if not i['type']:
            i['type'] = 'http'
            proxy = pro_status(my_ip, i)
            save_pro(proxy)

            i['type'] = 'https'
            proxy = pro_status(my_ip, i)
            save_pro(proxy)


def test():
    driver = webdriver.Chrome()
    proxy_list = ProxyList.select()

    r = requests.get('http://httpbin.org/ip')
    my_ip = r.json()['origin']

    for i in range(338):
        print 'CRAWLING: ' + proxy_list[i].url
        if proxy_list[i].crawling_method == ProxyList.REQUEST:
            try:
                res = requests.get(proxy_list[i].url)
                page = pq(res.text)

            except:
                print '  FAILURE'
                continue

        if proxy_list[i].crawling_method == ProxyList.DRIVER:
            try:
                driver.get(proxy_list[i].url)
                page = pq(driver.page_source)

            except:
                print '  FAILURE'
                continue

        page.remove_namespaces()

        th = page('table th')
        tr = page('table tr')
        ul = page('ul')

        if tr:
            proxies = parse_page(th, tr, 'td')
            check_pros(proxies, my_ip)
            print '  SUCCESS'

        if ul:
            proxies = parse_page(th, ul, 'li')
            check_pros(proxies, my_ip)
            print '  SUCCESS'

if __name__ == '__main__':
    test()