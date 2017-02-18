import psycopg2
import pandas as pd
from io import StringIO
from lxml import etree
from time import sleep
from random import randint
from selenium import webdriver
from sqlalchemy import create_engine
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.firefox.firefox_binary import FirefoxBinary
from stem import Signal
from stem.control import Controller

# First, we run TOR.
controller = Controller.from_port(port=9051)

def renew_tor():
    controller.authenticate('Behemoth3d')
    controller.signal(Signal.NEWNYM)

# def test_tor():
#     browser.get('http://www.whatsmyip.org/')


def google_pdf_parser(all_links, page, parser, error_appears, engine,
                      browser, a_keyword):
    dict_links = []
    _next_page_exist = True
    print("Kukusiki!")
    _all_links = all_links
    print("_all_links = ", _all_links)
    print("dict_links = ", dict_links)
    while _next_page_exist:
        print('next_page_exist is {0}'.format(_next_page_exist))
        print("_all_links = ", _all_links)
        print("dict_links = ", dict_links)
        try:
            page += 1
            page_xml = browser.page_source
            tree = etree.parse(StringIO(page_xml), parser)
            pdfki = tree.xpath(".//div[@class='g']")
            for a_link in range(0, len(browser.find_elements_by_xpath(".//div[@class='g']//h3/a[@href]"))-1):
                print(a_link, "  ", ''.join(pdfki[a_link].xpath(".//h3/a/@href")))
                if '.pdf' in ''.join(pdfki[a_link].xpath(".//h3/a/@href")):
                    if ''.join(pdfki[a_link].xpath(".//h3/a/@href")) not in _all_links:
                        print(a_link)
                        print(pdfki[a_link].xpath(".//h3/a/@href"),
                              '\n',
                              pdfki[a_link].xpath('.//div[@class="f slp"]/text()'),
                              '\n',
                              pdfki[a_link].xpath('.//span[@class = "st"]/text()'),
                              '\n',
                              tree.xpath('//div[@id="resultStats"]/text()'),
                              '\n',
                              pdfki[a_link].xpath(
                                  './/div[@class="f slp"]/a[@href][contains(text(), "Related articles")]/@href'),
                              '\n',
                              pdfki[a_link].xpath(
                                  './/div[@class="f slp"]/a[@href][contains(text(), "Cited by")]/@href'),
                              '\n')
                        _all_links.append(''.join(pdfki[a_link].xpath(".//h3/a/@href")))
                        dict_links.append({'pdf_link': ''.join(pdfki[a_link].xpath(".//h3/a/@href")),
                                           'title': ''.join(pdfki[a_link].xpath(".//h3/a[@href]/text()")),
                                           'google_page': page,
                                           'author_and_date': ''.join(
                                               pdfki[a_link].xpath('.//div[@class="f slp"]/text()')),
                                           'result_stats': ''.join(tree.xpath('//div[@id="resultStats"]/text()')),
                                           'pdf_cited_by': ''.join(pdfki[a_link].xpath(
                                               './/div[@class="f slp"]/a[@href][contains(text(), "Cited by")]/@href')),
                                           'pdf_related_articles': ''.join(pdfki[a_link].xpath(
                                               './/div[@class="f slp"]/a[@href][contains(text(), "Related articles")]/@href')),
                                           'pdf_snippets': ' '.join(
                                               pdfki[a_link].xpath('.//span[@class = "st"]/text()')),
                                           'keyword': a_keyword})
            sql_table = pd.DataFrame(dict_links)
            dict_links = []
            sql_table.to_sql('google_pdfs', engine, if_exists='append', index=False)
            button = browser.find_element_by_xpath('//div[@id="foot"]//span[text()="Next"]')
            button.click()
            sleep(randint(10, 15))
        except NoSuchElementException as e:
            sleep(15)
            print('Hey!I am here!')
            page_xml_1 = browser.page_source
            tree_1 = etree.parse(StringIO(page_xml_1), parser)
            I_am_not_a_robot = tree_1.xpath('//*[@id = "g-recaptcha-response"]')
            print(I_am_not_a_robot)
            if len(I_am_not_a_robot) != 0:
                print('I have got a captcha!')
                sleep(randint(45, 60))
                I_am_not_a_robot = []
                print('We solve the captcha! We are good!', '\n', 'I_am_not_a_robot = ', I_am_not_a_robot)
                error_appears, _all_links = google_pdf_parser(_all_links, page, parser,
                                                             error_appears, engine,
                                                             browser, a_keyword)
            elif len(tree_1.xpath('.//text()[contains(.,"s an error.")]')) != 0:
                print('We have got an error. It is needed to be solved.')
                error_appears = True
                _next_page_exist = False
            else:
                print('Ura!', '\n', 'There is no next page!')
                sleep(20)
                print('Let us check if any error appears!')
                page_xml_2 = browser.page_source
                tree_2 = etree.parse(StringIO(page_xml_2), parser)
                if len(tree_2.xpath('.//text()[contains(.,"s an error.")]')) != 0 and len(tree_2.xpath(
                        ".//div[@class='g']//h3/a[@href]")) == 0:
                    print("Again an error...")
                    error_appears, _all_links = google_pdf_parser(_all_links, page,
                                                                 parser, error_appears, engine,
                                                                 browser, a_keyword)
                    error_appears = True
                print('next_page_exist is {0}'.format(_next_page_exist))
                print("No. There is no error.")
                _next_page_exist = False
                print("Another keyword is coming!", "\n", "dict_links= ", dict_links, "\n", "next_page_exist = ",
                      _next_page_exist)
                print('next_page_exist is {0}'.format(_next_page_exist))
    return error_appears, _all_links


def browser_run(a_keyword, parser, engine):
    all_links = []
    print(a_keyword)
    error = False
    binary = FirefoxBinary('/usr/bin/firefox')
    fp = webdriver.FirefoxProfile('/home/user/.mozilla/firefox/vmofja5l.default/')
    browser = webdriver.Firefox(firefox_profile=fp, firefox_binary=binary)
    browser.get("http://www.google.com/ncr")
    page = 0
    elem = browser.find_element_by_name("q")
    elem.clear()
    sleep(randint(3, 5))
    elem.send_keys("%s filetype:pdf" % a_keyword)  # in search box of the site
    elem.send_keys(Keys.RETURN)
    sleep(randint(3, 5))
    assert "No results found." not in browser.page_source
    print("We have typed the keyword")
    error, all_links = google_pdf_parser(all_links, page,
                                         parser, error, engine, browser, a_keyword)
    print("%s keyword has been captured!" % a_keyword)
    sleep(3)
    print("We are going to close the browser!")
    browser.quit()
    if error:
        print("Is an error? ", error, "\n", "We are going to change IP!")
        renew_tor()
        print(a_keyword)
        browser_run(a_keyword, parser, engine)
        error_keyword.append(a_keyword)
    sleep(10)
    return


engine = create_engine(
    'postgresql://textminer:Infrared spectroscopy@ec2-54-202-180-1.us-west-2.compute.amazonaws.com:5432/pdfs')

DB = {
    'drivername': 'postgres',
    'database': 'pdfs',
    'host': 'ec2-54-202-180-1.us-west-2.compute.amazonaws.com',
    'port': '5432',
    'username': 'textminer',
    'password': "'Infrared spectroscopy'"
}

dsn = "host={} dbname={} user={} password={}".format(DB['host'],
                                                     DB['database'],
                                                     DB['username'],
                                                     DB['password'])
connection = psycopg2.connect(dsn)
cur = connection.cursor()

key_df = pd.read_csv('/home/user/PycharmProjects/Selenium/keywords.txt',
                     sep=',', header=None)
keywords = key_df[0].tolist()
error_keyword = []
final_list_of_links = []
parser = etree.HTMLParser()

for i in range(0, len(keywords)):
    a_keyword = keywords[i]
    browser_run(a_keyword, parser, engine)

cur.close()
connection.close()




# for i in range(0, len(keywords)):
#     print(keywords[i])
#     error = False
#     binary = FirefoxBinary('/usr/bin/firefox')
#     fp = webdriver.FirefoxProfile('/home/user/.mozilla/firefox/vmofja5l.default/')
#     browser = webdriver.Firefox(firefox_profile=fp, firefox_binary=binary)
#     browser.get("http://www.google.com/ncr")
#     page = 1
#     elem = browser.find_element_by_name("q")
#     elem.clear()
#     sleep(randint(3, 5))
#     elem.send_keys("%s filetype:pdf" % keywords[i])  # in search box of the site
#     elem.send_keys(Keys.RETURN)
#     sleep(randint(3, 5))
#     assert "No results found." not in browser.page_source
#     next_page_exist = True
#     dict_links_full, error, all_links = google_pdf_parser(dict_links_full, dict_links, all_links, page,
#                                                           next_page_exist, parser, error, engine)
#     print("%s keyword has been captured!" % keywords[i])
#     browser.quit()
#     if error:
#         print("Is an error? ", error,"\n","We are going to change IP!")
#         renew_tor()
#         print(keywords[i])
#         error_keyword.append(keywords[i])
#     sleep(10)