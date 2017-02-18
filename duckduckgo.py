import psycopg2
import pandas as pd
from io import StringIO
from lxml import etree
from time import sleep
from random import randint
from selenium import webdriver
from sqlalchemy import create_engine
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.firefox.firefox_binary import FirefoxBinary



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

TOPIC = 'mining'
key_df = pd.read_csv('/home/user/PycharmProjects/Selenium/keywords.txt', sep=',', header=None)
keywords = key_df[0].tolist()
parser = etree.HTMLParser()
binary = FirefoxBinary('/usr/bin/firefox')
fp = webdriver.FirefoxProfile('/home/user/.mozilla/firefox/vmofja5l.default/')
browser = webdriver.Firefox(firefox_profile=fp, firefox_binary=binary)
SEARCH_ENGINE = "https://duckduckgo.com/"
browser.get(SEARCH_ENGINE)
page = 0
# empty = []
# table = pd.DataFrame(empty)
# writer = pd.ExcelWriter('/home/user/PycharmProjects/Selenium/PDF_links/duck_duck_go.xlsx')
# table.to_excel(writer, 'links', index=False)
# writer.save()

for a_keyword in keywords:
    elem = browser.find_element_by_name("q")
    elem.clear()
    sleep(randint(3, 5))
    elem.send_keys("%s filetype pdf" % a_keyword)  # in search box of the site
    elem.send_keys(Keys.RETURN)
    sleep(randint(3, 5))
    assert "No results found." not in browser.page_source
    dict_links = []
    all_links = []
    lenOfPage = browser.execute_script(
        "window.scrollTo(0, document.body.scrollHeight);var lenOfPage=document.body.scrollHeight;return lenOfPage;")
    match = False
    while (match == False):
        lastCount = lenOfPage
        sleep(5)
        lenOfPage = browser.execute_script(
            "window.scrollTo(0, document.body.scrollHeight);var lenOfPage=document.body.scrollHeight;return lenOfPage;")
        if lastCount == lenOfPage:
            match = True
    page_xml = browser.page_source
    tree = etree.parse(StringIO(page_xml), parser)
    pdfki = tree.xpath(".//div[@class='result results_links_deep highlight_d']")
    for a_link in range(0, len(browser.find_elements_by_xpath(
            ".//div[@class='result results_links_deep highlight_d']//h2/a[@href][1]"))):
        print(a_link, "  ", ''.join(pdfki[a_link].xpath(".//h2/a[1]/@href")))
        if '.pdf' in ''.join(pdfki[a_link].xpath(".//h2/a[1]/@href")):
            if ''.join(pdfki[a_link].xpath(".//h2/a[1]/@href")) not in all_links:
                print(a_link)
                print(''.join(pdfki[a_link].xpath(".//h2/a[1]/@href")),
                      '\n',
                      # //div[@class='detalhescolunadados_blocos'][1]//text()[not(ancestor::h5)]
                      ''.join(pdfki[a_link].xpath(".//h2/a[1]/text()")),
                      '\n',
                      ''.join(pdfki[a_link].xpath('.//div[@class="result__snippet"]/*/text()|.//div[@class="result__snippet"]/text()')))
                all_links.append(''.join(pdfki[a_link].xpath(".//h2/a[1]/@href")))
                dict_links.append({'pdf_link': ''.join(pdfki[a_link].xpath(".//h2/a[1]/@href")),
                                   'keyword': a_keyword,
                                   'google_page': '-',
                                   'result_stats': '',
                                   'title': ''.join(pdfki[a_link].xpath(".//h2/a[1]/text()")),
                                   'author_and_date': '',
                                   'pdf_snippets': ' '.join(
                                       pdfki[a_link].xpath(
                                           './/div[@class="result__snippet"]/*/text()|.//div[@class="result__snippet"]/text()')),
                                   'pdf_related_articles': '',
                                   'pdf_cited_by': '',
                                   'search_engine': SEARCH_ENGINE,
                                   'topic': TOPIC})
    sql_table = pd.DataFrame(dict_links)
    dict_links = []
    sql_table.to_sql('google_pdfs', engine, if_exists='append', index=False)
    # excel = pd.ExcelFile('/home/user/PycharmProjects/Selenium/PDF_links/duck_duck_go.xlsx')
    # exceltable = excel.parse('links')
    # table3 = pd.DataFrame(dict_links)
    # writer3 = pd.ExcelWriter('/home/user/PycharmProjects/Selenium/PDF_links/duck_duck_go.xlsx', engine='openpyxl')
    # concat = pd.concat([exceltable, table3], ignore_index=True)
    # concat.to_excel(writer3, 'links', index=False,
    #                 columns=['search_engine', 'topic','pdf_link', 'keyword', 'title', 'pdf_snippets'])
    # writer3.save()
    sleep(randint(20, 30))

cur.close()
connection.close()


# cur.close()
# connection.close()
# except NoSuchElementException as e:
#     sleep(15)
#     print('Hey!I am here!')
#     page_xml_1 = browser.page_source
#     tree_1 = etree.parse(StringIO(page_xml_1), parser)
#     I_am_not_a_robot = tree_1.xpath('//*[@id = "g-recaptcha-response"]')
#     print(I_am_not_a_robot)
#     if len(I_am_not_a_robot) != 0:
#         print('I have got a captcha!')
#         sleep(randint(45, 60))
#         I_am_not_a_robot = []
#         print('We solve the captcha! We are good!', '\n', 'I_am_not_a_robot = ', I_am_not_a_robot)
#         error_appears, _all_links = google_pdf_parser(_all_links, page, parser,
#                                                       error_appears, engine,
#                                                       browser, a_keyword)
#     elif len(tree_1.xpath('.//text()[contains(.,"s an error.")]')) != 0:
#         print('We have got an error. It is needed to be solved.')
#         error_appears = True
#         _next_page_exist = False
#     else:
#         print('Ura!', '\n', 'There is no next page!')
#         sleep(20)
#         print('Let us check if any error appears!')
#         page_xml_2 = browser.page_source
#         tree_2 = etree.parse(StringIO(page_xml_2), parser)
#         if len(tree_2.xpath('.//text()[contains(.,"s an error.")]')) != 0 and len(tree_2.xpath(
#                 ".//div[@class='g']//h3/a[@href]")) == 0:
#             print("Again an error...")
#             error_appears, _all_links = google_pdf_parser(_all_links, page,
#                                                           parser, error_appears, engine,
#                                                           browser, a_keyword)
#             error_appears = True
#         print('next_page_exist is {0}'.format(_next_page_exist))
#         print("No. There is no error.")
#         _next_page_exist = False
#         print("Another keyword is coming!", "\n", "dict_links= ", dict_links, "\n", "next_page_exist = ",
#               _next_page_exist)
#         print('next_page_exist is {0}'.format(_next_page_exist))
