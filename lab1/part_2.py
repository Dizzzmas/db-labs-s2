import json
import os
import webbrowser
from typing import List, Tuple
from urllib.request import urlopen
from lxml import etree
import xml.etree.cElementTree as ET


INITIAL_URL = "https://rozetka.com.ua/acer_nh_q8leu_004/p214604215/"


def parse_rozetka_website() -> None:
    """Scrape images and text from pages of the KPI website. Save to an XML file.
    Return the total number of text tags.
    """
    xml_root = ET.Element("shop")
    htmlparser = etree.HTMLParser()

    # Parse INITIAL_URL and get a list of the subsequent URLs to be parsed.
    urls_to_parse: List[str] = parse_initial_page(xml_root, htmlparser)

    # Go through the list of URLs and append them to the XML root.
    for url in urls_to_parse:
        response = urlopen(url)
        tree = etree.parse(response, htmlparser)

        parse_url(xml_root, tree, INITIAL_URL)

    # Write all the parsed pages to an XML file
    xml_tree = ET.ElementTree(xml_root)
    xml_tree.write("rozetka_website.xml", encoding="UTF-8")

    transform = etree.XSLT(etree.parse("./transform.xsl"))
    result = transform(etree.parse("./rozetka_website.xml"))
    result.write("./parsed_rozetka.xhtml", pretty_print=True, encoding="UTF-8")
    webbrowser.open('file://' + os.path.realpath("./parsed_rozetka.xhtml"))


def parse_initial_page(xml_root, htmlparser) -> List[str]:
    """ Scrape data from `INITIAL_URL` and determine which other urls should be parsed. """
    response = urlopen(INITIAL_URL)
    tree = etree.parse(response, htmlparser)
    urls = tree.xpath("//a[@class='lite-tile__title']/@href")
    urls_to_parse = urls[1:20]

    parse_url(xml_root, tree, INITIAL_URL)

    return urls_to_parse


def parse_url(xml_root, tree, url) -> None:
    """ Extract from Rozetka product page name, image, description and price of the product.
        Create an entry in the XML tree.
     """
    product_name: str = parse_product_name(tree)
    product_image_url: str = parse_product_image_url(tree)
    product_description, product_price = parse_product_description_and_price(tree)

    # Create page entry in the XML tree
    page = ET.SubElement(xml_root, "product", url=url)
    ET.SubElement(page, "name").text = product_name
    ET.SubElement(page, "description").text = product_description
    ET.SubElement(page, "image").text = product_image_url
    ET.SubElement(page, "price").text = product_price


def parse_product_description_and_price(tree) -> Tuple[str, str]:
    """ Fetch and decode the product description from Rozetka """
    product: str = tree.xpath("//script[@data-seo='Product']")[0].text
    product: dict = json.loads(product)
    product_description: str = product['description']
    product_description: str = product_description.encode('latin1').decode('utf8')

    offers: dict = product['offers']
    price: str = offers['price']

    return product_description, price


def parse_product_name(tree):
    """ Get product's name from its Rozetka page """
    product_name: str = tree.xpath("//h1[@class='product__title']/text()")[0]
    return product_name.encode('latin1').decode('utf8')


def parse_product_image_url(tree):
    """ Get product's image URL from Rozetka """
    return tree.xpath("//img[@class='product-photo__picture']/@src")[0]


def cleanup():
    try:
        os.remove("parsed_rozetka.xhtml")
        os.remove("rozetka_website.xml")
    except OSError:
        pass


if __name__ == "__main__":
    cleanup()
    parse_rozetka_website()
