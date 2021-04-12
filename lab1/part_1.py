from typing import List
from urllib.request import urlopen
from lxml import etree
import xml.etree.cElementTree as ET

INITIAL_URL = "https://kpi.ua"


def parse_kpi_website() -> int:
    """Scrape images and text from pages of the KPI website. Save to an XML file.
    Return the total number of text tags.
    """
    xml_root = ET.Element("data")
    htmlparser = etree.HTMLParser()

    # Parse INITIAL_URL and get a list of the subsequent URLs to be parsed.
    urls_to_parse: List[str] = parse_initial_page(xml_root, htmlparser)

    # Go through the list of URLs and append them to the XML root.
    for url in urls_to_parse:
        url_to_parse = INITIAL_URL + url
        response = urlopen(url_to_parse)
        tree = etree.parse(response, htmlparser)

        parse_url(xml_root, tree, INITIAL_URL)

    # Write all the parsed pages to an XML file
    xml_tree = ET.ElementTree(xml_root)
    xml_tree.write("kpi_website.xml", encoding="UTF-8")

    text_tags_count = len(xml_tree.findall(".//fragment[@type='text']"))

    return text_tags_count


def parse_initial_page(xml_root, htmlparser) -> List[str]:
    """ Scrape data from `INITIAL_URL` and determine which other urls should be parsed. """
    response = urlopen(INITIAL_URL)
    tree = etree.parse(response, htmlparser)
    urls = tree.xpath("//a/@href")
    urls_to_parse = urls[1:20]

    parse_url(xml_root, tree, INITIAL_URL)

    return urls_to_parse


def parse_url(xml_root, tree, url) -> None:
    """ Extract all the text and image elements from a webpage and appends them to an XML file. """
    text_pieces: List[str] = [
        text_piece
        for text_piece in (tree.xpath("//body//text()"))
        if any(char.isalpha() for char in text_piece)
    ]
    image_urls: List[str] = [image.attrib["src"] for image in tree.xpath("//body//img")]

    page = ET.SubElement(xml_root, "page", url=url)
    for text in text_pieces:
        ET.SubElement(page, "fragment", type="text").text = text
    for image_url in image_urls:
        ET.SubElement(page, "fragment", type="image").text = image_url


if __name__ == "__main__":
    number_of_text_tags = parse_kpi_website()

    print(f"Total number of text tags accumulated: {number_of_text_tags}")
