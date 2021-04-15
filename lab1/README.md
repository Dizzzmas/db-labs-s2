## Pre-requisites
You should have [poetry](https://python-poetry.org/) installed.

## Quickstart
```
poetry install
python3 part_1.py
python3 part_2.py
```

## Part1
Will parse ~20 URLs from the https://kpi.ua/ website.
Will extract text and images for each page and write them to `kpi_website.xml`.
Upon completion will output the URL of the page with max text elements.

## Part2
Will parse ~20 URls from the https://rozetka.com.ua/ website.
Will extract product's name, image, price, description and write to `rozetka_website.xml`
Then will use XSLT from `transform.xsl` to generate `parsed_rozetka.xhtml` from `rozetka_website.xml`.
