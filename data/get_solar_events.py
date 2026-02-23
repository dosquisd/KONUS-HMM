from urllib.request import urlopen

import polars as pl
from bs4 import BeautifulSoup

from utils import DATADIR

BASE_URL = "https://www.ioffe.ru/LEA/Solar/"


def get_href_from_td(td: BeautifulSoup | None) -> str | None:
    if td is None:
        return None

    link = td.find("a")
    if link is None:
        return None

    href = link.get("href")
    if href is None:
        return None

    return str(href)


def main(filename: str) -> None:
    with urlopen(BASE_URL) as response:
        html = response.read()

    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table")
    assert table is not None, "No table found on the page"

    columns = []
    rows = []
    for tr in table.find_all("tr"):
        if len(columns) == 0:
            for th in tr.find_all("th"):
                columns.append(str(th.contents[0]).strip().lower())
            columns.append("href")
            continue

        rows.append([])

        th = tr.find("th")
        assert th is not None, "No th found in tr"

        year = str(th.contents[0]).strip().removesuffix("</a>").split(">")[-1]
        rows[-1].append(year)

        href = get_href_from_td(th)  # type: ignore
        url = BASE_URL + href if href is not None else None
        for td in tr.find_all("td"):
            rows[-1].append(str(td.contents[0]).strip())

        rows[-1].append(url)

    df = pl.DataFrame(rows, schema=columns, orient="row")
    df.write_csv(DATADIR / filename)
    print(df)


if __name__ == "__main__":
    import sys

    args = sys.argv[1:]
    if len(args) > 0:
        filename = args[0]
    else:
        filename = "solar_events_metadata.csv"

    main(filename)
