from pathlib import Path
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


def load_ascii_table_from_url(url: str, filepath: str | Path) -> None:
    try:
        with urlopen(url) as response:
            html = response.read()
    except Exception as e:
        print(f"Error fetching {url}: {e}")
        return

    soup = BeautifulSoup(html, "html.parser")
    ascii_url = soup.find(string="ASCII", attrs={"href": True})  # type: ignore
    if ascii_url is None:
        print(f"No ASCII link found in {url}")
        return

    href = get_href_from_td(ascii_url.parent)  # type: ignore
    if href is None:
        print(f"No href found for ASCII link in {url}")
        return

    parent_url = "/".join(url.split("/")[:-1]) + "/"
    data_url = href if href.startswith("http") else parent_url + href
    with urlopen(data_url) as response:
        data = response.read().decode("utf-8")

    lines = data.splitlines()
    with open(filepath, "w") as f:
        f.writelines([line + "\n" for line in lines])


def main(solar_events_path: str | Path) -> None:
    # Load dataframe
    df = pl.read_csv(solar_events_path)

    for data in df.iter_rows(named=True):
        href = data["href"]
        if href is None:
            print(f"No href for year {data['year']}, skipping")
            continue

        # Fetch the page
        with urlopen(href) as response:
            html = response.read()

        soup = BeautifulSoup(html, "html.parser")
        table = soup.find("table")
        assert table is not None, "No table found on the page"

        basepath = DATADIR / str(data["year"]).replace("*", "")
        rows = []
        columns = []

        print(f"Processing {data['year']}")
        for tr in table.find_all("tr"):
            if len(columns) == 0:
                for th in tr.find_all("th"):
                    content = "".join(
                        [
                            str(content)
                            if "<sub>" not in str(content)
                            else str(content)
                            .replace("<sub>", "_")
                            .replace("</sub>", "")
                            for content in th.contents
                        ]
                    )
                    columns.append(content.strip().lower())
                continue

            row = []
            for td in tr.find_all("td"):
                content = str(td.contents[0]).strip()
                if "<a " in content:
                    content = get_href_from_td(td)  # type: ignore
                    assert content is not None, "No href found in td"

                    if not content.startswith("http"):
                        content = BASE_URL + content
                    else:
                        path = basepath / f"{row[0]}.txt"
                        if not path.exists():
                            load_ascii_table_from_url(content, path)
                row.append(content)

            print(f"Processed row: {row[:2]}")
            rows.append(row)

        df = pl.DataFrame(rows, schema=columns, orient="row")
        path = basepath / "event_metadata.csv"
        path.parent.mkdir(parents=True, exist_ok=True)
        df.write_csv(path)


if __name__ == "__main__":
    import sys

    args = sys.argv[1:]
    if len(args) > 0:
        solar_events_path = args[0]
    else:
        solar_events_path = DATADIR / "solar_events_metadata.csv"

    main(solar_events_path)
