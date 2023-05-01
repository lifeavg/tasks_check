from pathlib import Path
from typing import Iterable, Sequence, Hashable

import pandas as pd


def add_demo_meta(data: pd.DataFrame, meta: str) -> None:
    data['file'] = meta


def first_sheet_name(book: dict[str, pd.DataFrame]) -> str:
    """
    requires python 3.7+ as depends on order of keys in dict
    :param book: result of pd.read_excel(file, None)
    :return: first sheet name
    """
    return list(book.keys())[0]


def process_first_book(file: Path) -> tuple[pd.DataFrame, str]:
    """
    :param file: excel book path
    :return: first sheet data and name
    """
    book = pd.read_excel(file, None)
    sheet_name = first_sheet_name(book)
    data = book[sheet_name]
    add_demo_meta(data, file.name)
    return data, sheet_name


def add_book(file: Path, destination: pd.DataFrame) -> pd.DataFrame:
    """
    :param file: excel file path
    :param destination: dataframe where to add first sheet data
    :return: destination + first sheet data
    """
    data = pd.read_excel(file)
    add_demo_meta(data, file.name)
    return pd.concat([destination, data], ignore_index=True)


def merge_books(books: Iterable[Path], out: Path, sort_by: Sequence[Hashable] | None = None) -> None:
    """
    :param books: excel books
    :param out: output file
    :param sort_by: list of fields to sort
    :return: writes new excel file
    """
    sheet_name = 'Sheet 1'
    dataframe = pd.DataFrame()
    for file_index, file in enumerate(books):
        if file_index == 0:
            dataframe, sheet_name = process_first_book(file)
        else:
            dataframe = add_book(file, dataframe)
    if sort_by:
        dataframe.sort_values(sort_by, inplace=True, ignore_index=True)
    dataframe.to_excel(out, index=False, sheet_name=sheet_name)


if __name__ == '__main__':
    merge_books(Path('data').iterdir(), Path('data/merged.xlsx'), ['field a'])
