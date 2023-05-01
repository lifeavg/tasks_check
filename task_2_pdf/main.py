import json
import re
from pathlib import Path
from typing import Iterable

from pypdf import PdfReader, PdfMerger, PageObject


class PdfFile:
    number_re = re.compile(r'Номер[ ]+[0-9]{9}')
    undefined_number = 'UNDEFINED'

    def __init__(self, file: Path) -> None:
        self.file = file
        self._reader: PdfReader | None = None
        self._number = 0

    @property
    def reader(self) -> PdfReader:
        if not self._reader:
            self._reader = PdfReader(self.file)
        return self._reader

    @property
    def size(self) -> int:
        return self.file.stat().st_size

    @property
    def number(self) -> int | str:
        if not self._number:
            self._parse_number()
        return self._number

    def _parse_number(self) -> None:
        text = self._get_text()
        if text:
            number_str = self._get_number_str(text)
            if number_str:
                self._number = int(number_str)
                return
        self._number = self.undefined_number

    def _get_number_str(self, text: str) -> str | None:
        match = re.match(self.number_re, text)
        if match:
            return match[0].split()[-1]

    def _get_text(self) -> str | None:
        first_page = self._get_first_page()
        if first_page:
            return first_page.extract_text()

    def _get_first_page(self) -> PageObject | None:
        if self.reader.pages:
            return self.reader.pages[0]

    def __repr__(self) -> str:
        return str(self.file)


def key_sort_number(file: PdfFile) -> int:
    if file.number != file.undefined_number:
        return file.number
    return 0


class Merger:

    def __init__(self, files: Iterable[Path], chunk_size: int = 102400) -> None:
        self._original_files = files
        self._files: list[PdfFile] | None = None
        self.chunk_size = chunk_size
        self._pdf_merger: PdfMerger = PdfMerger()
        self._current_chunk_size = 0
        self._chunk_number = 1
        self._files_in_chunk = []

    @property
    def files(self) -> list[PdfFile]:
        if not self._files:
            self._files = sorted(
                (PdfFile(file) for file in Path('data').iterdir()),
                key=key_sort_number
            )
        return self._files

    def merge(self, out_folder: Path) -> None:
        for file in self.files:
            if self._current_chunk_size + file.size < self.chunk_size:
                self._append_to_chunk(file)
            else:
                self._write_chunk(out_folder)
                self._next_chunk()
                self._append_to_chunk(file)
        self._write_chunk(out_folder)
        self._reset_merger()
        self._chunk_number = 1

    def _append_to_chunk(self, file: PdfFile) -> None:
        self._current_chunk_size += file.size
        self._pdf_merger.append(file.reader)
        self._files_in_chunk.append(file.file.name)

    def _reset_merger(self) -> None:
        self._pdf_merger = PdfMerger()
        self._current_chunk_size = 0
        self._files_in_chunk = []

    def _next_chunk(self) -> None:
        self._chunk_number += 1
        self._reset_merger()

    def _write_chunk(self, out_folder: Path) -> None:
        if self._pdf_merger.pages:
            file_name = f'pack_{self._chunk_number}'
            pack = Path(file_name + '.pdf')
            self._pdf_merger.write(out_folder / pack)
            with Path(out_folder / (file_name + '.json')).open(mode='w', encoding='utf-8') as file:
                json.dump({'pack': pack.name, 'files': self._files_in_chunk}, file)


if __name__ == '__main__':
    data = Path('data')
    Merger(files=data.iterdir()).merge(data)
