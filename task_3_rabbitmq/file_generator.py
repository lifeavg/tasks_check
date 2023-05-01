from pathlib import Path
from time import sleep
from uuid import uuid4

from config import config


def create_file(destination: Path, name: str) -> None:
    with (destination / Path(name)).open('w', encoding='utf-8') as file:
        file.write(name)


def file_generator(delay: int, destination: Path) -> None:
    while True:
        create_file(destination, str(uuid4()))
        sleep(delay)


if __name__ == '__main__':
    file_generator(config.generator_delay, config.wk_dir)
