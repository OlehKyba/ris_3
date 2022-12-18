import logging
from typing import NamedTuple
from random import randrange, choice, choices
from datetime import datetime, timedelta

from tabulate import tabulate

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


class Goods(NamedTuple):
    name: str
    price: float


class Sale(NamedTuple):
    bill_id: int
    sale_date: datetime
    shop: str
    goods: list[Goods]


def generate_sales(
    total_count: int,
    goods: tuple[Goods, ...],
    max_goods_len: int,
    shops: tuple[str, ...],
    date_start: datetime,
    date_end: datetime,
) -> tuple[Sale, ...]:
    time_between_dates = date_end - date_start
    seconds_between_dates = (
        (time_between_dates.days * 24 * 60 * 60)
        + time_between_dates.seconds
    )
    return tuple(
        Sale(
            bill_id=i,
            sale_date=(
                date_start + timedelta(
                    seconds=randrange(seconds_between_dates)
                )
            ),
            shop=choice(shops),
            goods=choices(goods, k=randrange(1, max_goods_len))

        )
        for i in range(1, total_count + 1)
    )


def log_goods_table(title: str, goods: tuple[Goods, ...]) -> None:
    table = tabulate(
        (('назва', 'ціна'), *goods),
        headers='firstrow',
        tablefmt='fancy_grid',
    )
    log.info(f'{title}\n{table}')


def log_shops_income(title: str, shop_to_income: dict[str, float]) -> None:
    table = tabulate(
        (('магазин', 'прибуток'), *shop_to_income.items()),
        headers='firstrow',
        tablefmt='fancy_grid',
    )
    log.info(f'{title}\n{table}')


def log_top(title: str, top: list[tuple[list[str], int]]) -> None:
    table = tabulate(
        (
            ('товари', 'кількість'),
            *[(', '.join(goods), count) for goods, count in top]
        ),
        headers='firstrow',
        tablefmt='fancy_grid',
    )
    log.info(f'{title}\n{table}')


if __name__ == '__main__':
    goods = (
        Goods('хліб', 10),
        Goods('молоко', 20),
        Goods('пиво', 26.2),
        Goods('чай', 22.3),
        Goods('кава', 55),
    )

    log_goods_table('Таблиця товарів:', goods)
