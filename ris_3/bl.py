from random import randrange, choice, choices
from datetime import datetime, timedelta

from dataclasses import dataclass


@dataclass(frozen=True)
class Goods:
    name: str
    price: float


@dataclass(frozen=True)
class Sale:
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
    # random_date = date_start + timedelta(seconds=randrange(seconds_between_dates))


if __name__ == '__main__':
    goods = (
        Goods('хліб', 10),
        Goods('молоко', 20),
        Goods('пиво', 26.2),
        Goods('чай', 22.3),
        Goods('кава', 55),
    )

    print(generate_sales(
        total_count=10,
        goods=goods,
        max_goods_len=4,
        shops=('атб', 'фора', 'сільпо'),
        date_start=datetime(2022, 9, 17),
        date_end=datetime(2022, 12, 17),
    ))
