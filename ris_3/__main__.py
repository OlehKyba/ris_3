import logging
from datetime import datetime

from ris_3.bl import generate_sales, Goods
from ris_3.repositories import (
    sql_sales_repository,
    column_sales_repository,
)

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

goods = (
    Goods('хліб', 10),
    Goods('молоко', 20),
    Goods('пиво', 26.2),
    Goods('чай', 22.3),
    Goods('кава', 55),
)

sales = generate_sales(
    total_count=100,
    goods=goods,
    max_goods_len=5,
    shops=('атб', 'фора', 'сільпо'),
    date_start=datetime(2022, 9, 17),
    date_end=datetime(2022, 12, 17),
)

with sql_sales_repository() as sql_repo, column_sales_repository() as column_repo:
    sql_repo.fill_db(sales)
    column_repo.fill_db(sales)

    # 1. Порахувати кількість проданового товару
    sql_count = sql_repo.count_sales()
    column_count = column_repo.count_sales()

    # 2. Порахувати вартість проданого товару
    sql_price = sql_repo.calculate_income()
    column_price = column_repo.calculate_income()

    # 3. Порахувати вартість проданого товару за період
    sql_price_date = sql_repo.calculate_income(
        date_start=datetime(2022, 10, 1),
        date_end=datetime(2022, 11, 1),
    )
    column_price_date = column_repo.calculate_income(
        date_start=datetime(2022, 10, 1),
        date_end=datetime(2022, 11, 1),
    )

    # 4. Порахувати скільки було придбано товару А в мазазині В за період С
    sql_count_by_shop_goods_date = sql_repo.count_sales(
        goods='хліб',
        shop='атб',
        date_start=datetime(2022, 10, 1),
        date_end=datetime(2022, 11, 1),
    )
    column_count_by_shop_goods_date = column_repo.count_sales(
        goods='хліб',
        shop='атб',
        date_start=datetime(2022, 10, 1),
        date_end=datetime(2022, 11, 1),
    )

    # 5. Порахувати скільки було придбано товару А в усіх магазинах за період С
    sql_count_by_goods_date = sql_repo.count_sales(
        goods='хліб',
        date_start=datetime(2022, 10, 1),
        date_end=datetime(2022, 11, 1),
    )
    column_count_by_goods_date = column_repo.count_sales(
        goods='хліб',
        date_start=datetime(2022, 10, 1),
        date_end=datetime(2022, 11, 1),
    )

    # 6. Порахувати сумарну виручку магазинів за період С
    sql_income_by_shops = sql_repo.calculate_income_by_shops(
        date_start=datetime(2022, 10, 1),
        date_end=datetime(2022, 11, 1),
    )
    column_income_by_shops = column_repo.calculate_income_by_shops(
        date_start=datetime(2022, 10, 1),
        date_end=datetime(2022, 11, 1),
    )

    # 7. Вивести топ 10 купівель товарів по два за період С (наприклад масло, хліб - 1000 разів)
    sql_top_2 = sql_repo.calculate_top(
        items_in_bill=2,
        date_start=datetime(2022, 10, 1),
        date_end=datetime(2022, 11, 1),
    )
    column_top_2 = column_repo.calculate_top(
        items_in_bill=2,
        date_start=datetime(2022, 10, 1),
        date_end=datetime(2022, 11, 1),
    )

    # 8. Вивести топ 10 купівель товарів по три за період С (наприклад молоко, масло, хліб - 1000 разів)
    sql_top_3 = sql_repo.calculate_top(
        items_in_bill=3,
        date_start=datetime(2022, 10, 1),
        date_end=datetime(2022, 11, 1),
    )
    column_top_3 = column_repo.calculate_top(
        items_in_bill=3,
        date_start=datetime(2022, 10, 1),
        date_end=datetime(2022, 11, 1),
    )

    # 9. Вивести топ 10 купівель товарів по чотири за період С
    sql_top_4 = sql_repo.calculate_top(
        items_in_bill=4,
        date_start=datetime(2022, 10, 1),
        date_end=datetime(2022, 11, 1),
    )
    column_top_4 = column_repo.calculate_top(
        items_in_bill=4,
        date_start=datetime(2022, 10, 1),
        date_end=datetime(2022, 11, 1),
    )

    log.info(f'{sql_count=}, {column_count=}')
    log.info(f'{sql_price=}, {column_price=}')
    log.info(f'{sql_price_date=}, {column_price_date=}')
    log.info(f'{sql_count_by_shop_goods_date=}, {column_count_by_shop_goods_date=}')
    log.info(f'{sql_count_by_goods_date=}, {column_count_by_goods_date=}')
    log.info(f'{sql_income_by_shops=}, {column_income_by_shops=}')
    log.info(f'{sql_top_2=}, {column_top_2=}')
    log.info(f'{sql_top_3=}, {column_top_3=}')
    log.info(f'{sql_top_4=}, {column_top_4=}')
