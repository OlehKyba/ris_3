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
    max_goods_len=4,
    shops=('атб', 'фора', 'сільпо'),
    date_start=datetime(2022, 9, 17),
    date_end=datetime(2022, 12, 17),
)

with sql_sales_repository() as sql_repo, column_sales_repository() as column_repo:
    log.info(sql_repo.count_all_sales())

    sql_repo.fill_db(sales)
    column_repo.fill_db(sales)

    sql_count = sql_repo.count_all_sales()
    column_count = column_repo.count_all_sales()

    sql_price = sql_repo.calculate_income()
    column_price = column_repo.calculate_income()

    sql_goods_to_income = sql_repo.calculate_income_by_goods()
    column_goods_to_income = column_repo.calculate_income_by_goods()

    log.info(f'{sql_count=}, {column_count=}')
    log.info(f'{sql_price=}, {column_price=}')
    log.info(f'{sql_goods_to_income=}, {column_goods_to_income=}')
