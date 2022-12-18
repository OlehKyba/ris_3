import logging
from datetime import datetime

from ris_3.bl import (
    generate_sales,
    log_goods_table,
    log_shops_income,
    log_top,
    Goods,
)
from ris_3.repositories import (
    sql_sales_repository,
    column_sales_repository,
)

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

goods = (
    Goods('картопля', 15),
    Goods('філе', 120),
    Goods('яйця', 60),
    Goods('молоко', 35),
    Goods('хліб', 25),
    Goods('пиво', 35),
    Goods('чай', 40),
    Goods('кава', 120),
    Goods('масло', 60),
    Goods('цибуля', 35),
)
log_goods_table('Таблиця товарів:', goods)

sales = generate_sales(
    total_count=100000,
    goods=goods,
    max_goods_len=5,
    shops=('АТБ', 'Фора', 'Сільпо', 'Novus', 'Полісся'),
    date_start=datetime(2022, 9, 17),
    date_end=datetime(2022, 12, 17),
)
log.info(f'Було згенеровано {len(sales)} чеків')

with sql_sales_repository() as sql_repo, column_sales_repository() as column_repo:
    log.info(f'Завантажуємо {len(sales)} чеків у PostgreSQL...')
    sql_repo.fill_db(sales)
    log.info(f'Завершено завантаження {len(sales)} чеків у PostgreSQL.')

    log.info(f'Завантажуємо {len(sales)} чеків у ClickHouse...')
    column_repo.fill_db(sales)
    log.info(f'Завершено завантаження {len(sales)} чеків у ClickHouse.')

    # 1. Порахувати кількість проданового товару.
    log.info('1. Порахувати кількість проданового товару.')
    sql_count = sql_repo.count_sales()
    log.info(f'Кількість проданового товару в PostgreSQL: {sql_count}')
    column_count = column_repo.count_sales()
    log.info(f'Кількість проданового товару в ClickHouse: {column_count}')

    # 2. Порахувати вартість проданого товару
    log.info('2. Порахувати вартість проданого товару.')
    sql_price = sql_repo.calculate_income()
    log.info(f'Вартість проданого товару в PostgreSQL: {sql_price}')
    column_price = column_repo.calculate_income()
    log.info(f'Вартість проданого товару в ClickHouse: {column_price}')

    # 3. Порахувати вартість проданого товару за період
    log.info('3. Порахувати вартість проданого товару за період: 01.10.2022 - 01.11.2022')
    sql_price_date = sql_repo.calculate_income(
        date_start=datetime(2022, 10, 1),
        date_end=datetime(2022, 11, 1),
    )
    log.info(f'Вартість проданого товару за період у PostgreSQL: {sql_price_date}')
    column_price_date = column_repo.calculate_income(
        date_start=datetime(2022, 10, 1),
        date_end=datetime(2022, 11, 1),
    )
    log.info(f'Вартість проданого товару за період у ClickHouse: {column_price_date}')

    # 4. Порахувати скільки було придбано товару А в магазині В за період С
    log.info(
        '4. Порахувати скільки було придбано товару '
        '"хліб" в магазині "АТБ" за період 01.10.2022 - 01.11.2022'
    )
    sql_count_by_shop_goods_date = sql_repo.count_sales(
        goods='хліб',
        shop='АТБ',
        date_start=datetime(2022, 10, 1),
        date_end=datetime(2022, 11, 1),
    )
    log.info(
        f'Кількість придбаного хлібу в АТБ за жовтень у PostgreSQL: '
        f'{sql_count_by_shop_goods_date}'
    )
    column_count_by_shop_goods_date = column_repo.count_sales(
        goods='хліб',
        shop='атб',
        date_start=datetime(2022, 10, 1),
        date_end=datetime(2022, 11, 1),
    )
    log.info(
        f'Кількість придбаного хлібу в АТБ за жовтень у ClickHouse: '
        f'{sql_count_by_shop_goods_date}'
    )

    # 5. Порахувати скільки було придбано товару А в усіх магазинах за період С
    log.info(
        '5. Порахувати скільки було придбано товару '
        '"пиво" в усіх магазинах за період 01.10.2022 - 01.11.2022'
    )
    sql_count_by_goods_date = sql_repo.count_sales(
        goods='пиво',
        date_start=datetime(2022, 10, 1),
        date_end=datetime(2022, 11, 1),
    )
    log.info(
        f'Кількість придбаного пива а жовтень у PostgreSQL: '
        f'{sql_count_by_goods_date}'
    )
    column_count_by_goods_date = column_repo.count_sales(
        goods='пиво',
        date_start=datetime(2022, 10, 1),
        date_end=datetime(2022, 11, 1),
    )
    log.info(
        f'Кількість придбаного пива а жовтень у ClickHouse: '
        f'{column_count_by_goods_date}'
    )

    # 6. Порахувати сумарну виручку магазинів за період С
    log.info(
        'Порахувати сумарну виручку магазинів '
        'за період 01.10.2022 - 01.11.2022'
    )
    sql_income_by_shops = sql_repo.calculate_income_by_shops(
        date_start=datetime(2022, 10, 1),
        date_end=datetime(2022, 11, 1),
    )
    log_shops_income('Виручка з PostgreSQL:', sql_income_by_shops)
    column_income_by_shops = column_repo.calculate_income_by_shops(
        date_start=datetime(2022, 10, 1),
        date_end=datetime(2022, 11, 1),
    )
    log_shops_income('Виручка з ClickHouse:', column_income_by_shops)

    # 7. Вивести топ 10 купівель товарів по два за період С (наприклад масло, хліб - 1000 разів)
    log.info(
        '7. Вивести топ 10 купівель товарів по два'
        ' за період 01.10.2022 - 01.11.2022'
    )
    sql_top_2 = sql_repo.calculate_top(
        items_in_bill=2,
        date_start=datetime(2022, 10, 1),
        date_end=datetime(2022, 11, 1),
    )
    log_top('Таблиця з PostgreSQL:', sql_top_2)
    column_top_2 = column_repo.calculate_top(
        items_in_bill=2,
        date_start=datetime(2022, 10, 1),
        date_end=datetime(2022, 11, 1),
    )
    log_top('Таблиця з ClickHouse:', column_top_2)

    # 8. Вивести топ 10 купівель товарів по три за період С (наприклад молоко, масло, хліб - 1000 разів)
    log.info(
        '8. Вивести топ 10 купівель товарів по три '
        'за період 01.10.2022 - 01.11.2022'
    )
    sql_top_3 = sql_repo.calculate_top(
        items_in_bill=3,
        date_start=datetime(2022, 10, 1),
        date_end=datetime(2022, 11, 1),
    )
    log_top('Таблиця з PostgreSQL:', sql_top_3)
    column_top_3 = column_repo.calculate_top(
        items_in_bill=3,
        date_start=datetime(2022, 10, 1),
        date_end=datetime(2022, 11, 1),
    )
    log_top('Таблиця з ClickHouse:', column_top_3)

    # 9. Вивести топ 10 купівель товарів по чотири за період С
    log.info(
        '9. Вивести топ 10 купівель товарів по чотири '
        'за період 01.10.2022 - 01.11.2022'
    )
    sql_top_4 = sql_repo.calculate_top(
        items_in_bill=4,
        date_start=datetime(2022, 10, 1),
        date_end=datetime(2022, 11, 1),
    )
    log_top('Таблиця з PostgreSQL:', sql_top_4)
    column_top_4 = column_repo.calculate_top(
        items_in_bill=4,
        date_start=datetime(2022, 10, 1),
        date_end=datetime(2022, 11, 1),
    )
    log_top('Таблиця з ClickHouse:', column_top_4)
