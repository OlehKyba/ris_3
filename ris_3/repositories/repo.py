import abc
from datetime import datetime
from contextlib import contextmanager
from typing import Union, Type

from sqlalchemy import select, func, desc
from sqlalchemy.sql.functions import Function
from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import aggregate_order_by

from ris_3.bl import Sale
from ris_3.repositories.sql.models import SqlSale
from ris_3.repositories.sql.core import create_sql_session
from ris_3.repositories.column.model import ColumnSale
from ris_3.repositories.column.core import create_column_session


class SalesRepository(abc.ABC):
    Model: Union[Type[SqlSale], Type[ColumnSale]]

    def __init__(self, session: Session):
        self.session = session

    def fill_db(
        self,
        sales: tuple[Sale, ...],
    ) -> None:
        id_ = 1
        for sale in sales:
            for goods in sale.goods:
                self.session.add(
                    self.Model(
                        id=id_,
                        name=goods.name,
                        price=goods.price,
                        shop=sale.shop,
                        sale_date=sale.sale_date,
                        bill_id=sale.bill_id,
                    )
                )
                id_ += 1

        self.session.commit()

    def calculate_income(
        self,
        date_start: datetime = None,
        date_end: datetime = None,
    ) -> float:
        query = select(func.sum(self.Model.price))

        if date_start:
            query = query.where(self.Model.sale_date >= date_start)

        if date_end:
            query = query.where(self.Model.sale_date < date_end)

        result = self.session.execute(query)
        return result.scalar_one()

    def calculate_income_by_goods(self) -> dict[str, float]:
        query = (
            select([self.Model.name, func.sum(self.Model.price)])
            .group_by(self.Model.name)
        )
        result = self.session.execute(query)
        return {goods: income for goods, income in result.all()}

    def count_sales(
        self,
        goods: str = None,
        shop: str = None,
        date_start: datetime = None,
        date_end: datetime = None,
    ) -> float:
        query = select(func.count()).select_from(self.Model)

        if goods:
            query = query.where(self.Model.name == goods)

        if shop:
            query = query.where(self.Model.shop == shop)

        if date_start:
            query = query.where(self.Model.sale_date >= date_start)

        if date_end:
            query = query.where(self.Model.sale_date < date_end)

        result = self.session.execute(query)
        return result.scalar_one()

    def calculate_income_by_shops(
        self,
        date_start: datetime = None,
        date_end: datetime = None,
    ) -> dict[str, float]:
        query = (
            select([self.Model.shop, func.sum(self.Model.price)])
            .group_by(self.Model.shop)
        )

        if date_start:
            query = query.where(self.Model.sale_date >= date_start)

        if date_end:
            query = query.where(self.Model.sale_date < date_end)

        result = self.session.execute(query)
        return {shop: income for shop, income in result.all()}

    @abc.abstractmethod
    def get_calculate_top_array_agg(self) -> Function:
        raise NotImplemented

    def calculate_top(
        self,
        items_in_bill: int,
        top_count: int = 10,
        date_start: datetime = None,
        date_end: datetime = None,
    ) -> list[tuple[list[str], int]]:
        subquery = (
            select(
                self.get_calculate_top_array_agg()
                .label('goods_array')
            )
            .group_by(self.Model.bill_id)
            .having(func.count(self.Model.name) == items_in_bill)
        )

        if date_start:
            subquery = subquery.where(self.Model.sale_date >= date_start)

        if date_end:
            subquery = subquery.where(self.Model.sale_date < date_end)

        count = func.count(subquery.c.goods_array).label('count')
        query = (
            select(
                subquery.c.goods_array,
                count,
            )
            .select_from(subquery)
            .group_by(subquery.c.goods_array)
            .order_by(desc(count), subquery.c.goods_array)
            .limit(top_count)
        )
        result = self.session.execute(query)
        return result.all()


class SqlSalesRepository(SalesRepository):
    Model = SqlSale

    def get_calculate_top_array_agg(self) -> Function:
        return func.array_agg(
            aggregate_order_by(
                self.Model.name,
                self.Model.name,
            )
        )


class ColumnSalesRepository(SalesRepository):
    Model = ColumnSale

    def get_calculate_top_array_agg(self) -> Function:
        return func.arraySort(
            func.groupArray(self.Model.name)
        )


@contextmanager
def sql_sales_repository() -> SqlSalesRepository:
    with create_sql_session() as session:
        yield SqlSalesRepository(session)


@contextmanager
def column_sales_repository() -> ColumnSalesRepository:
    with create_column_session() as session:
        yield ColumnSalesRepository(session)
