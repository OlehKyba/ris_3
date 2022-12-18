from contextlib import contextmanager
from typing import Union, Type

from sqlalchemy import select, func
from sqlalchemy.orm import Session

from ris_3.bl import Sale
from ris_3.repositories.sql.models import SqlSale
from ris_3.repositories.sql.core import create_sql_session
from ris_3.repositories.column.model import ColumnSale
from ris_3.repositories.column.core import create_column_session


class SalesRepository:
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

    def count_all_sales(self) -> int:
        result = self.session.execute(
            select(func.count()).select_from(self.Model)
        )
        return result.scalar_one()

    def calculate_income(self) -> float:
        result = self.session.execute(
            select(func.sum(self.Model.price))
        )
        return result.scalar_one()

    def calculate_income_by_goods(self) -> dict[str, float]:
        query = (
            select([self.Model.name, func.sum(self.Model.price)])
            .group_by(self.Model.name)
        )
        result = self.session.execute(query)
        return {goods: income for goods, income in result.all()}


class SqlSalesRepository(SalesRepository):
    Model = SqlSale


class ColumnSalesRepository(SalesRepository):
    Model = ColumnSale


@contextmanager
def sql_sales_repository() -> SqlSalesRepository:
    with create_sql_session() as session:
        yield SqlSalesRepository(session)


@contextmanager
def column_sales_repository() -> ColumnSalesRepository:
    with create_column_session() as session:
        yield ColumnSalesRepository(session)
