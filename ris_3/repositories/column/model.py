from datetime import datetime

from sqlalchemy import Column
from clickhouse_sqlalchemy.engines import MergeTree
from clickhouse_sqlalchemy.types import Int, String, Float, DateTime

from ris_3.repositories.column.core import Base


class ColumnSale(Base):
    __tablename__ = 'sales'
    __table_args__ = (
        MergeTree(
            # order_by='sale_date',
            primary_key='id',
        ),
    )

    id = Column(Int, primary_key=True)
    name = Column(String(120), nullable=False)
    price = Column(Float, nullable=False)
    shop = Column(String(120), nullable=False)
    sale_date = Column(
        DateTime, nullable=False, default=datetime.now
    )
    bill_id = Column(Int, nullable=False)
