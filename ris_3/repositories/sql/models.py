from datetime import datetime

from sqlalchemy import Column, Integer, String, Float, DateTime

from ris_3.repositories.sql.core import Base


class SqlSale(Base):
    __tablename__ = 'sales'

    id = Column(Integer, primary_key=True)
    name = Column(String(120), nullable=False)
    price = Column(Float, nullable=False)
    shop = Column(String(120), nullable=False)
    sale_date = Column(
        DateTime, nullable=False, default=datetime.now
    )
    bill_id = Column(Integer, nullable=False)
