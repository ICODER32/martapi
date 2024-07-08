from sqlmodel import SQLModel, create_engine
from sqlmodel import Field, Session, select
from pydantic import BaseModel

class Product(SQLModel, table=True):
    # id should be an autoincrementing primary key
    id: int = Field(default=None, primary_key=True)
    name: str
    description: str
    price: float
    category: str
    product_id: str 

class ProductSchema(BaseModel):
    name: str
    description: str
    price: float
    category: str
pg_url = "postgresql://IBTISAM:IBTISAM@pg_container:5432/ibtisam_martapi"
engine=create_engine(pg_url)


def create_db_and_tables():
    print("Creating database and tables")
    SQLModel.metadata.create_all(engine)
