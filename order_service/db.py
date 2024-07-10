from sqlmodel import SQLModel, create_engine,Field

pgsql_url = "postgresql://IBTISAM:IBTISAM@pg_container:5432/ibtisam_martapi"


engine=create_engine(pgsql_url, echo=True)

class Order(SQLModel, table=True):
    id:int = Field(default=None, primary_key=True)
    user_id: str
    product_id: str
    quantity: int
    price: float


class OrderCreate(SQLModel, table=False):
    product_id: str
    quantity: int
    price: float

class Product(SQLModel, table=True):
    # id should be an autoincrementing primary key
    id: int = Field(default=None, primary_key=True)
    name: str
    description: str
    price: float
    category: str
    product_id: str 

def create_db_and_tables():
    SQLModel.metadata.create_all(engine)

