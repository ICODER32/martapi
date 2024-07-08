from sqlmodel import create_engine,SQLModel,Field

class Inventory(SQLModel,table=True):
    id:int=Field(default=None,primary_key=True)
    product_id:str
    name:str
    description:str
    price:float
    quantity:int
    category:str

pgsql_url="postgresql://IBTISAM:IBTISAM@pg_container:5432/ibtisam_martapi"


engine=create_engine(pgsql_url)

def create_db_and_tables():
    print("Creating database and tables")
    SQLModel.metadata.create_all(engine)

