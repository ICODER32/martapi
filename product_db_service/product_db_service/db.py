from sqlmodel import SQLModel, create_engine, Session, select,Field
            
pgsql_url = "postgresql://IBTISAM:IBTISAM@pg_container:5432/ibtisam_martapi"

engine=create_engine(pgsql_url)


class Product(SQLModel, table=True):
    id: str = Field(default=None, primary_key=True)
    name: str=Field()
    description: str=Field()
    category: str=Field()
    price: float=Field()

def create_db_and_tables():
    SQLModel.metadata.create_all(engine)
    print("Database and tables created")

def get_session():
    with Session(engine) as session:
        yield session