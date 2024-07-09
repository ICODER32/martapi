from sqlmodel import SQLModel, create_engine,Field,Session

pgsql_url = "postgresql://IBTISAM:IBTISAM@pg_container:5432/ibtisam_martapi"


engine=create_engine(pgsql_url, echo=True)

class User(SQLModel, table=True):
    id:int = Field(default=None, primary_key=True)
    name: str
    email: str
    password: str
    role: str

class UserCreate(SQLModel, table=False):
    name: str
    email: str
    password: str
    role: str

def create_db_and_tables():
    SQLModel.metadata.create_all(engine)

def get_session():
    with Session(engine) as session:
        yield session