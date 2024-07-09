from fastapi import FastAPI,Depends,HTTPException
from contextlib import asynccontextmanager
from db import create_db_and_tables,engine,User,UserCreate,get_session
from sqlmodel import Session,select
from pydantic import BaseModel
import json
from bcrypt import hashpw, gensalt
import logging
from jose import JWTError, jwt
logging.basicConfig(level=logging.INFO)
loggger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    create_db_and_tables()
    yield


class UseLogin(BaseModel):
    email: str
    password: str


app = FastAPI(
    lifespan=lifespan
)



@app.get("/")
def read_root():
    return {"Hello": "World"}


@app.post("/register/")
async def create_user(user: UserCreate, session: Session = Depends(get_session)):
    loggger.info(f"Creating user {user}")
    hashed_password = hashpw(user.password.encode("utf-8"), gensalt())

    user = User(
        name=user.name,
        email=user.email,
        password=hashed_password.decode("utf-8"),
        role=user.role
    )
    session.add(user)
    session.commit()
    session.refresh(user)
    return {
        "user": user
    }
   


@app.post("/login")
async def login(user: UseLogin, session: Session = Depends(get_session)):
    loggger.info(f"Logging in user {user}")
    isUser = session.exec(select(User).where(User.email == user.email)).first()
    if isUser is None:
        raise HTTPException(status_code=404, detail="User not found")
    
    
    if not hashpw(user.password.encode("utf-8"), isUser.password.encode("utf-8")) == isUser.password.encode("utf-8"):
        raise HTTPException(status_code=401, detail="Invalid password")
    
    
    
    # create jwt token
    user_data = {
        "id": isUser.id,
        "name": isUser.name,
        "email": isUser.email,
        "role": isUser.role
    }
    token = jwt.encode(user_data, "secret", algorithm="HS256")
    return {
        "token": token
    }


@app.get("/secret")
async def secret(token: str):
    try:
        payload = jwt.decode(token, "secret", algorithms=["HS256"])
        return payload
    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid token")
    