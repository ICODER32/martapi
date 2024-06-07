from product_db_service.db import get_session, Product
from typing import Annotated
from fastapi import Depends

def add_product(product: Product,session=Depends(get_session)):
    session.add(product)
    session.commit()
    session.refresh(product)
    return product