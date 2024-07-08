FROM python:3.12

WORKDIR /app

COPY . .

RUN pip install poetry

RUN poetry config virtualenvs.create false

RUN poetry install 

CMD ["poetry", "run","uvicorn","main:app","--host","0.0.0.0","--port","8000"]