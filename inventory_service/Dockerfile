FROM python:3.12

# Set the working directory in the container

WORKDIR /app

# Copy the dependencies file to the working directory

COPY . .

RUN pip install poetry

RUN poetry config virtualenvs.create false

RUN poetry install 
# Copy the content of the local src directory to the working directory

EXPOSE 8000

CMD ["poetry","run","uvicorn","main:app","--host","0.0.0.0","--port","8000"]