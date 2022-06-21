from sqlalchemy import create_engine, text
from os import getenv

# from dotenv import load_dotenv
# load_dotenv()

DB_DANAIDES = f"postgresql://{getenv('POSTGRES_USER')}:{getenv('POSTGRES_PASSWORD')}@{getenv('POSTGRES_HOST')}:{getenv('POSTGRES_PORT')}/{getenv('POSTGRES_DBNM')}"
eng = create_engine(DB_DANAIDES)
