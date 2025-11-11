import requests
from sqlalchemy import create_engine, Column, String, Float, Integer, DateTime
from sqlalchemy.orm import declarative_base, sessionmaker
from datetime import datetime
from time import sleep
from dotenv import load_dotenv
import os
load_dotenv()


DATABASE_URL = os.getenv("DATABASE_KEY")

engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
Base = declarative_base()

# Setting table
class BitcoinData(Base):
    __tablename__ = "bitcoin_dados"
    
    id = Column(Integer, primary_key=True)
    price = Column(Float)
    cripto = Column(String(10))
    currency = Column(String(10))
    timestamp = Column(DateTime)

# Create table
Base.metadata.create_all(engine)

def extract_bitcoin_data():
    """Extract complete json from API Coinbase."""
    url = 'https://api.coinbase.com/v2/prices/spot'
    response = requests.get(url)
    return response.json()

def treat_bitcoin_data(json_data):
    """Transform gross data from API and add timestamp."""
    price = float(json_data['data']['amount'])
    cripto = json_data['data']['base']
    currency = json_data['data']['currency']
    treated_data = BitcoinData(
        price=price,
        cripto=cripto,
        currency=currency,
        timestamp=datetime.now()
    )
    return treated_data


def save_in_sqlalchemy(data):
    """Save data in PostgreSQL using SQLAlchemy."""
    with Session() as session:
        session.add(data)
        session.commit()
        print("Saved in PostgreSQL!")

if __name__ == "__main__":
    while True:
        # Extract and clean data
        json_data = extract_bitcoin_data()
        treated_data = treat_bitcoin_data(json_data)

        # Show treated data
        print("Treated data:")        
        # Save in PostgreSQL
        save_in_sqlalchemy(treated_data)

        # Pause for 5 seconds
        print("Waiting 5 seconds...")
        sleep(5)