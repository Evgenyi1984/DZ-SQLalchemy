from sqlalchemy import ForeignKey
from sqlalchemy.orm import declarative_base, sessionmaker, relationship
from sqlalchemy import create_engine, Column, Integer, String, Float, Date
import json
from os import getenv
from dotenv import load_dotenv


# ============================================================
# ORM classes
# ============================================================

Base = declarative_base()

class Publisher(Base):
    __tablename__ = 'publisher'
    
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)

class Book(Base):
    __tablename__ = 'book'

    id = Column(Integer, primary_key=True)
    title = Column(String, nullable=False)
    id_publisher = Column(Integer, ForeignKey('publisher.id'), nullable=False)
    publisher = relationship(Publisher, backref='pub')
    def __repr__(self):
        return "".format(self.code)

class Shop(Base):
    __tablename__ = 'shop'

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    

class Stock(Base):
    __tablename__ = 'stock'

    id = Column(Integer, primary_key=True)
    id_book = Column(Integer, ForeignKey('book.id'), nullable=False)
    id_shop = Column(Integer, ForeignKey('shop.id'), nullable=False)
    count = Column(Integer, nullable=False)

    book = relationship(Book, backref='bk')
    shop = relationship(Shop, backref='ref')

class Sale(Base):
    __tablename__ = 'sale'

    id = Column(Integer, primary_key=True)
    price = Column(Float)
    date_sale = Column(Date)
    id_stock = Column(Integer, ForeignKey('stock.id'), nullable=False)
    count = Column(Integer)

    stock = relationship(Stock, backref='stck')


# ============================================================
# initialization
# ============================================================

db_session = None

def initialize():

    # set up thte connection

    load_dotenv()
    credentials = getenv('POSTGRES')
    database = getenv("BASE")
    engine = create_engine(f'postgresql://{credentials}@{database}')

    engine.connect()

    Base.metadata.create_all(engine)

    Session = sessionmaker(bind=engine)

    global db_session
    db_session = Session()

    # clean and recreate the database

    Base.metadata.drop_all(bind=engine, 
        tables=[Publisher.__table__,
            Book.__table__,
            Stock.__table__,
            Shop.__table__,
            Sale.__table__])

    Base.metadata.create_all(bind=engine)

    # populate the database

    with open('/home/evgenii/Рабочий стол/GIT/SQLalchemy/tests_data.json', 'r') as fd:
        data = json.load(fd)

    for record in data:
        model = {
            'publisher': Publisher,
            'shop': Shop,
            'book': Book,
            'stock': Stock,
            'sale': Sale,
        }[record.get('model')]
        db_session.add(model(id=record.get('pk'), **record.get('fields')))

    db_session.commit()

    
def get_shops(search_string):

    # find data

    global db_session
    query = db_session.query(
        Book.title,
        Shop.name,
        Sale.price,
        Sale.date_sale
    ).select_from(Shop).\
        join(Stock, Shop.id == Stock.id_shop).\
        join(Book, Stock.id_book == Book.id).\
        join(Publisher, Book.id_publisher == Publisher.id).\
        join(Sale, Stock.id == Sale.id_stock)
    
    if search_string.isdigit() and int(search_string) > 0:
        results = query.filter(Publisher.id == int(search_string)).all()
    else:
        results = query.filter(Book.title.ilike(f"%{search_string}%")).all()
    
    for book_title, shop_name, sale_price, sale_date in results:
        print(f"{book_title: <43} | {shop_name: <12} | {sale_price: <8} | {sale_date.strftime('%d-%m-%Y')}")

# ============================================================
# main program code
# ============================================================

if __name__ == '__main__':
    initialize()
    search_string = input('Введите название или номер издателя: ')
    get_shops(search_string)
