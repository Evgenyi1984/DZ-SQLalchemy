from sqlalchemy import tuple_, table, ForeignKey
from sqlalchemy.orm import declarative_base, sessionmaker, relationship
from sqlalchemy import create_engine, Column, Integer, String, Float, Date
from icecream import ic
from datetime import date
import json
from os import getenv
from dotenv import load_dotenv

# ============================================================
# set up thte connection
# ============================================================
load_dotenv()
credentials = getenv('POSTGRES')
database = getenv("BASE")
engine = create_engine(f'postgresql://{credentials}@{database}')

engine.connect()

Base = declarative_base()
Base.metadata.create_all(engine)


Session = sessionmaker(bind=engine)
session = Session()



# ============================================================
# declare classes
# ============================================================

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
# clean the data
# ============================================================

Base.metadata.drop_all(bind=engine, tables=[Publisher.__table__,
                                            Book.__table__,
                                            Stock.__table__,
                                            Shop.__table__,
                                            Sale.__table__])

Base.metadata.create_all(bind=engine)



# ============================================================
# populate data
# ============================================================

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
    session.add(model(id=record.get('pk'), **record.get('fields')))

session.commit()


# ============================================================
# find data
# ============================================================

publisher_id = input('Введите название или номер издателя: ')

pq = session.query(Publisher, Book, Stock, Shop, Sale).filter(Publisher.id==publisher_id)
pq = pq.join(Book, Book.id_publisher == Publisher.id)
pq = pq.join(Stock, Stock.id_book == Book.id)
pq = pq.join(Shop, Shop.id == Stock.id_shop)
pq = pq.join(Sale, Sale.id_stock == Stock.id)


1
print('Найдено строк:', pq.count())

for (pub, book, stock, shop, sale) in pq.all():
    # print(pub.name)
    print(f'{book.title:<43} | {shop.name:<12} | {sale.price:>8} | {sale.date_sale}')
    
