from sqlalchemy import func
import sqlalchemy as db
from sqlalchemy import Column, Integer, String, Date, DateTime, cast
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql.expression import text, label, null
Base = declarative_base()
 
engine = db.create_engine("mysql+pymysql://root:Radugailislon123@localhost/task")

class TableA(Base):
    __tablename__ = 'tableas'
    Base.metadata
    id = Column(Integer,primary_key=True)
    Date = Column(Date)
    Campaign = Column(String)
    Ad = Column(String)
    Impression = Column(String)
    Click = Column(Integer)
    Cost = Column(Integer)

class TableB(Base):
    __tablename__ = 'tablebs'
    Base.metadata
    id = Column(Integer,primary_key=True)
    Date = Column(DateTime)
    Campaign = Column(String)
    Ad = Column(String)
    Impression = Column(String)
    Click = Column(Integer)
    Cost = Column(Integer)

class TableC(Base):
    __tablename__ = 'tablecs'
    Base.metadata
    id = Column(Integer,primary_key=True)
    Date = Column(Date)
    Source = Column(String)
    Campaign = Column(String)
    Ad = Column(String)
    Install = Column(Integer)
    Purchase = Column(Integer)
 
meta_data = db.MetaData(bind=engine)
db.MetaData.reflect(meta_data)

sub_query_1 = db.select(TableA.Date,null().label('Source'),TableA.Campaign, TableA.Ad, TableA.Click,TableA.Cost,null().label('Install'),null().label('Purchase'))
sub_query_2 = db.select(cast(TableB.Date, Date),null().label('Source'),TableB.Campaign,TableB.Ad, TableB.Click,TableB.Cost,null().label('Install'),null().label('Purchase'))
sub_query_3 = db.select(TableC.Date,TableC.Source,TableC.Campaign,TableC.Ad,null().label('Click'),null().label('Cost'),TableC.Install,TableC.Purchase)
sub_query = db.union_all(sub_query_1, sub_query_2, sub_query_3).subquery()

query = db.select([sub_query,func.sum(sub_query.c.Click).label('Total_Click'),func.sum(sub_query.c.Cost).label('Total_Cost'),func.sum(sub_query.c.Install).label('Total_Install'),func.sum(sub_query.c.Purchase).label('Total_Purchase')]) \
    .group_by(sub_query.c.Date,sub_query.c.Source,sub_query.c.Campaign,sub_query.c.Ad)

with engine.connect() as conn:
    result = conn.execute(query).all()

for row in result:
    print(row.Date,row.Source,row.Campaign,row.Ad, row.Total_Click, row.Total_Cost,row.Total_Install,row.Total_Purchase)