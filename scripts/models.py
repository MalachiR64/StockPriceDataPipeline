from sqlalchemy import Column, Integer, String, Float, DateTime, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()

class Sector(Base):
    __tablename__ = 'sectors'
    
    sector_id = Column(Integer, primary_key=True)
    sector_name = Column(String, nullable=False)
    market_cap = Column(Float, nullable=False)
    last_updated = Column(DateTime, nullable=False)
    
    industries = relationship("Industry", back_populates="sector")

class Industry(Base):
    __tablename__ = 'industries'
    
    industry_id = Column(Integer, primary_key=True)
    sector_id = Column(Integer, ForeignKey('sectors.sector_id'), nullable=False)
    industry_name = Column(String, nullable=False)
    market_cap = Column(Float, nullable=False)
    last_updated = Column(DateTime, nullable=False)
    
    sector = relationship("Sector", back_populates="industries")
    stocks = relationship("Stock", back_populates="industry")

class Stock(Base):
    __tablename__ = 'stocks'
    
    stock_id = Column(Integer, primary_key=True)
    industry_id = Column(Integer, ForeignKey('industries.industry_id'), nullable=False)
    sector_id = Column(Integer, ForeignKey('sectors.sector_id'), nullable=False)
    stock_symbol = Column(String, nullable=False, unique=True)
    stock_name = Column(String, nullable=False)
    stock_price = Column(Float, nullable=False)
    market_cap = Column(Float, nullable=False)
    last_updated = Column(DateTime, nullable=False)
    
    industry = relationship("Industry", back_populates="stocks")
    sector = relationship("Sector")