from datetime import datetime
from sqlalchemy import create_engine, Column, String, Integer, Text, DateTime
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class Submission(Base):
    __tablename__ = "submissions"
    id = Column(String(255), primary_key=True)
    author = Column(String(255))
    title = Column(Text)
    created_utc = Column(DateTime)
    selftext = Column(Text)
    subreddit = Column(String(255))
    link_flair_text = Column(String(255))
    link = Column(Text)
    num_comments = Column(Integer)
    score = Column(Integer)
    
    
    