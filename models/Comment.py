from datetime import datetime
from sqlalchemy import create_engine, Column, String, Integer, Text, DateTime, ForeignKey
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class Comment(Base):
    __tablename__ = "comments"
    id = Column(String(255), primary_key=True)
    post_id = Column(String(255))
    parent_id = Column(String(255))
    author = Column(String(255))
    created_utc = Column(DateTime)
    body = Column(Text)
    depth = Column(Integer)
    
    def __repr__(self):
        return f"Comment(id={self.id}, post_id={self.post_id}, parent_id={self.parent_id}, author={self.author}, created_utc={self.created_utc}, body={self.body}, depth={self.depth})"