"""
Customer model
"""
import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from sqlalchemy import Column, Integer, String, DateTime
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from utils.database import Base


class Customer(Base):
    __tablename__ = "customers"
    
    customer_id = Column(Integer, primary_key=True, index=True)
    first_name = Column(String(100), nullable=False)
    last_name = Column(String(100), nullable=False)
    email = Column(String(100), unique=True, nullable=False, index=True)
    password = Column(String(255), nullable=False)
    address = Column(String(100))
    phone_number = Column(String(100))
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    
    # Relationships
    orders = relationship("Order", back_populates="customer")
    cart_items = relationship("Cart", back_populates="customer")
    wishlist_items = relationship("Wishlist", back_populates="customer")
    payments = relationship("Payment", back_populates="customer")
    shipments = relationship("Shipment", back_populates="customer")

    def __repr__(self):
        return f"<Customer(id={self.customer_id}, email={self.email})>"
