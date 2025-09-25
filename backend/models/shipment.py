"""
Shipment model
"""
from sqlalchemy import Column, Integer, String, DateTime, ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from utils.database import Base


class Shipment(Base):
    __tablename__ = "shipments"
    
    shipment_id = Column(Integer, primary_key=True, index=True)
    shipment_date = Column(DateTime(timezone=True))
    address = Column(String(100), nullable=False)
    city = Column(String(100), nullable=False)
    state = Column(String(20))
    country = Column(String(50), nullable=False)
    zip_code = Column(String(10))
    status = Column(String(50), default="preparing")
    tracking_number = Column(String(100), unique=True)
    customer_id = Column(Integer, ForeignKey("customers.customer_id"))
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    
    # Relationships
    customer = relationship("Customer", back_populates="shipments")
    order = relationship("Order", back_populates="shipment", uselist=False)

    def __repr__(self):
        return f"<Shipment(id={self.shipment_id}, status={self.status}, tracking={self.tracking_number})>"
