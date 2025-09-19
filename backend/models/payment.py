"""
Payment model
"""
from sqlalchemy import Column, Integer, String, DateTime, DECIMAL, ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from utils.database import Base


class Payment(Base):
    __tablename__ = "payments"
    
    payment_id = Column(Integer, primary_key=True, index=True)
    payment_date = Column(DateTime(timezone=True), server_default=func.now())
    payment_method = Column(String(100), nullable=False)
    amount = Column(DECIMAL(10, 2), nullable=False)
    status = Column(String(50), default="pending")
    transaction_id = Column(String(100), unique=True)
    customer_id = Column(Integer, ForeignKey("customers.customer_id"))
    
    # Relationships
    customer = relationship("Customer", back_populates="payments")
    order = relationship("Order", back_populates="payment", uselist=False)

    def __repr__(self):
        return f"<Payment(id={self.payment_id}, amount={self.amount}, method={self.payment_method}, status={self.status})>"
