"""
Order and OrderItem models
"""
from sqlalchemy import Column, Integer, String, DateTime, DECIMAL, ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from utils.database import Base


class Order(Base):
    __tablename__ = "orders"
    
    order_id = Column(Integer, primary_key=True, index=True)
    order_date = Column(DateTime(timezone=True), server_default=func.now())
    total_price = Column(DECIMAL(10, 2), nullable=False)
    status = Column(String(50), default="pending")
    customer_id = Column(Integer, ForeignKey("customers.customer_id"))
    payment_id = Column(Integer, ForeignKey("payments.payment_id"), nullable=True)
    shipment_id = Column(Integer, ForeignKey("shipments.shipment_id"), nullable=True)
    
    # Relationships
    customer = relationship("Customer", back_populates="orders")
    payment = relationship("Payment", back_populates="order")
    shipment = relationship("Shipment", back_populates="order")
    order_items = relationship("OrderItem", back_populates="order", cascade="all, delete-orphan")

    def __repr__(self):
        return f"<Order(id={self.order_id}, total={self.total_price}, status={self.status})>"


class OrderItem(Base):
    __tablename__ = "order_items"
    
    order_item_id = Column(Integer, primary_key=True, index=True)
    quantity = Column(Integer, nullable=False, default=1)
    price = Column(DECIMAL(10, 2), nullable=False)
    order_id = Column(Integer, ForeignKey("orders.order_id"))
    product_id = Column(Integer, ForeignKey("products.product_id"))
    
    # Relationships
    order = relationship("Order", back_populates="order_items")
    product = relationship("Product", back_populates="order_items")

    def __repr__(self):
        return f"<OrderItem(id={self.order_item_id}, quantity={self.quantity}, price={self.price})>"
