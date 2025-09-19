"""
Cart and Wishlist models
"""
from sqlalchemy import Column, Integer, DateTime, ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from utils.database import Base


class Cart(Base):
    __tablename__ = "carts"
    
    cart_id = Column(Integer, primary_key=True, index=True)
    quantity = Column(Integer, nullable=False, default=1)
    added_at = Column(DateTime(timezone=True), server_default=func.now())
    customer_id = Column(Integer, ForeignKey("customers.customer_id"))
    product_id = Column(Integer, ForeignKey("products.product_id"))
    
    # Relationships
    customer = relationship("Customer", back_populates="cart_items")
    product = relationship("Product", back_populates="cart_items")

    def __repr__(self):
        return f"<Cart(id={self.cart_id}, customer_id={self.customer_id}, product_id={self.product_id})>"


class Wishlist(Base):
    __tablename__ = "wishlists"
    
    wishlist_id = Column(Integer, primary_key=True, index=True)
    added_at = Column(DateTime(timezone=True), server_default=func.now())
    customer_id = Column(Integer, ForeignKey("customers.customer_id"))
    product_id = Column(Integer, ForeignKey("products.product_id"))
    
    # Relationships
    customer = relationship("Customer", back_populates="wishlist_items")
    product = relationship("Product", back_populates="wishlist_items")

    def __repr__(self):
        return f"<Wishlist(id={self.wishlist_id}, customer_id={self.customer_id}, product_id={self.product_id})>"
