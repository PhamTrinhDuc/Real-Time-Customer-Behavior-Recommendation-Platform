"""
SQLAlchemy models for the recommendation platform
Based on the provided ERD diagram
"""

# Import all models to make them available when importing from models
from .customer import Customer
from .product import Product, Category
from .order import Order, OrderItem
from .cart import Cart, Wishlist
from .payment import Payment
from .shipment import Shipment

__all__ = [
    "Customer",
    "Product", 
    "Category",
    "Order",
    "OrderItem",
    "Cart",
    "Wishlist", 
    "Payment",
    "Shipment"
]
