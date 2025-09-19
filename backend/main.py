"""
Main FastAPI application
"""
from fastapi import FastAPI, Depends, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
import os
from dotenv import load_dotenv

from utils.database import get_db, create_tables
from models import Customer, Product, Category, Order, Payment, OrderItem, Cart, Wishlist
# Load environment variables
load_dotenv()

app = FastAPI(
    title=os.getenv("APP_NAME", "Recommendation Platform API"),
    version=os.getenv("APP_VERSION", "1.0.0"),
    description="Real-time customer behavior recommendation platform API"
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=os.getenv("ALLOWED_ORIGINS", "http://localhost:3000").split(","),
    allow_credentials=True,
    allow_methods=os.getenv("ALLOWED_METHODS", "GET,POST,PUT,DELETE").split(","),
    allow_headers=os.getenv("ALLOWED_HEADERS", "*").split(","),
)


@app.on_event("startup")
async def startup_event():
    """Create database tables on startup"""
    create_tables()


@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "message": "Welcome to Recommendation Platform API",
        "version": os.getenv("APP_VERSION", "1.0.0"),
        "docs": "/docs"
    }


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy"}


@app.get("/customers")
async def get_customers(db: Session = Depends(get_db)):
    customers = db.query(Customer).all()
    return customers


@app.get("/products")
async def get_products(db: Session = Depends(get_db)):
    products = db.query(Product).all()
    return products


@app.get("/categories")
async def get_categories(db: Session = Depends(get_db)):
    categories = db.query(Category).all()
    return categories

@app.get("/order")
async def get_categories(db: Session = Depends(get_db)):
    orders = db.query(Order).all()
    return orders

@app.get("/payment")
async def get_categories(db: Session = Depends(get_db)):
    payments = db.query(Payment).all()
    return payments

@app.get("/order_item")
async def get_categories(db: Session = Depends(get_db)):
    items = db.query(OrderItem).all()
    return items

@app.get("/cart")
async def get_categories(db: Session = Depends(get_db)):
    carts = db.query(Cart).all()
    return carts

@app.get("/wishlist")
async def get_categories(db: Session = Depends(get_db)):
    wishlist = db.query(Wishlist).all()
    return wishlist

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
