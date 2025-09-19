import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from backend.utils.database import SessionLocal
from backend.models import Customer

db = SessionLocal()

def insert_customer(): 
    new_customer = Customer(first_name="John", last_name="Doe", email="john.doe@example.com", password="securepassword", address="123 Main St", phone_number="123-456-7890")
    db.add(new_customer)
    db.commit()
    db.refresh(new_customer)
    print(f"Inserted: {new_customer}")

def update_customer():
    email = "john.doe@example.com"
    customer = db.query(Customer).filter(Customer.email == email).first()
    if customer:
        customer.first_name = "Jane"
        customer.last_name = "Doe"
        db.commit()
        db.refresh(customer)
        print(f"Updated: {customer}")
    else:
        print("Customer not found.")

def delete_customer():
    email = "john.doe@example.com"
    customer = db.query(Customer).filter(Customer.email == email).first()
    if customer:
        db.delete(customer)
        db.commit()
        print(f"Deleted customer with email: {email}")

if __name__ == "__main__":
    insert_customer()
    update_customer()
    delete_customer()