"""
User behavior simulation script
This script simulates real user behavior to generate dynamic data for:
- Cart items (user adding/removing items from cart)
- Wishlist items (user adding/removing items to wishlist)
- Orders and Order items (user placing orders)
- Payments (processing payments for orders)
- Shipments (shipping orders)

This creates realistic data flow patterns for recommendation system training
"""
import sys
import os
from decimal import Decimal
from datetime import datetime, timedelta
import random
import time
from typing import List, Optional

# Add the backend directory to the path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from utils.database import SessionLocal
from models.customer import Customer
from models.product import Product, Category
from models.order import Order, OrderItem
from models.cart import Cart, Wishlist
from models.payment import Payment
from models.shipment import Shipment


class UserBehaviorSimulator:
    def __init__(self):
        self.db = SessionLocal()
        
    def __enter__(self):
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.db.close()

    def get_random_customer(self) -> Optional[Customer]:
        """Get a random customer from database"""
        customers = self.db.query(Customer).all()
        return random.choice(customers) if customers else None

    def get_random_products(self, count: int = 1) -> List[Product]:
        """Get random products from database"""
        products = self.db.query(Product).filter(Product.is_active == True).all()
        if not products:
            return []
        return random.sample(products, min(count, len(products)))

    def simulate_browsing_behavior(self, customer: Customer, duration_minutes: int = 30):
        """Simulate a customer browsing session"""
        print(f"👤 Customer {customer.email} starts browsing...")
        
        session_start = datetime.now()
        session_end = session_start + timedelta(minutes=duration_minutes)
        
        actions = []
        current_time = session_start
        
        while current_time < session_end:
            # Random action weights
            action_weights = {
                'view_product': 0.4,
                'add_to_cart': 0.2,
                'add_to_wishlist': 0.15,
                'remove_from_cart': 0.1,
                'place_order': 0.1,
                'browse_category': 0.05
            }
            
            action = random.choices(
                list(action_weights.keys()),
                weights=list(action_weights.values())
            )[0]
            
            try:
                if action == 'view_product':
                    # Xử lý xem sản phẩm - có thể là print hoặc gọi hàm
                    print(f"👀 Customer viewed a product")
                elif action == 'browse_category':
                    # Xử lý duyệt danh mục
                    print(f"🔍 Customer browsed a category")
                if action == 'add_to_cart':
                    self.add_to_cart(customer)
                elif action == 'add_to_wishlist':
                    self.add_to_wishlist(customer)
                elif action == 'remove_from_cart':
                    self.remove_from_cart(customer)
                elif action == 'place_order':
                    if self.place_order(customer):
                        break  # End session after placing order
                
                actions.append(action)
                
            except Exception as e:
                print(f"Error during {action}: {e}")

            finally:
                # Luôn cập nhật thời gian và delay thực sự
                wait_time = random.randint(1, 3)
                time.sleep(wait_time)  # Delay thực sự
                current_time = datetime.now()  # Cập nhật thời gian thực

        print(f"👤 Customer {customer.email} session ended. Actions: {len(actions)}")
        return actions

    def add_to_cart(self, customer: Customer) -> bool:
        """Add a random product to customer's cart"""
        products = self.get_random_products(1)
        if not products:
            return False
        
        product = products[0]
        
        # Check if product already in cart
        existing_cart = self.db.query(Cart).filter(
            Cart.customer_id == customer.customer_id,
            Cart.product_id == product.product_id
        ).first()
        
        if existing_cart:
            # Increase quantity
            existing_cart.quantity += random.randint(1, 3)
            print(f"🛒 Increased quantity for {product.name} in cart")
        else:
            # Add new item to cart
            cart_item = Cart(
                customer_id=customer.customer_id,
                product_id=product.product_id,
                quantity=random.randint(1, 5)
            )
            self.db.add(cart_item)
            print(f"🛒 Added {product.name} to cart")
        
        try:
            self.db.commit()
            return True
        except Exception as e:
            print(f"Error adding to cart: {e}")
            self.db.rollback()
            return False

    def add_to_wishlist(self, customer: Customer) -> bool:
        """Add a random product to customer's wishlist"""
        products = self.get_random_products(1)
        if not products:
            return False
        
        product = products[0]
        
        # Check if product already in wishlist
        existing_wishlist = self.db.query(Wishlist).filter(
            Wishlist.customer_id == customer.customer_id,
            Wishlist.product_id == product.product_id
        ).first()
        
        if existing_wishlist:
            return False  # Already in wishlist
        
        wishlist_item = Wishlist(
            customer_id=customer.customer_id,
            product_id=product.product_id
        )
        self.db.add(wishlist_item)
        print(f"❤️ Added {product.name} to wishlist")
        
        try:
            self.db.commit()
            return True
        except Exception as e:
            print(f"Error adding to wishlist: {e}")
            self.db.rollback()
            return False

    def remove_from_cart(self, customer: Customer) -> bool:
        """Remove a random item from customer's cart"""
        cart_items = self.db.query(Cart).filter(
            Cart.customer_id == customer.customer_id
        ).all()
        
        if not cart_items:
            return False
        
        cart_item = random.choice(cart_items)
        product_name = cart_item.product.name
        
        if cart_item.quantity > 1:
            # Decrease quantity
            cart_item.quantity -= 1
            print(f"🛒 Decreased quantity for {product_name} in cart")
        else:
            # Remove completely
            self.db.delete(cart_item)
            print(f"🗑️ Removed {product_name} from cart")
        
        try:
            self.db.commit()
            return True
        except Exception as e:
            print(f"Error removing from cart: {e}")
            self.db.rollback()
            return False

    def place_order(self, customer: Customer) -> bool:
        """Place an order with items from customer's cart"""
        cart_items = self.db.query(Cart).filter(
            Cart.customer_id == customer.customer_id
        ).all()
        
        if not cart_items:
            return False
        
        # Calculate total price
        total_price = Decimal('0')
        for cart_item in cart_items:
            total_price += cart_item.product.price * cart_item.quantity
        
        # Create order
        order = Order(
            customer_id=customer.customer_id,
            total_price=total_price,
            status="pending",
        )
        self.db.add(order)
        self.db.flush()  # Get order ID
        
        # Create order items from cart
        for cart_item in cart_items:
            order_item = OrderItem(
                order_id=order.order_id,
                product_id=cart_item.product_id,
                quantity=cart_item.quantity,
                price=cart_item.product.price
            )
            self.db.add(order_item)
        
        # Create payment
        payment = Payment(
            customer_id=customer.customer_id,
            amount=total_price,
            payment_method=random.choice(["credit_card", "debit_card", "paypal", "bank_transfer"]),
            status="completed",
            transaction_id=f"TXN{random.randint(100000, 999999)}"
        )
        self.db.add(payment)
        self.db.flush()
        
        # Link payment to order
        order.payment_id = payment.payment_id
        
        # Create shipment
        shipment = Shipment(
            customer_id=customer.customer_id,
            address=customer.address or "123 Default Street",
            city="Ho Chi Minh City",
            country="Vietnam",
            zip_code=f"{random.randint(10000, 99999)}",
            status="preparing",
            tracking_number=f"TRACK{random.randint(100000, 999999)}"
        )
        self.db.add(shipment)
        self.db.flush()
        
        # Link shipment to order
        order.shipment_id = shipment.shipment_id
        order.status = "confirmed"
        
        # Clear cart
        for cart_item in cart_items:
            self.db.delete(cart_item)
        
        try:
            self.db.commit()
            print(f"📦 Order placed! Total: {total_price:,.0f} VND, Items: {len(cart_items)}")
            return True
        except Exception as e:
            print(f"Error placing order: {e}")
            self.db.rollback()
            return False

    def simulate_multiple_users(self, num_sessions: int = 20, session_duration: int = 30):
        """Simulate multiple user sessions"""
        print(f"🎭 Starting simulation of {num_sessions} user sessions...")
        
        for i in range(num_sessions):
            customer = self.get_random_customer()
            if customer:
                print(f"\n--- Session {i+1}/{num_sessions} ---")
                self.simulate_browsing_behavior(customer, session_duration)
                
                # Small delay between sessions
                time.sleep(0.5)
            else:
                print("No customers found. Please generate customer data first.")
                break
        
        print(f"\n🎭 Simulation complete! {num_sessions} sessions processed.")

    def generate_historical_data(self, days_back: int = 30):
        """Generate historical user behavior data"""
        print(f"📊 Generating {days_back} days of historical data...")
        
        customers = self.db.query(Customer).all()
        if not customers:
            print("No customers found. Please generate customer data first.")
            return
        
        # Generate data for each day
        for day in range(days_back):
            date = datetime.now() - timedelta(days=day)
            daily_sessions = random.randint(5, 20)  # 5-20 sessions per day
            
            print(f"📅 Day {day+1}/{days_back}: {date.strftime('%Y-%m-%d')} - {daily_sessions} sessions")
            
            for _ in range(daily_sessions):
                customer = random.choice(customers)
                self.simulate_browsing_behavior(customer, random.randint(10, 60))
        
        print("📊 Historical data generation complete!")

    def clear_dynamic_data(self):
        """Clear all dynamic data (keep master data)"""
        print("🧹 Clearing dynamic data...")
        try:
            # Delete in correct order to avoid foreign key constraints
            self.db.query(OrderItem).delete()
            self.db.query(Order).delete()
            self.db.query(Payment).delete()
            self.db.query(Shipment).delete()
            self.db.query(Cart).delete()
            self.db.query(Wishlist).delete()
            self.db.commit()
            print("✓ Dynamic data cleared")
        except Exception as e:
            print(f"✗ Error clearing dynamic data: {e}")
            self.db.rollback()


def main():
    """Main function to run user behavior simulation"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Simulate user behavior for recommendation platform")
    parser.add_argument("--sessions", type=int, default=5, help="Number of user sessions to simulate")
    parser.add_argument("--duration", type=int, default=5, help="Average session duration in minutes")
    parser.add_argument("--historical", type=int, help="Generate historical data for N days back")
    parser.add_argument("--clear", action="store_true", help="Clear all dynamic data")
    
    args = parser.parse_args()
    
    with UserBehaviorSimulator() as simulator:
        if args.clear:
            simulator.clear_dynamic_data()
        elif args.historical:
            simulator.generate_historical_data(args.historical)
        else:
            simulator.simulate_multiple_users(args.sessions, args.duration)


if __name__ == "__main__":
    main()