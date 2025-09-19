import time
import random
import json
from datetime import datetime
from models.customer import Customer
from models.product import Product
from models.cart import Cart
from models.order import Order
from models.payment import Payment
from models.shipment import Shipment
from utils.database import SessionLocal
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class OrderSimulator:
    def __init__(self):
        self.customers = []
        self.products = []
        self.load_customers()
        self.load_products()
    
    def load_customers(self):
        """Load existing customers from database"""
        db = SessionLocal()
        try:
            # Query customers using SQLAlchemy ORM
            customers_query = db.query(Customer).limit(50).all()
            
            for customer in customers_query:
                self.customers.append(customer)
            
            logger.info(f"Loaded {len(self.customers)} customers")
        except Exception as e:
            logger.error(f"Error loading customers: {e}")
        finally:
            db.close()
    
    def load_products(self):
        """Load existing products from database"""
        db = SessionLocal()
        try:
            # Query products using SQLAlchemy ORM
            products_query = db.query(Product).limit(50).all()
            
            for product in products_query:
                self.products.append(product)
            
            logger.info(f"Loaded {len(self.products)} products")
        except Exception as e:
            logger.error(f"Error loading products: {e}")
        finally:
            db.close()
    
    def create_random_order(self):
        """Create a random order for simulation"""
        db = SessionLocal()
        try:
            # Chọn ngẫu nhiên khách hàng
            customer = random.choice(self.customers)
            
            # Tạo giỏ hàng với 1-5 sản phẩm ngẫu nhiên
            num_items = random.randint(1, 5)
            
            total_amount = 0
            cart_items = []
            
            for _ in range(num_items):
                product = random.choice(self.products)
                quantity = random.randint(1, 3)
                
                cart_item = {
                    'product_id': product.product_id,
                    'product_name': product.name,
                    'quantity': quantity,
                    'price': float(product.price),
                    'subtotal': float(product.price) * quantity
                }
                
                cart_items.append(cart_item)
                total_amount += cart_item['subtotal']
            
            # Tạo đơn hàng
            order = Order(
                customer_id=customer.customer_id,
                total_price=total_amount,
                status='pending'
            )
            db.add(order)
            db.commit()
            db.refresh(order)
            
            # Tạo thanh toán
            payment_methods = ['credit_card', 'debit_card', 'paypal', 'bank_transfer']
            payment = Payment(
                customer_id=customer.customer_id,
                amount=total_amount,
                payment_method=random.choice(payment_methods),
                status='completed',
                transaction_id=f"TXN_{random.randint(100000, 999999)}"
            )
            db.add(payment)
            db.commit()
            db.refresh(payment)
            
            # Update order with payment_id
            order.payment_id = payment.payment_id
            
            # Tạo thông tin giao hàng
            shipment = Shipment(
                customer_id=customer.customer_id,
                address=f"Address {random.randint(1, 1000)}",
                city=f"City {random.randint(1, 50)}",
                country="Vietnam",
                status='preparing',
                tracking_number=f"TRACK_{random.randint(100000, 999999)}"
            )
            db.add(shipment)
            db.commit()
            db.refresh(shipment)
            
            # Update order with shipment_id
            order.shipment_id = shipment.shipment_id
            db.commit()
            
            # Log thông tin đơn hàng
            customer_name = f"{customer.first_name} {customer.last_name}"
            order_info = {
                'timestamp': datetime.now().isoformat(),
                'order_id': order.order_id,
                'customer_id': customer.customer_id,
                'customer_name': customer_name,
                'total_amount': total_amount,
                'items_count': len(cart_items),
                'payment_method': payment.payment_method,
                'cart_items': cart_items
            }
            
            logger.info(f"Order created: {json.dumps(order_info, indent=2)}")
            return order_info
            
        except Exception as e:
            db.rollback()
            logger.error(f"Error creating order: {e}")
            return None
        finally:
            db.close()
    
    def run_simulation(self, duration_minutes=60):
        """Run order simulation for specified duration"""
        logger.info(f"Starting order simulation for {duration_minutes} minutes...")
        
        start_time = time.time()
        end_time = start_time + (duration_minutes * 60)
        order_count = 0
        
        try:
            while time.time() < end_time:
                # Tạo đơn hàng
                order_info = self.create_random_order()
                
                if order_info:
                    order_count += 1
                    logger.info(f"Total orders created: {order_count}")
                
                # Sleep 3-5 giây
                sleep_time = random.uniform(3, 5)
                logger.info(f"Waiting {sleep_time:.2f} seconds before next order...")
                time.sleep(sleep_time)
                
        except KeyboardInterrupt:
            logger.info("Simulation interrupted by user")
        except Exception as e:
            logger.error(f"Error in simulation: {e}")
        
        logger.info(f"Simulation completed. Total orders created: {order_count}")

def main():
    simulator = OrderSimulator()
    
    # Kiểm tra dữ liệu
    if not simulator.customers:
        logger.error("No customers found. Please run generate_data.py first.")
        return
    
    if not simulator.products:
        logger.error("No products found. Please run generate_data.py first.")
        return
    
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument(
      "-t",
      "--duration", 
      default=10,
      type=int,
      help="Duration of the simulation in minutes",
    )
    args = parser.parse_args()
    parsed_args = vars(args)
    simulator.run_simulation(duration_minutes=parsed_args["duration"])

if __name__ == "__main__":
    main()