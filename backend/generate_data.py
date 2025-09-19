"""
Data generation script for fixed/master data tables
This script generates data for:
- Categories (fixed/master data)
- Products (fixed/master data) 
- Customers (fixed/master data)

For dynamic user behavior simulation (orders, cart, wishlist), use simulate_user_behavior.py
"""
import sys
import os
from decimal import Decimal
from datetime import datetime, timedelta
import random

# Add the backend directory to the path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from utils.database import SessionLocal
from models.customer import Customer
from models.product import Product, Category
from faker import Faker
import bcrypt

fake = Faker(['vi_VN', 'en_US'])  # Vietnamese and English locales


class DataGenerator:
    def __init__(self):
        self.db = SessionLocal()
        
    def __enter__(self):
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.db.close()

    def hash_password(self, password: str) -> str:
        """Hash password using bcrypt"""
        salt = bcrypt.gensalt()
        hashed = bcrypt.hashpw(password.encode('utf-8'), salt)
        return hashed.decode('utf-8')

    def generate_categories(self, count: int = 10):
        """Generate product categories"""
        print(f"Generating {count} categories...")
        
        category_data = [
            ("Điện tử", "Thiết bị điện tử và phụ kiện công nghệ"),
            ("Thời trang", "Quần áo, giày dép và phụ kiện thời trang"),
            ("Sách", "Sách và tài liệu học tập"),
            ("Nhà cửa & Gia đình", "Đồ dùng gia đình và trang trí nội thất"),
            ("Thể thao", "Dụng cụ thể thao và trang phục tập luyện"),
            ("Làm đẹp", "Mỹ phẩm và sản phẩm chăm sóc sắc đẹp"),
            ("Ẩm thực", "Thực phẩm và đồ uống"),
            ("Xe cộ", "Phụ tùng và phụ kiện xe hơi, xe máy"),
            ("Giáo dục", "Đồ chơi giáo dục và học liệu"),
            ("Sức khỏe", "Sản phẩm chăm sóc sức khỏe")
        ]
        
        categories = []
        for i, (name, description) in enumerate(category_data[:count]):
            category = Category(
                name=name,
                description=description
            )
            categories.append(category)
            self.db.add(category)
        
        try:
            self.db.commit()
            print(f"✓ Created {len(categories)} categories")
            return categories
        except Exception as e:
            print(f"✗ Error creating categories: {e}")
            self.db.rollback()
            return []

    def generate_products(self, count: int = 100):
        """Generate products for existing categories"""
        print(f"Generating {count} products...")
        
        # Get existing categories
        categories = self.db.query(Category).all()
        if not categories:
            print("No categories found. Please generate categories first.")
            return []
        
        # Product templates by category
        product_templates = {
            "Điện tử": [
                ("Laptop Gaming", "Laptop chơi game hiệu năng cao", 15000000, 25000000),
                ("Smartphone", "Điện thoại thông minh", 5000000, 15000000),
                ("Tai nghe Bluetooth", "Tai nghe không dây chất lượng cao", 500000, 2000000),
                ("Máy tính bảng", "Tablet đa năng", 3000000, 8000000),
                ("Smartwatch", "Đồng hồ thông minh", 2000000, 6000000),
            ],
            "Thời trang": [
                ("Áo thun", "Áo thun cotton thoáng mát", 100000, 300000),
                ("Quần jeans", "Quần jean thời trang", 200000, 500000),
                ("Giày sneaker", "Giày thể thao phong cách", 800000, 2000000),
                ("Váy dự tiệc", "Váy đầm dự tiệc sang trọng", 500000, 1500000),
                ("Túi xách", "Túi xách thời trang", 300000, 1000000),
            ],
            "Sách": [
                ("Sách lập trình", "Sách học lập trình từ cơ bản đến nâng cao", 100000, 300000),
                ("Tiểu thuyết", "Tiểu thuyết văn học hay", 50000, 200000),
                ("Sách kinh doanh", "Sách về kinh doanh và khởi nghiệp", 150000, 400000),
                ("Sách nấu ăn", "Công thức nấu ăn ngon", 80000, 250000),
            ],
            "Nhà cửa & Gia đình": [
                ("Bộ chăn ga", "Bộ chăn ga gối cotton cao cấp", 300000, 800000),
                ("Nồi cơm điện", "Nồi cơm điện thông minh", 800000, 2000000),
                ("Bàn làm việc", "Bàn làm việc gỗ tự nhiên", 1000000, 3000000),
                ("Đèn trang trí", "Đèn trang trí phòng khách", 200000, 600000),
            ], 
            "Thể thao": [
                ("Giày chạy bộ", "Giày chạy bộ chuyên nghiệp", 1000000, 3000000),
                ("Quần áo thể thao", "Bộ quần áo thể thao thoáng mát", 300000, 800000),
                ("Bóng đá", "Bóng đá chính hãng", 200000, 600000),
                ("Tạ tay", "Tạ tay tập gym", 500000, 1500000),
            ],
            "Làm đẹp": [
                ("Son môi", "Son môi lâu trôi", 200000, 800000),
                ("Kem dưỡng da", "Kem dưỡng da ban ngày", 300000, 1000000),
                ("Nước hoa", "Nước hoa cao cấp", 500000,  2000000),
                ("Sữa rửa mặt", "Sữa rửa mặt dịu nhẹ", 150000, 500000),
            ],
            "Ẩm thực": [
                ("Cà phê hạt", "Cà phê hạt nguyên chất", 200000, 600000),
                ("Trà xanh", "Trà xanh thượng hạng", 100000, 400000), 
                ("Socola", "Socola nhập khẩu", 150000, 500000),
                ("Mứt trái cây", "Mứt trái cây tự nhiên", 80000, 300000),
            ],
            "Xe cộ": [
                ("Áo mưa xe máy", "Áo mưa chống thấm cho xe máy", 100000, 300000),
                ("Gương chiếu hậu", "Gương chiếu hậu xe hơi", 200000, 800000),
                ("Bộ đồ nghề sửa xe", "Bộ dụng cụ sửa chữa xe cơ bản", 500000, 1500000),
                ("Nước rửa xe", "Nước rửa xe chuyên dụng", 100000, 400000),
            ],
            "Giáo dục": [
                ("Đồ chơi xếp hình", "Đồ chơi xếp hình phát triển tư duy", 200000, 600000),
                ("Sách tô màu", "Sách tô màu cho trẻ em", 50000, 200000),
                ("Bảng học chữ", "Bảng học chữ cho bé", 300000, 800000),
                ("Đồ chơi khoa học", "Bộ đồ chơi thí nghiệm khoa học", 400000, 1200000),
            ],
            "Sức khỏe": [
                ("Thực phẩm chức năng", "Viên uống bổ sung vitamin", 200000, 800000),
                ("Máy đo huyết áp", "Máy đo huyết áp điện tử", 500000, 2000000),
                ("Nhiệt kế điện tử", "Nhiệt kế điện tử chính xác", 300000, 1000000),
                ("Khẩu trang y tế", "Khẩu trang y tế 4 lớp", 50000, 200000),
            ],
        }
        
        products = []
        for _ in range(count):
            category = random.choice(categories)
            category_name = category.name
            
            # Get product template for this category
            if category_name in product_templates:
                template = random.choice(product_templates[category_name])
                base_name, base_desc, min_price, max_price = template
                
                # Add variation to product name
                brand_prefixes = ["Sony", "Samsung", "Apple", "LG", "Xiaomi", "Adidas", "Nike", "Zara", "H&M"]
                name = f"{random.choice(brand_prefixes)} {base_name}"
                description = base_desc
                price = random.randint(min_price, max_price)
            else:
                # Generic product for categories without templates
                name = f"{fake.word().title()} {category_name}"
                description = f"Sản phẩm {category_name.lower()} chất lượng cao"
                price = random.randint(100000, 5000000)
            
            # Generate SKU
            sku = f"{category_name[:3].upper()}{fake.random_number(digits=6)}"
            
            product = Product(
                sku=sku,
                name=name,
                description=description,
                price=Decimal(str(price)),
                stock=random.randint(10, 100),
                category_id=category.category_id,
                is_active=random.choice([True, True, True, False])  # 75% active
            )
            
            products.append(product)
            self.db.add(product)
        
        try:
            self.db.commit()
            print(f"✓ Created {len(products)} products")
            return products
        except Exception as e:
            print(f"✗ Error creating products: {e}")
            self.db.rollback()
            return []

    def generate_customers(self, count: int = 50):
        """Generate customer accounts"""
        print(f"Generating {count} customers...")
        
        customers = []
        for _ in range(count):
            # Generate Vietnamese names more frequently
            if random.random() < 0.7:  # 70% Vietnamese names
                first_name = fake.first_name()
                last_name = fake.last_name()
            else:
                first_name = fake.first_name_nonbinary()
                last_name = fake.last_name_nonbinary()
            
            # Generate email
            email_prefix = f"{first_name.lower()}.{last_name.lower()}"
            email_suffix = random.choice(['@gmail.com', '@yahoo.com', '@hotmail.com', '@outlook.com'])
            email = f"{email_prefix}{random.randint(1, 999)}{email_suffix}"
            
            # Generate phone number (Vietnamese format)
            phone_prefixes = ['090', '091', '092', '093', '094', '095', '096', '097', '098', '099']
            phone = f"{random.choice(phone_prefixes)}{random.randint(1000000, 9999999)}"
            
            customer = Customer(
                first_name=first_name,
                last_name=last_name,
                email=email,
                password=self.hash_password("password123"),  # Default password
                address=fake.address(),
                phone_number=phone
            )
            
            customers.append(customer)
            self.db.add(customer)
        
        try:
            self.db.commit()
            print(f"✓ Created {len(customers)} customers")
            return customers
        except Exception as e:
            print(f"✗ Error creating customers: {e}")
            self.db.rollback()
            return []

    def clear_all_data(self):
        """Clear all data from database"""
        print("Clearing all data...")
        try:
            # Delete in correct order to avoid foreign key constraints
            self.db.query(Customer).delete()
            self.db.query(Product).delete()
            self.db.query(Category).delete()
            self.db.commit()
            print("✓ All data cleared")
        except Exception as e:
            print(f"✗ Error clearing data: {e}")
            self.db.rollback()

    def generate_all_master_data(self, categories=10, products=100, customers=50):
        """Generate all master/fixed data"""
        print("=== Generating Master Data ===")
        
        # Clear existing data
        self.clear_all_data()
        
        # Generate data
        categories = self.generate_categories(categories)
        if categories:
            products = self.generate_products(products)
            customers = self.generate_customers(customers)
            
            print("\n=== Master Data Generation Complete ===")
            print(f"Categories: {len(categories)}")
            print(f"Products: {len(products)}")
            print(f"Customers: {len(customers)}")
        else:
            print("Failed to generate categories. Stopping.")


def main():
    """Main function to run data generation"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Generate master data for recommendation platform")
    parser.add_argument("--categories", type=int, default=10, help="Number of categories to generate")
    parser.add_argument("--products", type=int, default=100, help="Number of products to generate")
    parser.add_argument("--customers", type=int, default=50, help="Number of customers to generate")
    parser.add_argument("--clear", action="store_true", help="Clear all existing data")
    
    args = parser.parse_args()
    
    with DataGenerator() as generator:
      if args.clear:
          generator.clear_all_data()
      else:
        generator.generate_all_master_data(
            categories=args.categories,
            products=args.products,
            customers=args.customers
        )


if __name__ == "__main__":
    main()
