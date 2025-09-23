import os
import sys 
from dotenv import load_dotenv
load_dotenv()

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(os.path.join(os.path.dirname(__file__), "..", ".."))

from fastapi import FastAPI
from fastapi import Depends
from sqlalchemy.orm import Session
from sqlalchemy import text
from backend.utils.database import get_db

app = FastAPI()

@app.post("/enrich-data")
async def enrich_data(data: dict, db: Session = Depends(get_db)):
    product_id = data.get("product_id")
    if not product_id:
        return {"error": "product_id is required"}

    product = db.execute(
      text("""
        SELECT *
        FROM products
        WHERE product_id = :product_id
      """),
      {"product_id": product_id}
    ).fetchone()
    print(product)

    if not product:
        return {"error": "Product not found"}
  
    return {
      "product_name": product.name,
      "category_id": product.category_id,
      "price": product.price, 
      "rank": data.get("rank"),
      "quantity": data.get("quantity"),
      "window_start": data.get("window_start"),
      "window_end": data.get("window_end")
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
