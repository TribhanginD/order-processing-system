from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy.orm import Session
from . import models, kafka
import uuid
import os

models.Base.metadata.create_all(bind=models.engine)

app = FastAPI(title="Order Service")

def get_db():
    db = models.SessionLocal()
    try:
        yield db
    finally:
        db.close()

@app.post("/orders")
def create_order(product_id: str, quantity: int, idempotency_key: str, db: Session = Depends(get_db)):
    # Check idempotency
    existing = db.query(models.Order).filter(models.Order.idempotency_key == idempotency_key).first()
    if existing:
        return existing

    order_id = str(uuid.uuid4())
    new_order = models.Order(
        id=order_id,
        product_id=product_id,
        quantity=quantity,
        idempotency_key=idempotency_key
    )
    db.add(new_order)
    db.commit()
    db.refresh(new_order)

    # Emit event
    kafka.producer.send_event("order.created", {
        "order_id": order_id,
        "product_id": product_id,
        "quantity": quantity
    })

    return new_order

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/orders/{order_id}")
def get_order(order_id: str, db: Session = Depends(get_db)):
    order = db.query(models.Order).filter(models.Order.id == order_id).first()
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    return order
