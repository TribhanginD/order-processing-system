from locust import HttpUser, task, between
import uuid

class OrderUser(HttpUser):
    wait_time = between(1, 2)

    @task
    def place_order(self):
        self.client.post("/orders", params={
            "product_id": "laptop",
            "quantity": 1,
            "idempotency_key": str(uuid.uuid4())
        })

    @task(3)
    def view_order(self):
        # We assume some order IDs exist or just use a dummy for load testing GET performance
        self.client.get("/orders/dummy-id")
