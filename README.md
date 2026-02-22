# Order Processing System

Event-driven e-commerce backend demonstration.

## Services
- **Order Service**: Entry point, emits `order.created`.
- **Inventory Service**: Consumes `order.created`, uses optimistic locking, emits `inventory.reserved`.
- **Payment Service**: Consumes `inventory.reserved`, implements exponential backoff/retries, emits `payment.succeeded` or `order.dlq`.
- **Notification Service**: Consumes all result events and logs notifications.

## Reliability Patterns
- **Idempotency**: UUID-based deduplication in Order DB.
- **Circuit Breaker**: Simulated gateway protection.
- **Dead Letter Queue (DLQ)**: Failed payments routed to `order.dlq`.
- **Optimistic Locking**: Version-based consistency in Inventory.

## Setup
```bash
docker-compose up --build
```
Post an order:
```bash
curl -X POST "http://localhost:8001/orders?product_id=laptop&quantity=1&idempotency_key=unique-key-1"
```
