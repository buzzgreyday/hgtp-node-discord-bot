from fastapi import HTTPException, FastAPI, Request
import stripe
import os

dev_env = os.getenv("NODEBOT_DEV_ENV")

app = FastAPI()
if dev_env:
    stripe.api_key = os.getenv("STRIPE_API_TEST_KEY")
    endpoint_secret = os.getenv("STRIPE_ENDPOINT_SECRET")
else:
    stripe.api_key = os.getenv("STRIPE_API_KEY")
    endpoint_secret = os.getenv("STRIPE_ENDPOINT_SECRET")

@app.post("/webhooks")
async def stripe_webhook(request: Request):
    try:
        payload = await request.json()
        # Just log and return a simple success response
        event_type = payload.get("type")
        # Log the event type for debugging
        print(f"Received event: {event_type}")

        if event_type == 'payment_intent.succeeded':
            handle_payment_succeeded(payload)
            # Add your logic to handle successful payment
        elif event_type == 'customer.subscription.created':
            print("Subscription created!")
            handle_subscription_created(payload)
            # Add your logic to handle subscription creation

        return {"status": "success"}
    except Exception as e:
        print(f"Error: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")


def handle_payment_succeeded(payload):
    # Update your database with successful payment information
    print(payload)


def handle_subscription_created(payload):
    # Update your database with the new subscription information
    print("Customer ID:", payload["data"]["object"]["customer"])