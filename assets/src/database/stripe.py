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
        payload = await request.body()
        sig_header = request.headers.get('stripe-signature')
        print(sig_header)
        try:
            # Construct the event to validate the signature
            event = stripe.Webhook.construct_event(
                payload, sig_header, endpoint_secret
            )
            print("Event verified:", event)
        except ValueError as e:
            # Invalid payload
            print(f"Invalid payload: {e}")
            raise HTTPException(status_code=400, detail="Invalid payload")
        except stripe.error.SignatureVerificationError as e:
            # Invalid signature
            print(f"Signature verification failed: {e}")
            raise HTTPException(status_code=400, detail="Invalid signature")
        # Just log and return a simple success response
        # Log the event type for debugging
        print(f"Received event: {event['type']}")


        if event['type'] == 'payment_intent.succeeded':
            handle_payment_succeeded(event)
            # Add your logic to handle successful payment
        elif event['type'] == 'customer.subscription.created':
            print("Subscription created!")
            await handle_subscription_created(event)
            # Add your logic to handle subscription creation

        return {"status": "success"}
    except Exception as e:
        print(f"{e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")


def handle_payment_succeeded(payload):
    # Update your database with successful payment information
    print(payload)


async def handle_subscription_created(payload):
    # Update your database with the new subscription information
    # We need to insert multiple subscriptions in the local database
    # REMEMBER TO ADD "customer_id" to database schema and model


    print("customer_id", payload["data"]["object"]["customer"])
