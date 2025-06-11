import grpc.experimental.gevent as grpc_gevent
grpc_gevent.init_gevent()

import time
from locust import User, task, between, events

# --- Configuration ---
PROJECT_ID = "diesel-patrol-382622"
REGION = "us-central1"
ENDPOINT_ID = "73514446944731136" 

INPUT_TOKEN_COUNT = 9000
OUTPUT_TOKEN_COUNT = 1024

def generate_prompt(token_count):
    """Generates a string with a specific number of tokens."""
    return " ".join(["test"] * token_count)

class VertexAIUser(User):
    wait_time = between(1, 2)
    
    # We will initialize the high-level endpoint object in on_start.
    endpoint = None
    prompt = None

    def on_start(self):
        """
        This method runs inside each worker process.
        We will import the library and initialize the high-level Endpoint object.
        """
        # STEP 2: Perform local import.
        from google.cloud import aiplatform
        from google.api_core import exceptions

        try:
            # STEP 3: Create the full resource name string for the endpoint.
            endpoint_name_str = f"projects/{PROJECT_ID}/locations/{REGION}/endpoints/{ENDPOINT_ID}"
            
            # STEP 4: Initialize the high-level aiplatform.Endpoint object.
            # This object handles the dedicated domain connection automatically.
            self.endpoint = aiplatform.Endpoint(endpoint_name=endpoint_name_str)
            
            self.prompt = generate_prompt(INPUT_TOKEN_COUNT)

            print("SUCCESSFULLY INITIATED CONNECTION")
            
        except Exception as e:
            print(f"FATAL: Failed to initialize user. Quitting. Error: {e}")
            self.environment.runner.quit()


    @task
    def predict(self):
        """
        Sends a prediction request using the high-level endpoint object.
        """
        from google.api_core import exceptions

        if not self.endpoint:
            return

        # This high-level client takes a simple list of Python dictionaries.
        # No need to manually convert to protobuf.
        instances_list = [
            { "prompt": self.prompt, "max_tokens": OUTPUT_TOKEN_COUNT }
        ]

        request_name = "vertex_predict_phi3"
        start_time = time.time()

        try:
            # STEP 5: Call the .predict() method on the Endpoint object itself.
            self.endpoint.predict(instances=instances_list)

            total_time = int((time.time() - start_time) * 1000)
            events.request.fire(
                request_type="gRPC", name=request_name, response_time=total_time, response_length=0
            )
        except exceptions.GoogleAPICallError as e:
            total_time = int((time.time() - start_time) * 1000)
            events.request.fire(
                request_type="gRPC", name=request_name, response_time=total_time, response_length=0, exception=e
            )
