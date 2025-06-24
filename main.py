# Import the FastAPI class from fastapi library.
# This class is what we'll use to create our web application.
from fastapi import FastAPI

# Import the router object that contains your API endpoints
from app.api.routes import router


# Create an instance of the FastAPI app.
# Think of this like the brain of your app. It handles incoming requests and sends responsed.
app = FastAPI()

# Register the router with the app (attaches all defined routes)
app.include_router(router)



# This line defines a route, a URL path that users or programs can call.
# The @ symbol is a decorator, which means it modifies the function directly below it.
# In this case, it tells FastAPI: "If someone accesses the root URL '/', run the function below."
@app.get("/")

def read_root():
    # This is what will be returned when someone visits the root URL. 
    # It returns a python dictionary, which FastAPI automatically converts to JSON for the web.
    
    return {"ClimaStation": "Backend is running!"}




