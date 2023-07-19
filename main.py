from fastapi import FastAPI
from pydantic import BaseModel, Field

class User(BaseModel):
    user_id: int
    user_name: str = Field(max_length=12)


app = FastAPI()

@app.get("/")
async def index():
    return {"message": "Success"}

@app.post("/users")
async def users(users:User):

    return {"code":200, "users": users}

