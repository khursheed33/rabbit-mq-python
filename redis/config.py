# config.py
from pydantic import BaseModel

class Settings(BaseModel):
    REDIS_HOST: str = "localhost"
    REDIS_PORT: int = 6379
    REDIS_PASSWORD: str = "root"
    API_HOST: str = "0.0.0.0"
    API_PORT: int = 8000

settings = Settings()
