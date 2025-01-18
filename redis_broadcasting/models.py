from pydantic import BaseModel

class Settings(BaseModel):
    REDIS_HOST: str = "localhost"
    REDIS_PORT: int = 6379
    REDIS_PASSWORD: str = "root"
    MESSAGE_RETENTION_DAYS: int = 7
