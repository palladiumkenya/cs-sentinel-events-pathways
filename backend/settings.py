from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    db_username: str
    db_password: str
    db_host: str
    db_name: str

    class Config:
        env_file = None  # No .env file used
