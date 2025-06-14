"""Runtime config using pydantic BaseSettings."""
from pydantic import BaseSettings, Field

class Settings(BaseSettings):
    # Database
    postgres_host: str = Field(..., env="POSTGRES_HOST")
    postgres_port: int = Field(5432, env="POSTGRES_PORT")
    postgres_db: str = Field(..., env="POSTGRES_DB")
    postgres_user: str = Field(..., env="POSTGRES_USER")
    postgres_password: str = Field(..., env="POSTGRES_PASSWORD")

    # Source data
    source_data_path: str = Field(..., env="SOURCE_DATA_PATH")

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"

settings = Settings()
