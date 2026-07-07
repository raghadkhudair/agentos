import asyncpg
from pathlib import Path
from agentos.config.settings import Settings

class DatabaseManager:
    """Manages async PostgreSQL connection pooling and schema initialization."""
    
    def __init__(self, settings: Settings):
        self.settings = settings
        self.pool = None

    async def connect(self) -> None:
        """Establish the connection pool."""
        if not self.pool:
            # Connects using the database_url from settings.py
            self.pool = await asyncpg.create_pool(self.settings.database_url)

    async def disconnect(self) -> None:
        """Close the connection pool cleanly."""
        if self.pool:
            await self.pool.close()

    async def initialize_schema(self) -> None:
        """Read schema.sql and execute it to build the tables and vector extension."""
        if not self.pool:
            raise RuntimeError("Database pool not initialized. Call connect() first.")
            
        schema_path = Path(__file__).parent / "schema.sql"
        schema_sql = schema_path.read_text(encoding="utf-8")
        
        async with self.pool.acquire() as conn:
            await conn.execute(schema_sql)