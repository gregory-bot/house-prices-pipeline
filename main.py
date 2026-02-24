
from fastapi import FastAPI, HTTPException, Response
from fastapi.middleware.cors import CORSMiddleware
from typing import List, Optional
from pydantic import BaseModel
import asyncpg
import os
import math
import secrets
import csv
import io

app = FastAPI()

# Enable CORS for frontend requests
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Or specify ["http://localhost:8080"]
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# --- Register API Key Endpoint ---
class RegisterRequest(BaseModel):
    name: str
    email: str
    phone: Optional[str] = None
    company: Optional[str] = None

@app.post("/register")
async def register_user(req: RegisterRequest):
    api_key = secrets.token_hex(32)
    query = """
        INSERT INTO developers (full_name, email, phone, company, api_key)
        VALUES ($1, $2, $3, $4, $5)
        RETURNING api_key
    """
    async with app.state.pool.acquire() as conn:
        try:
            row = await conn.fetchrow(query, req.name, req.email, req.phone, req.company, api_key)
            return {"api_key": row["api_key"]}
        except Exception as e:
            raise HTTPException(status_code=400, detail="Registration failed: " + str(e))


# --- Download Properties as CSV Endpoint ---
@app.get("/properties/csv")
async def download_properties_csv():
    query = "SELECT * FROM nairobi_properties ORDER BY scraped_at DESC"
    async with app.state.pool.acquire() as conn:
        rows = await conn.fetch(query)
        if not rows:
            raise HTTPException(status_code=404, detail="No properties found")
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=rows[0].keys())
        writer.writeheader()
        for row in rows:
            writer.writerow(dict(row))
        csv_data = output.getvalue()
        output.close()
        return Response(content=csv_data, media_type="text/csv", headers={
            "Content-Disposition": "attachment; filename=properties.csv"
        })

@app.get("/")
async def root():
    return {"message": "Welcome to the Nairobi Property API!"}


DB_CONFIG = {
    'user': os.environ.get('DB_USER'),
    'password': os.environ.get('DB_PASSWORD'),
    'database': os.environ.get('DB_NAME'),
    'host': os.environ.get('DB_HOST'),
    'port': int(os.environ.get('DB_PORT', 5432)),
}


@app.on_event("startup")
async def startup():
    app.state.pool = await asyncpg.create_pool(**DB_CONFIG, min_size=1, max_size=5)

@app.on_event("shutdown")
async def shutdown():
    await app.state.pool.close()

@app.get("/properties")
async def get_properties(limit: int = 20, offset: int = 0):
    query = "SELECT * FROM nairobi_properties ORDER BY scraped_at DESC LIMIT $1 OFFSET $2"
    async with app.state.pool.acquire() as conn:
        rows = await conn.fetch(query, limit, offset)
        return [dict(row) for row in rows]

@app.get("/properties/{property_id}")
async def get_property(property_id: int):
    query = "SELECT * FROM nairobi_properties WHERE id = $1"
    async with app.state.pool.acquire() as conn:
        row = await conn.fetchrow(query, property_id)
        if row:
            return dict(row)
        raise HTTPException(status_code=404, detail="Property not found")

@app.get("/summary/location")
async def get_location_summary():
    query = """
        SELECT location, AVG(price_normalized) as avg_price, AVG(price_per_bedroom) as avg_price_per_bedroom, COUNT(*) as count
        FROM nairobi_properties
        GROUP BY location
        ORDER BY avg_price_per_bedroom ASC
    """
    async with app.state.pool.acquire() as conn:
        rows = await conn.fetch(query)
        result = []
        for row in rows:
            row_dict = dict(row)
            for k, v in row_dict.items():
                if isinstance(v, float) and (math.isnan(v) or v is None):
                    row_dict[k] = None
            result.append(row_dict)
        return result
