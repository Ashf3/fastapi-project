from fastapi import FastAPI, HTTPException
from supabase import create_client, Client
import os

# Initialize FastAPI app
app = FastAPI()

# Initialize Supabase client
url = os.getenv('SUPABASE_URL')  # Supabase URL from environment variable
key = os.getenv('SUPABASE_KEY')  # Supabase API key from environment variable
supabase: Client = create_client(url, key)

@app.get("/companies/count")
async def get_company_count():
    try:
        # Query the count of unique companies
        result = supabase.table('company').select('id', count='exact').execute()
        return {"company_count": result.count}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/directors/count")
async def get_director_count():
    try:
        # Query the count of unique directors
        result = supabase.table('directors').select('id', count='exact').execute()
        return {"director_count": result.count}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
