import os
from datetime import date, datetime, timedelta
from fastapi import FastAPI, Query, Depends, HTTPException
from typing import List, Optional
from supabase import create_client, Client

# Initialize FastAPI app
app = FastAPI()

# Initialize Supabase client
url = os.getenv('SUPABASE_URL')  # Supabase URL from environment variable
key = os.getenv('SUPABASE_KEY')  # Supabase API key from environment variable
supabase: Client = create_client(url, key)

# Filter function with case-insensitive string matching
def company_filters(
    cnumber: Optional[str] = None,
    cname: Optional[str] = None,
    address_line_1: Optional[str] = None,
    address_line_2: Optional[str] = None,
    address_locality: Optional[str] = None,
    address_region: Optional[str] = None,
    address_country: Optional[str] = None,
    address_postal_code: Optional[str] = None,
    siccodes: Optional[str] = None,
    capital_amount: Optional[str] = None,
    capital_currency: Optional[str] = None,
    incorporated_from: Optional[date] = None,
    incorporated_to: Optional[date] = None,
    sort_by: Optional[str] = None,
    sort_order: Optional[str] = "desc" 
):
    filters = {}
    
    # Case-insensitive filtering for string fields using ilike
    if cnumber:
        filters['cnumber'] = cnumber
    if cname:
        filters['cname'] = cname
    if address_line_1:
        filters['address_line_1'] = address_line_1
    if address_line_2:
        filters['address_line_2'] = address_line_2
    if address_locality:
        filters['address_locality'] = address_locality
    if address_region:
        filters['address_region'] = address_region
    if address_country:
        filters['address_country'] = address_country
    if address_postal_code:
        filters['address_postal_code'] = address_postal_code
    if siccodes:
        filters['siccodes'] = siccodes
    if capital_amount:
        filters['capital_amount'] = capital_amount
    if capital_currency:
        filters['capital_currency'] = capital_currency
    if incorporated_from:
        filters['incorporated_from'] = incorporated_from.isoformat()
    if incorporated_to:
        filters['incorporated_to'] = incorporated_to.isoformat()

    return filters, sort_by, sort_order

# Utility function to query data or get the count of records
def query_companies(filters: dict, sort_by: Optional[str] = None, sort_order: Optional[str] = "desc", date_from: Optional[date] = None, date_to: Optional[date] = None, count_only: bool = False):
    query = supabase.table('company').select("*" if not count_only else "count", count='exact')
    
    # Apply filters
    for column, value in filters.items():
        if column in ['cnumber', 'cname', 'address_line_1', 'address_line_2', 'address_locality', 
                      'address_region', 'address_country', 'address_postal_code', 'siccodes', 'capital_currency']:
            query = query.ilike(column, f"%{value}%")
        else:
            query = query.eq(column, value)
    
    # Apply date filters if specified
    if date_from:
        query = query.gte('incorporated', date_from.isoformat())
    if date_to:
        query = query.lte('incorporated', date_to.isoformat())

    # Apply sorting if specified
    if sort_by:
        query = query.order(sort_by, { 'ascending': sort_order == 'asc' })

    # If count_only is True, return just the count
    response = query.execute()
    if count_only:
        return {"count": response.count}  # Supabase provides the count in response.count
    return response.data

# Endpoint: Fetch all companies with optional filters and count query
@app.get("/company/all")
def get_all_companies(
    filters: dict = Depends(company_filters), 
    count_only: bool = Query(False),
    sort_by: Optional[str] = Query(None),
    sort_order: Optional[str] = Query("desc")):
    result = query_companies(filters, sort_by=sort_by, sort_order=sort_order, count_only=count_only)
    return {"result": result}

# Endpoint: Fetch companies incorporated today with optional filters and count query
@app.get("/company/today")
def get_companies_today(
    filters: dict = Depends(company_filters), 
    count_only: bool = Query(False), 
    sort_by: Optional[str] = Query(None),
    sort_order: Optional[str] = Query("desc")):
    today = date.today()
    result = query_companies(filters, sort_by=sort_by, sort_order=sort_order, date_from=today, date_to=today, count_only=count_only)
    return {"result": result}

# Endpoint: Fetch companies incorporated this week (Monday - Sunday) with optional filters and count query
# Endpoint: Fetch companies incorporated this week (Monday - Sunday) with optional filters and count query
@app.get("/company/week")
def get_companies_week(
    filters: dict = Depends(company_filters),
    count_only: bool = Query(False),
    sort_by: Optional[str] = Query(None),
    sort_order: Optional[str] = Query("desc")
):
    today = date.today()
    start_of_week = today - timedelta(days=today.weekday())  # Monday
    end_of_week = start_of_week + timedelta(days=6)  # Sunday
    result = query_companies(filters, sort_by=sort_by, sort_order=sort_order, date_from=start_of_week, date_to=end_of_week, count_only=count_only)
    return {"result": result}

# Endpoint: Fetch companies incorporated this month with optional filters and count query
@app.get("/company/month")
def get_companies_month(
    filters: dict = Depends(company_filters), 
    count_only: bool = Query(False),
    sort_by: Optional[str] = Query(None),
    sort_order: Optional[str] = Query("desc")):

    today = date.today()
    start_of_month = today.replace(day=1)
    end_of_month = (start_of_month + timedelta(days=31)).replace(day=1) - timedelta(days=1)
    result = query_companies(filters, sort_by=sort_by, sort_order=sort_order, date_from=start_of_month, date_to=end_of_month, count_only=count_only)
    return {"result": result}

# Endpoint: Fetch companies incorporated this year with optional filters and count query
@app.get("/company/year")
def get_companies_month(
    filters: dict = Depends(company_filters), 
    count_only: bool = Query(False),
    sort_by: Optional[str] = Query(None),
    sort_order: Optional[str] = Query("desc")):

    today = date.today()
    start_of_year = today.replace(month=1, day=1)
    end_of_year = today.replace(month=12, day=31)
    result = query_companies(filters, sort_by=sort_by, sort_order=sort_order, date_from=start_of_year, date_to=end_of_year, count_only=count_only)
    return {"result": result}


# Endpoint: Fetch top 5 addresses by count for a specified time period
@app.get("/company/{parameter}/address_top5")
async def get_top_addresses(parameter: str):
    # Determine the date range based on the parameter
    today = date.today()
    
    if parameter == "today":
        start_date = today
        end_date = today
    elif parameter == "week":
        start_date = today - timedelta(days=today.weekday())  # Monday
        end_date = start_date + timedelta(days=6)  # Sunday
    elif parameter == "month":
        start_date = today.replace(day=1)
        end_date = (start_date + timedelta(days=31)).replace(day=1) - timedelta(days=1)
    elif parameter == "year":
        start_date = today.replace(month=1, day=1)
        end_date = today.replace(month=12, day=31)
    elif parameter == "all":
        start_date = today.replace(month=1, day=1, year=2020)
        end_date = today.replace(month=12, day=31, year=2300)
    else:
        raise HTTPException(status_code=400, detail="Invalid parameter. Choose from 'today', 'week', 'month', or 'year'.")
    
    try:
        # Call the PostgreSQL function to get top addresses
        response = supabase.rpc('get_top_addresses', {'start_date': start_date.isoformat(), 'end_date': end_date.isoformat()}).execute()
        return {"top_addresses": response.data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
# Endpoint: Fetch top 5 directors by count for a specified time period
@app.get("/directors/{parameter}/directors_top5")
async def get_top_directors(parameter: str):
    # Determine the date range based on the parameter
    today = date.today()
    
    if parameter == "today":
        start_date = today
        end_date = today
    elif parameter == "week":
        start_date = today - timedelta(days=today.weekday())  # Monday
        end_date = start_date + timedelta(days=6)  # Sunday
    elif parameter == "month":
        start_date = today.replace(day=1)
        end_date = (start_date + timedelta(days=31)).replace(day=1) - timedelta(days=1)
    elif parameter == "year":
        start_date = today.replace(month=1, day=1)
        end_date = today.replace(month=12, day=31)
    elif parameter == "all":
        start_date = today.replace(month=1, day=1, year=2020)
        end_date = today.replace(month=12, day=31, year=2300)
    else:
        raise HTTPException(status_code=400, detail="Invalid parameter. Choose from 'today', 'week', 'month', or 'year'.")
    
    try:
        # Call the PostgreSQL function to get top addresses
        response = supabase.rpc('get_top_directors', {'start_date': start_date.isoformat(), 'end_date': end_date.isoformat()}).execute()
        return {"top_directors": response.data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    

# Endpoint: Fetch top 5 SIC Codes by count for a specified time period
@app.get("/company/{parameter}/sic_top5")
async def get_top_sic_codes(parameter: str):
    # Determine the date range based on the parameter
    today = date.today()
    
    if parameter == "today":
        start_date = today
        end_date = today
    elif parameter == "week":
        start_date = today - timedelta(days=today.weekday())  # Monday
        end_date = start_date + timedelta(days=6)  # Sunday
    elif parameter == "month":
        start_date = today.replace(day=1)
        end_date = (start_date + timedelta(days=31)).replace(day=1) - timedelta(days=1)
    elif parameter == "year":
        start_date = today.replace(month=1, day=1)
        end_date = today.replace(month=12, day=31)
    elif parameter == "all":
        start_date = today.replace(month=1, day=1, year=2020)
        end_date = today.replace(month=12, day=31, year=2300)
    else:
        raise HTTPException(status_code=400, detail="Invalid parameter. Choose from 'today', 'week', 'month', or 'year'.")
    
    try:
        # Call the PostgreSQL function to get top addresses
        response = supabase.rpc('get_top_sic_codes', {'start_date': start_date.isoformat(), 'end_date': end_date.isoformat()}).execute()
        return {"top_sic_codes": response.data}
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
