# transactions_api.py
from fastapi import FastAPI, APIRouter, Header, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
import os
import datetime
import requests
import re

from dateutil.relativedelta import relativedelta

# ---------------------------
# Config (use env vars)
# ---------------------------


# create engine (sync)


app = FastAPI(title="Transactions API (converted from Go)")
router = APIRouter()

# ---------------------------
# Pydantic models (payloads & responses)
# ---------------------------
class PostTransactionData(BaseModel):
    Branchcode: List[str] = Field(default_factory=list)
    Appliancesbranchcode: List[str] = Field(default_factory=list)
    Category: List[str] = Field(default_factory=list)
    StartDate: Optional[str] = ""
    EndDate: Optional[str] = ""

class Message(BaseModel):
    message: str

# Minimal representations to hold query results
class TransactionRow(BaseModel):
    item_product_name: Optional[str]
    branch_code: Optional[str]
    branch_name: Optional[str]
    total_transactions: Optional[int] = 0
    invoice_gross_amount: Optional[float] = 0.0
    invoice_tax_amount: Optional[float] = 0.0
    invoice_total_with_tax: Optional[float] = 0.0
    mobile_count: Optional[int] = 0
    invoice_count: Optional[int] = 0

# Accessories API response format (adjust to the real API)
class AccessoriesData(BaseModel):
    Name: str
    Category: str

class AccessoriesSalesAPIResponse(BaseModel):
    Data: Dict[str, List[AccessoriesData]] = {}

# Response wrapper helper
def my_response(data: Any, status: int = 1):
    return {"status": status, "data": data}

# ---------------------------
# Utility functions
# ---------------------------
def is_valid_mobile_number(number: str) -> bool:
    """Simple 10-digit validation (adjust as needed)."""
    pattern = r'^[0-9]{10}$'
    return bool(re.match(pattern, number or ""))

def get_prev_month_range(date_str: str) -> str:
    """Return date string one month before date_str (format YYYY-MM-DD)."""
    dt = datetime.datetime.strptime(date_str, "%Y-%m-%d").date()
    prev = dt - relativedelta(months=1)
    return prev.strftime("%Y-%m-%d")

def get_prev_year_range(date_str: str) -> str:
    dt = datetime.datetime.strptime(date_str, "%Y-%m-%d").date()
    prev = dt - relativedelta(years=1)
    return prev.strftime("%Y-%m-%d")

def fetch_category_from_sales_api() -> AccessoriesSalesAPIResponse:
    url = SALES_API_BASE.rstrip("/") + "/accessories-grp"
    payload = {"StoreType": ["General_Store", "Appliance_Store"]}
    resp = requests.post(url, json=payload, timeout=20)
    resp.raise_for_status()
    body = resp.json()
    # Try to normalize to our Pydantic model shape. Caller should adjust as needed.
    return AccessoriesSalesAPIResponse.parse_obj(body)

# Helper to expand IN params using SQLAlchemy 'expanding' bindparam
def make_in_bindparam(name: str, value_list: List[Any]):
    return bindparam(name, value=value_list, expanding=True)

# ---------------------------
# Endpoint: /storesalescount/category  (TransactionSalesCountAPINew)
# ---------------------------
@router.post("/storesalescount/category")
def transaction_sales_count_api_new(request: Request, access_token: Optional[str] = Header(None, alias="access-token")):
    # token check



    try:
        post = PostTransactionData(**(request.json() if False else request._body))
    except Exception:
        # fallback: read raw body
        post = PostTransactionData(**(request.json() if False else {}))  # empty
    # Above is awkward because of sync vs async — use a simpler approach below:
    try:
        # restful frameworks use await request.json(); inside sync handler we'll use request._body
        # But to keep it simple and reliable for typical FastAPI usage, declare endpoint async.
        pass
    except Exception:
        pass

# Note: above sync approach can be brittle. Recreate endpoints below using async functions (recommended).

# ---------- Rewritten endpoints as async for reliable request.json() ----------
@router.post("/storesalescount/category")
async def transaction_sales_count_api_new_async(request: Request, access_token: Optional[str] = Header(None, alias="access-token")):


    body = await request.json()
    post = PostTransactionData.parse_obj(body)

    if not post.Branchcode:
        return JSONResponse(status_code=400, content=my_response({"message": "No branch codes found"}, 0))

    # base query pieces (close to your original queries)
    base_query = """
    SELECT t.item_product_name,
           t.store_code__c AS branch_code,
           t.billed_at_branch_name AS branch_name,
           COUNT(*) AS total_transactions,
           SUM(t.item_gross_amount__c) AS invoice_gross_amount,
           SUM(t.item_tax__c) AS invoice_tax_amount,
           SUM(t.item_gross_amount__c + t.item_tax__c) AS invoice_total_with_tax
    FROM Transaction t
    WHERE (t.bill_transaction_type__c NOT LIKE :d1 AND t.bill_transaction_type__c NOT LIKE :d2)
      AND (t.item_product_name NOT LIKE :n1 AND t.item_product_name NOT LIKE :n2 AND t.item_product_name NOT LIKE :n3
           AND t.item_product_name NOT LIKE :n4 AND t.item_product_name NOT LIKE :n5)
    """

    not_likes = {
        "d1": "delivery%",
        "d2": "sales%",
        "n1": "S-%",
        "n2": "m-free%",
        "n3": "m-own%",
        "n4": "l-bags%",
        "n5": "live%",
    }

    # filters: branch codes
    base_query += " AND t.store_code__c IN :branch_codes"

    # category filter
    if post.Category:
        base_query += " AND t.item_product_name IN :categories"

    # start/end date filters and previous ranges
    args_current = dict(**not_likes, branch_codes=post.Branchcode)
    args_prev_month = dict(**not_likes, branch_codes=post.Branchcode)
    args_prev_year = dict(**not_likes, branch_codes=post.Branchcode)

    if post.StartDate:
        base_query += " AND t.Bill_Date__c >= :start_date"
        args_current["start_date"] = post.StartDate
        args_prev_month["start_date"] = get_prev_month_range(post.StartDate)
        args_prev_year["start_date"] = get_prev_year_range(post.StartDate)

    if post.EndDate:
        base_query += " AND t.Bill_Date__c <= :end_date"
        args_current["end_date"] = post.EndDate
        args_prev_month["end_date"] = get_prev_month_range(post.EndDate)
        args_prev_year["end_date"] = get_prev_year_range(post.EndDate)

    # group by
    base_query += " GROUP BY t.store_code__c, t.item_product_name"

    # Build three queries: current, prev_month, prev_year by replacing date params
    q_current = text(base_query).bindparams(make_in_bindparam("branch_codes", post.Branchcode),
                                           **{k: v for k, v in not_likes.items()})
    if post.Category:
        q_current = q_current.bindparams(make_in_bindparam("categories", post.Category))

    q_prev_month = text(base_query).bindparams(make_in_bindparam("branch_codes", post.Branchcode),
                                               **{k: v for k, v in not_likes.items()})
    if post.Category:
        q_prev_month = q_prev_month.bindparams(make_in_bindparam("categories", post.Category))

    q_prev_year = text(base_query).bindparams(make_in_bindparam("branch_codes", post.Branchcode),
                                              **{k: v for k, v in not_likes.items()})
    if post.Category:
        q_prev_year = q_prev_year.bindparams(make_in_bindparam("categories", post.Category))

    # run the three queries
    with engine.connect() as conn:
        rows_current = conn.execute(q_current, args_current).fetchall()
        rows_prev_month = conn.execute(q_prev_month, args_prev_month).fetchall()
        rows_prev_year = conn.execute(q_prev_year, args_prev_year).fetchall()

        # categories list (distinct item_product_name for provided branches)
        q_categories = text("SELECT DISTINCT t.item_product_name as item_product_name FROM Transaction t WHERE t.store_code__c IN :branch_codes")\
            .bindparams(make_in_bindparam("branch_codes", post.Branchcode))
        categories_rows = conn.execute(q_categories, {"branch_codes": post.Branchcode}).fetchall()

    # convert rows to dicts/lists
    def row_to_dict(r):
        return {k: r[k] for k in r.keys()}

    current_list = [row_to_dict(r) for r in rows_current]
    prev_month_list = [row_to_dict(r) for r in rows_prev_month]
    prev_year_list = [row_to_dict(r) for r in rows_prev_year]
    categories_list = [r["item_product_name"] for r in categories_rows]

    response_for_all = {
        "CurrentFilter": current_list,
        "PreviousMonthFilter": prev_month_list,
        "PreviousYearFilter": prev_year_list,
        "Categories": categories_list,
    }
    return JSONResponse(content=my_response(response_for_all))

# ---------------------------
# Endpoint: /storesalescount  (TransactionSalesCountAPI)
# ---------------------------
@router.post("/storesalescount")
async def transaction_sales_count_api(request: Request, access_token: Optional[str] = Header(None, alias="access-token")):


    body = await request.json()
    post = PostTransactionData.parse_obj(body)

    if not post.Branchcode:
        return JSONResponse(status_code=400, content=my_response({"message": "No branch codes found"}, 0))

    base_query = """
    SELECT t.store_code__c AS branch_code,
           t.billed_at_branch_name AS branch_name,
           COUNT(*) AS total_transactions,
           COUNT(DISTINCT t.customer_mobile__c) AS mobile_count,
           COUNT(DISTINCT t.bill_transaction_no__c) AS invoice_count,
           SUM(t.item_gross_amount__c) AS invoice_gross_amount,
           SUM(t.item_tax__c) AS invoice_tax_amount,
           SUM(t.item_gross_amount__c + t.item_tax__c) AS invoice_total_with_tax
    FROM Transaction t
    WHERE (t.bill_transaction_type__c NOT LIKE :d1 AND t.bill_transaction_type__c NOT LIKE :d2)
      AND (t.item_product_name NOT LIKE :n1 AND t.item_product_name NOT LIKE :n2 AND t.item_product_name NOT LIKE :n3
           AND t.item_product_name NOT LIKE :n4 AND t.item_product_name NOT LIKE :n5)
      AND t.store_code__c IN :branch_codes
    """

    params = {
        "d1": "delivery%",
        "d2": "sales%",
        "n1": "S-%",
        "n2": "m-free%",
        "n3": "m-own%",
        "n4": "l-bags%",
        "n5": "live%",
        "branch_codes": post.Branchcode,
    }

    if post.StartDate:
        base_query += " AND t.Bill_Date__c >= :start_date"
        params["start_date"] = post.StartDate

    if post.EndDate:
        base_query += " AND t.Bill_Date__c <= :end_date"
        params["end_date"] = post.EndDate

    base_query += " GROUP BY t.store_code__c"

    q = text(base_query).bindparams(make_in_bindparam("branch_codes", post.Branchcode))
    with engine.connect() as conn:
        rows = conn.execute(q, params).fetchall()

    result = [{k: r[k] for k in r.keys()} for r in rows]
    return JSONResponse(content=my_response(result))

# ---------------------------

# ---------------------------
@router.post("/accessoriesreport")
async def transaction_accessories_api(request: Request, access_token: Optional[str] = Header(None, alias="access-token")):


    body = await request.json()
    post = PostTransactionData.parse_obj(body)

    # fetch categories from sales API
    try:
        sale_category_data = fetch_category_from_sales_api()
    except Exception as e:
        return JSONResponse(status_code=500, content=my_response({"message": f"sales API error: {e}"}, 0))

    # define store categories CSV from some helper — replace with your constants



    # pick categories from API that match our CSV
    general_store_slice = []
    general_accessories = []
    for item in sale_category_data.Data.get("GeneralStore", []):
        # item is an object/dict. adapt parsing accordingly
        try:
            name = item["Name"] if isinstance(item, dict) else item.Name
            cat = item["Category"] if isinstance(item, dict) else item.Category
        except Exception:
            continue
        if name in general_store_categories:
            general_store_slice.append(name)
            general_accessories.append({"Name": name, "Category": cat})

    appliance_store_slice = []
    appliance_accessories = []
    for item in sale_category_data.Data.get("ApplianceStore", []):
        try:
            name = item["Name"] if isinstance(item, dict) else item.Name
            cat = item["Category"] if isinstance(item, dict) else item.Category
        except Exception:
            continue
        if name in appliance_store_categories:
            appliance_store_slice.append(name)
            appliance_accessories.append({"Name": name, "Category": cat})

    # Run transaction query for general and appliance stores
    general_response = await transaction_query_response(general_store_slice, post, general_accessories, post.Branchcode)
    appliance_response = await transaction_query_response(appliance_store_slice, post, appliance_accessories, post.Appliancesbranchcode)

    response = {
        "GeneralStore": general_response,
        "ApplianceStore": appliance_response,
    }
    return JSONResponse(content=my_response(response))

# ---------------------------
# TransactionQueryResponse (replicates Go logic)
# ---------------------------
async def transaction_query_response(general_store_slice: List[str], post: PostTransactionData, accessories_data: List[Dict[str,str]], branch_codes: List[str]):
    """
    Returns list of branch detail dicts similar to your Go BranchDetails.
    """
    if not branch_codes:
        return []

    select_sql = """
    SELECT
      t.item_product_name as item_product_name,
      t.store_code__c as branch_code,
      t.billed_at_branch_name as branch_name,
      COUNT(*) AS total_transactions,
      SUM(t.item_gross_amount__c) AS invoice_gross_amount,
      SUM(t.item_tax__c) AS invoice_tax_amount,
      SUM(t.item_gross_amount__c + t.item_tax__c) AS invoice_total_with_tax
    FROM Transaction t
    WHERE t.store_code__c IN :branch_codes
    """

    params = {"branch_codes": branch_codes}
    if general_store_slice:
        select_sql += " AND t.item_product_name IN :categories"
        params["categories"] = general_store_slice

    if post.StartDate:
        select_sql += " AND t.updated_At >= :start_date"
        params["start_date"] = post.StartDate
    if post.EndDate:
        select_sql += " AND t.updated_At <= :end_date"
        params["end_date"] = post.EndDate

    select_sql += " GROUP BY t.store_code__c, t.item_product_name"

    q = text(select_sql).bindparams(make_in_bindparam("branch_codes", branch_codes))
    if general_store_slice:
        q = q.bindparams(make_in_bindparam("categories", general_store_slice))

    with engine.connect() as conn:
        rows = conn.execute(q, params).fetchall()

    # build branch map
    branch_map: Dict[str, Dict] = {}
    # helper to find accessory category by product
    def check_category_func(accessories, product_name):
        for a in accessories:
            name = a.get("Name") if isinstance(a, dict) else getattr(a, "Name", None)
            if name == product_name:
                return a.get("Category") if isinstance(a, dict) else getattr(a, "Category", "")
        return ""

    for r in rows:
        d = {k: r[k] for k in r.keys()}
        item_prod = d.get("item_product_name")
        branch_code = d.get("branch_code")
        branch_name = d.get("branch_name")
        category_data = check_category_func(accessories_data, item_prod)
        category_key = (category_data or "").lower().replace("-", "_") or "unknown"

        if branch_code not in branch_map:
            branch_map[branch_code] = {
                "BranchCode": branch_code,
                "BranchName": branch_name,
                "Items": {}
            }
        branch = branch_map[branch_code]
        if category_key not in branch["Items"]:
            branch["Items"][category_key] = {
                "Count": 0,
                "TotalGrossAmount": 0.0,
                "List": [],
                "MobileAmount": 0.0
            }

        cat_entry = branch["Items"][category_key]
        cat_entry["Count"] += int(d.get("total_transactions") or 0)
        cat_entry["TotalGrossAmount"] += float(d.get("invoice_total_with_tax") or 0.0)
        cat_entry["List"].append({
            "ItemProductName": item_prod,
            "Category": category_key,
            "TotalTransactions": int(d.get("total_transactions") or 0),
            "InvoiceGrossAmount": float(d.get("invoice_gross_amount") or 0.0),
            "InvoiceTaxAmount": float(d.get("invoice_tax_amount") or 0.0),
            "InvoiceTotalWithTax": float(d.get("invoice_total_with_tax") or 0.0),
        })

        # compute MobileAmount (sum where item_product_name = 'M-mobile' for this branch & date range)
        sub_sql = "SELECT SUM(t.item_gross_amount__c + t.item_tax__c) as total FROM Transaction t WHERE t.store_code__c = :branch_code AND t.item_product_name = 'M-mobile' "
        sub_params = {"branch_code": branch_code}
        if post.StartDate:
            sub_sql += " AND DATE(t.updated_At) >= :start_date"
            sub_params["start_date"] = post.StartDate
        if post.EndDate:
            sub_sql += " AND DATE(t.updated_At) <= :end_date"
            sub_params["end_date"] = post.EndDate
        with engine.connect() as conn:
            mobile_row = conn.execute(text(sub_sql), sub_params).fetchone()
            mobile_total = float(mobile_row[0] or 0.0) if mobile_row is not None else 0.0
        cat_entry["MobileAmount"] = mobile_total

    # convert map to list
    return list(branch_map.values())

# ---------------------------
# Endpoint: /accessories/timeslot  (checkTimeSlotForAccessoriesAPIReport)
# ---------------------------
@router.post("/accessories/timeslot")
async def check_time_slot_for_accessories_api_report(request: Request, access_token: Optional[str] = Header(None, alias="access-token")):

    body = await request.json()
    post = PostTransactionData.parse_obj(body)

    if post.StartDate != post.EndDate:
        return JSONResponse(status_code=400, content=my_response({"message": "Start date and end date must be the same"}, 0))

    try:
        sale_category_data = fetch_category_from_sales_api()
    except Exception as e:
        return JSONResponse(status_code=500, content=my_response({"message": f"sales API error: {e}"}, 0))




    general_store_slice = []
    accessories_data = []
    appliance_accessories_data = []
    for item in sale_category_data.Data.get("GeneralStore", []):
        name = item.get("Name") if isinstance(item, dict) else getattr(item, "Name", None)
        if name in general_store_categories:
            general_store_slice.append(name)
            accessories_data.append({"Name": name, "Category": item.get("Category") if isinstance(item, dict) else getattr(item, "Category", "")})

    for item in sale_category_data.Data.get("ApplianceStore", []):
        name = item.get("Name") if isinstance(item, dict) else getattr(item, "Name", None)
        if name in appliance_store_categories:
            appliance_accessories_data.append({"Name": name, "Category": item.get("Category") if isinstance(item, dict) else getattr(item, "Category", "")})

    general_slots = await get_slots(general_store_slice, post, accessories_data, post.Branchcode)
    appliance_slots = await get_slots(appliance_store_categories, post, appliance_accessories_data, post.Appliancesbranchcode)

    response = {
        "GeneralStore": general_slots,
        "ApplianceStore": appliance_slots,
    }
    return JSONResponse(content=my_response(response))

# ---------------------------
# GetSlots: hourly split logic (replicates Go GetSlots)
# ---------------------------
async def get_slots(general_store_slice: List[str], post: PostTransactionData, accessories_data: List[Dict[str,str]], branch_codes: List[str]):
    if not branch_codes:
        return []

    now = datetime.datetime.now()
    layout = "%Y-%m-%d"
    start_date = datetime.datetime.strptime(post.StartDate, layout).date()

    # start at 10:00, end at 23:59 (or current hour if same day)
    start_time = datetime.datetime.combine(start_date, datetime.time(10, 0))
    end_time = datetime.datetime.combine(start_date, datetime.time(23, 59))

    if start_date == now.date():
        current_hour = now.replace(minute=0, second=0, microsecond=0)
        if current_hour < end_time:
            end_time = current_hour

    branch_map = {}

    def check_category_func(accessories, product_name):
        for a in accessories:
            name = a.get("Name")
            if name == product_name:
                return a.get("Category", "")
        return ""

    t = start_time
    while t <= end_time:
        end_slot = t + datetime.timedelta(hours=1)
        if end_slot > now:
            end_slot = now

        slot_key = f"{t.hour:02d}-{end_slot.hour:02d}"

        # Query aggregated per hour
        select_sql = """
        SELECT
          t.item_product_name as item_product_name,
          t.store_code__c as branch_code,
          t.billed_at_branch_name as branch_name,
          COUNT(*) AS total_transactions,
          SUM(t.item_gross_amount__c) AS invoice_gross_amount,
          SUM(t.item_tax__c) AS invoice_tax_amount,
          SUM(t.item_gross_amount__c + t.item_tax__c) AS invoice_total_with_tax
        FROM Transaction t
        WHERE t.updated_At >= :start_ts AND t.updated_At < :end_ts
          AND t.store_code__c IN :branch_codes
        """

        params = {"start_ts": t.strftime("%Y-%m-%d %H:%M:%S"), "end_ts": end_slot.strftime("%Y-%m-%d %H:%M:%S"),
                  "branch_codes": branch_codes}

        if general_store_slice:
            select_sql += " AND t.item_product_name IN :categories"
            params["categories"] = general_store_slice

        select_sql += " GROUP BY t.store_code__c, t.item_product_name"

        q = text(select_sql).bindparams(make_in_bindparam("branch_codes", branch_codes))
        if general_store_slice:
            q = q.bindparams(make_in_bindparam("categories", general_store_slice))

        with engine.connect() as conn:
            rows = conn.execute(q, params).fetchall()

        for r in rows:
            d = {k: r[k] for k in r.keys()}
            item_prod = d.get("item_product_name")
            branch_code = d.get("branch_code")
            branch_name = d.get("branch_name")
            category_data = check_category_func(accessories_data, item_prod)
            category_key = (category_data or "").lower().replace("-", "_") or "unknown"

            if branch_code not in branch_map:
                branch_map[branch_code] = {
                    "BranchCode": branch_code,
                    "BranchName": branch_name,
                    "Items": {}
                }
            branch = branch_map[branch_code]
            if category_key not in branch["Items"]:
                branch["Items"][category_key] = {"TimeSlots": {}}
            category_entry = branch["Items"][category_key]

            if slot_key not in category_entry["TimeSlots"]:
                category_entry["TimeSlots"][slot_key] = {
                    "Count": 0,
                    "TotalGrossAmount": 0.0,
                    "List": [],
                    "MobileAmount": 0.0
                }

            slot = category_entry["TimeSlots"][slot_key]
            slot["Count"] += int(d.get("total_transactions") or 0)
            slot["TotalGrossAmount"] += float(d.get("invoice_total_with_tax") or 0.0)
            slot["List"].append({
                "ItemProductName": item_prod,
                "Category": category_key,
                "TotalTransactions": int(d.get("total_transactions") or 0),
                "InvoiceGrossAmount": float(d.get("invoice_gross_amount") or 0.0),
                "InvoiceTaxAmount": float(d.get("invoice_tax_amount") or 0.0),
                "InvoiceTotalWithTax": float(d.get("invoice_total_with_tax") or 0.0),
            })

            # compute MobileAmount for slot (approx: same date range as request; original Go uses date filters not hourly)
            sub_sql = "SELECT SUM(t.item_gross_amount__c + t.item_tax__c) as total FROM Transaction t WHERE t.store_code__c = :branch_code AND t.item_product_name = 'M-mobile'"
            sub_params = {"branch_code": branch_code}
            if post.StartDate:
                sub_sql += " AND DATE(t.updated_At) >= :start_date"
                sub_params["start_date"] = post.StartDate
            if post.EndDate:
                sub_sql += " AND DATE(t.updated_At) <= :end_date"
                sub_params["end_date"] = post.EndDate
            with engine.connect() as conn:
                mobile_row = conn.execute(text(sub_sql), sub_params).fetchone()
                slot["MobileAmount"] = float(mobile_row[0] or 0.0) if mobile_row is not None else 0.0

        t += datetime.timedelta(hours=1)

    return list(branch_map.values())

# ---------------------------
# attach router and run
# ---------------------------
app.include_router(router)