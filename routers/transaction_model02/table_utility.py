from pyiceberg.types import *
from datetime import datetime, date
from decimal import Decimal
import pyarrow as pa
from pyiceberg.schema import Schema

transaction_schema = [
        NestedField(1, "pri_id", LongType(),required=True),
        NestedField(2, "store_code__c", StringType()),
        NestedField(3, "Branch_Name__c", StringType()),
        NestedField(4, "customerId", StringType()),
        NestedField(5, "customer_mobile__c", LongType()),
        NestedField(6, "Customer_Name__c", StringType()),
        NestedField(7, "Bill_No__c", StringType()),
        NestedField(8, "Bill_Date__c", DateType()),
        NestedField(9, "Invoice_Amount__c", DoubleType()),
        NestedField(10, "bill_status__c", StringType()),
        NestedField(11, "bill_transaction_no__c", StringType()),
        NestedField(12, "Item_Code__c", LongType()),
        NestedField(13, "Item_Name__c", StringType()),
        NestedField(14, "bill_tax__c", DoubleType()),
        NestedField(15, "bill_grand_total__c", DoubleType()),
        NestedField(16, "CreatedDate", DateType()),
        NestedField(17, "tid", StringType()),
        NestedField(18, "Id", StringType()),
        NestedField(19, "OwnerId", StringType()),
        NestedField(20, "IsDeleted", LongType()),
        NestedField(21, "Name", StringType()),
        NestedField(22, "CreatedById", StringType()),
        NestedField(23, "LastModifiedDate", StringType()),
        NestedField(24, "LastModifiedById", StringType()),
        NestedField(25, "SystemModstamp", StringType()),
        NestedField(26, "LastActivityDate", StringType()),
        NestedField(27, "Contact__c", StringType()),
        NestedField(28, "Customer_Number__c", StringType()),
        NestedField(29, "Customer__c", StringType()),
        NestedField(30, "Invoice_Date__c", StringType()),
        NestedField(31, "Customer_Last_Name__c", StringType()),
        NestedField(32, "item_cgst_perc", DoubleType()),
        NestedField(33, "item_sgst_perc", DoubleType()),
        NestedField(34, "item_sgst", DoubleType()),
        NestedField(35, "item_igst_perc", DoubleType()),
        NestedField(36, "item_cgst", DoubleType()),
        NestedField(37, "item_igst", DoubleType()),
        NestedField(38, "Deptid", StringType()),
        NestedField(39, "billed_at_branch_name", StringType()),
        NestedField(40, "billed_at_company_name", StringType()),
        NestedField(41, "billed_at_address", StringType()),
        NestedField(42, "billed_at_city", StringType()),
        NestedField(43, "billed_at_state", StringType()),
        NestedField(44, "billed_at_phone_no1", StringType()),
        NestedField(45, "billed_at_phone_no2", StringType()),
        NestedField(46, "billed_at_GSTN_no", StringType()),
        NestedField(47, "billed_at_VAT_no", StringType()),
        NestedField(48, "billed_at_state_code", StringType()),
        NestedField(49, "billed_at_PAN_no", StringType()),
        NestedField(50, "delivery_from_branch_name", StringType()),
        NestedField(51, "delivery_from_company_name", StringType()),
        NestedField(52, "delivery_from_address", StringType()),
        NestedField(53, "delivery_from_city", StringType()),
        NestedField(54, "delivery_from_state", StringType()),
        NestedField(55, "delivery_from_phone_no1", StringType()),
        NestedField(56, "delivery_from_phone_no2", StringType()),
        NestedField(57, "delivery_from_GSTN_no", StringType()),
        NestedField(58, "delivery_from_VAT_no", StringType()),
        NestedField(59, "delivery_from_state_code", StringType()),
        NestedField(60, "delivery_from_PAN_no", StringType()),
        NestedField(61, "customer_state_code", StringType()),
        NestedField(62, "Email__c", StringType()),
        NestedField(63, "IMEINumber__c", StringType()),
        NestedField(64, "Item_Brand_Name__c", StringType()),
        NestedField(65, "Item_Group_Name__c", StringType()),
        NestedField(66, "Item_Rate__c", StringType()),
        NestedField(67, "Item_Remarks__c", StringType()),
        NestedField(68, "Location__c", StringType()),
        NestedField(69, "Product__c", StringType()),
        NestedField(70, "Products__c", StringType()),
        NestedField(71, "PurchasedDate__c", StringType()),
        NestedField(72, "Service_Center__c", StringType()),
        NestedField(73, "Showroom__c", StringType()),
        NestedField(74, "Showroom_code__c", StringType()),
        NestedField(75, "Status__c", StringType()),
        NestedField(76, "bill_cancel_against__c", StringType()),
        NestedField(77, "bill_cancel_amount__c", DoubleType()),
        NestedField(78, "bill_cancel_date__c", StringType()),
        NestedField(79, "bill_cancel_reason__c", StringType()),
        NestedField(80, "bill_cancel_time__c", StringType()),
        NestedField(81, "bill_discount__c", DoubleType()),
        NestedField(82, "bill_discount_per__c", StringType()),
        NestedField(83, "bill_gross_amount__c", DoubleType()),
        NestedField(84, "bill_modify__c", StringType()),
        NestedField(85, "bill_modify_datetime__c", StringType()),
        NestedField(86, "bill_modify_reason__c", StringType()),
        NestedField(87, "bill_net_amount__c", DoubleType()),
        NestedField(88, "bill_remarks1__c", StringType()),
        NestedField(89, "bill_remarks3__c", StringType()),
        NestedField(90, "bill_remarks4__c", StringType()),
        NestedField(91, "bill_remarks5__c", StringType()),
        NestedField(92, "bill_round_off_amount__c", StringType()),
        NestedField(93, "bill_service_tax__c", StringType()),
        NestedField(94, "bill_tender_type__c", StringType()),
        NestedField(95, "bill_time__c", StringType()),
        NestedField(96, "bill_transaction_type__c", StringType()),
        NestedField(97, "bill_type__c", StringType()),
        NestedField(98, "customer_address__c", StringType()),
        NestedField(99, "customer_area__c", StringType()),
        NestedField(100, "customer_city__c", StringType()),
        NestedField(101, "customer_doa__c", StringType()),
        NestedField(102, "customer_dob__c", StringType()),
        NestedField(103, "customer_email__c", StringType()),
        NestedField(104, "customer_fname__c", StringType()),
        NestedField(105, "customer_gender__c", StringType()),
        NestedField(106, "customer_lname__c", StringType()),
        NestedField(107, "customer_remarks1__c", StringType()),
        NestedField(108, "customer_remarks2__c", StringType()),
        NestedField(109, "customer_remarks3__c", StringType()),
        NestedField(110, "customer_remarks4__c", StringType()),
        NestedField(111, "customer_remarks5__c", StringType()),
        NestedField(112, "customer_state__c", StringType()),
        NestedField(113, "ext_param1__c", StringType()),
        NestedField(114, "ext_param2__c", StringType()),
        NestedField(115, "ext_param3__c", StringType()),
        NestedField(116, "ext_param4__c", StringType()),
        NestedField(117, "ext_param5__c", StringType()),
        NestedField(118, "item_barcode__c", StringType()),
        NestedField(119, "item_brand_code__c", StringType()),
        NestedField(120, "item_category_code__c", StringType()),
        NestedField(121, "item_category_name__c", StringType()),
        NestedField(122, "item_color_code__c", StringType()),
        NestedField(123, "item_color_name__c", StringType()),
        NestedField(124, "item_department_code__c", StringType()),
        NestedField(125, "item_department_name__c", StringType()),
        NestedField(126, "item_discount__c", DoubleType()),
        NestedField(127, "item_discount_per__c", DoubleType()),
        NestedField(128, "item_gross_amount__c", DoubleType()),
        NestedField(129, "item_group__c", StringType()),
        NestedField(130, "item_net_amount__c", DoubleType()),
        NestedField(131, "item_quantity__c", StringType()),
        NestedField(132, "item_remarks2__c", StringType()),
        NestedField(133, "item_remarks3__c", StringType()),
        NestedField(134, "item_remarks4__c", StringType()),
        NestedField(135, "item_remarks5__c", StringType()),
        NestedField(136, "item_serial_no__c", StringType()),
        NestedField(137, "item_service_tax__c", StringType()),
        NestedField(138, "item_size_code__c", StringType()),
        NestedField(139, "item_size_name__c", StringType()),
        NestedField(140, "item_status__c", StringType()),
        NestedField(141, "item_sub_category_code__c", StringType()),
        NestedField(142, "item_sub_category_name__c", StringType()),
        NestedField(143, "item_tax__c", StringType()),
        NestedField(144, "voucher_code__c", StringType()),
        NestedField(145, "voucher_type__c", StringType()),
        NestedField(146, "voucher_value__c", StringType()),
        NestedField(147, "Docid__c", StringType()),
        NestedField(148, "Customer_Address_New__c", StringType()),
        NestedField(149, "customer_mobile__c_backup", StringType()),
        NestedField(150, "billed_at_addressline2", StringType()),
        NestedField(151, "billed_at_addressline3", StringType()),
        NestedField(152, "billed_at_pincode", StringType()),
        NestedField(153, "delivery_from_branch_store_code", StringType()),
        NestedField(154, "delivery_from_addressline2", StringType()),
        NestedField(155, "delivery_from_addressline3", StringType()),
        NestedField(156, "bill_refference_no", StringType()),
        NestedField(157, "bill_refference_date", StringType()),
        NestedField(158, "bill_total_trade_deduction", DoubleType()),
        NestedField(159, "bill_total_trade_addition", DoubleType()),
        NestedField(160, "customer_addressline2", StringType()),
        NestedField(161, "customer_addressline3", StringType()),
        NestedField(162, "customer_pincode", StringType()),
        NestedField(163, "customer_GSTN_no", StringType()),
        NestedField(164, "customer_PAN_no", StringType()),
        NestedField(165, "customer_state", StringType()),
        NestedField(166, "delivery_to_Name", StringType()),
        NestedField(167, "delivery_to_addressline1", StringType()),
        NestedField(168, "delivery_to_addressline2", StringType()),
        NestedField(169, "delivery_to_addressline3", StringType()),
        NestedField(170, "delivery_to_city", StringType()),
        NestedField(171, "delivery_to_state", StringType()),
        NestedField(172, "delivery_to_pincode", StringType()),
        NestedField(173, "delivery_to_phone_no1", StringType()),
        NestedField(174, "delivery_to_phone_no2", StringType()),
        NestedField(175, "delivery_to_GSTN_no", StringType()),
        NestedField(176, "delivery_to_state_code", StringType()),
        NestedField(177, "delivery_to_PAN_no", StringType()),
        NestedField(178, "item_sno", StringType()),
        NestedField(179, "item_name", StringType()),
        NestedField(180, "item_gross_rate", DoubleType()),
        NestedField(181, "item_taxable_Amount", DoubleType()),
        NestedField(182, "item_brand_name", StringType()),
        NestedField(183, "item_product_name", StringType()),
        NestedField(184, "customer_phone_no2", StringType()),
        NestedField(185, "updated_At", DateType()),
        NestedField(186, "cusId_error", StringType()),
        NestedField(187, "customer_mobile__c2", StringType()),
        NestedField(188, "Customer_Code_New", StringType()),
        NestedField(189, "bill_modify_date", StringType()),
        NestedField(190, "bill_modify_time", StringType()),
        NestedField(191, "Customer_Code__c_new", StringType()),
        NestedField(192, "Customer_Code__c", StringType()),
        NestedField(193, "bill_remarks2", StringType()),
        NestedField(194, "bill_remarks2__c", StringType()),
        NestedField(195, "item_remarks1__c", StringType()),
        NestedField(196, "Bill_Grant_Total__c", DoubleType()),
        NestedField(197, "ageOfDevice", StringType()),
        NestedField(198, "emp_id", StringType()),
        NestedField(199, "emp_name", StringType()),
        NestedField(200, "created_At", DateType()),
        NestedField(201, "bill_datetime", DateType()),
    ]





STRING_FIELDS = set()

def transaction_clean_row(rows):
    # cleaned_rows = []
    
    # 1. Decimal Fields (DoubleType in schema)
    decimal_fields = [
        "Invoice_Amount__c", "bill_tax__c", "bill_grand_total__c", 
        "item_cgst_perc", "item_sgst_perc", "item_sgst", "item_igst_perc", 
        "item_cgst", "item_igst", "bill_cancel_amount__c", "bill_discount__c", 
        "bill_gross_amount__c", "bill_net_amount__c", "item_discount__c", 
        "item_discount_per__c", "item_gross_amount__c", "item_net_amount__c", 
        "bill_total_trade_deduction", "bill_total_trade_addition", 
        "item_gross_rate", "item_taxable_Amount", "Bill_Grant_Total__c"
    ]

    # 2. Integer Fields (LongType/IntegerType in schema)
    integer_fields = [
        "pri_id", "customer_mobile__c", "Item_Code__c", "IsDeleted"
    ]

    # 3. Timestamp Fields (DateType in schema)
    timestamp_fields = [
        "Bill_Date__c", "CreatedDate", "updated_At", "created_At", "bill_datetime"
    ]

    # 4. String Fields (StringType in schema)
    string_fields = [
        "store_code__c", "Branch_Name__c", "customerId", "Customer_Name__c", 
        "Bill_No__c", "bill_status__c", "bill_transaction_no__c", "Item_Name__c", 
        "tid", "Id", "OwnerId", "Name", "CreatedById", "LastModifiedDate", 
        "LastModifiedById", "SystemModstamp", "LastActivityDate", "Contact__c", 
        "Customer_Number__c", "Customer__c", "Invoice_Date__c", "Customer_Last_Name__c", 
        "Deptid", "billed_at_branch_name", "billed_at_company_name", "billed_at_address", 
        "billed_at_city", "billed_at_state", "billed_at_phone_no1", "billed_at_phone_no2", 
        "billed_at_GSTN_no", "billed_at_VAT_no", "billed_at_state_code", "billed_at_PAN_no", 
        "delivery_from_branch_name", "delivery_from_company_name", "delivery_from_address", 
        "delivery_from_city", "delivery_from_state", "delivery_from_phone_no1", 
        "delivery_from_phone_no2", "delivery_from_GSTN_no", "delivery_from_VAT_no", 
        "delivery_from_state_code", "delivery_from_PAN_no", "customer_state_code", 
        "Email__c", "IMEINumber__c", "Item_Brand_Name__c", "Item_Group_Name__c", 
        "Item_Rate__c", "Item_Remarks__c", "Location__c", "Product__c", "Products__c", 
        "PurchasedDate__c", "Service_Center__c", "Showroom__c", "Showroom_code__c", 
        "Status__c", "bill_cancel_against__c", "bill_cancel_reason__c", "bill_cancel_date__c",
        "bill_cancel_time__c", "bill_discount_per__c", "bill_modify__c", 
        "bill_modify_datetime__c", "bill_modify_reason__c", "bill_remarks1__c", 
        "bill_remarks3__c", "bill_remarks4__c", "bill_remarks5__c", 
        "bill_round_off_amount__c", "bill_service_tax__c", "bill_tender_type__c", 
        "bill_time__c", "bill_transaction_type__c", "bill_type__c", "customer_address__c", 
        "customer_area__c", "customer_city__c", "customer_doa__c", "customer_dob__c", 
        "customer_email__c", "customer_fname__c", "customer_gender__c", "customer_lname__c", 
        "customer_remarks1__c", "customer_remarks2__c", "customer_remarks3__c", 
        "customer_remarks4__c", "customer_remarks5__c", "customer_state__c", 
        "ext_param1__c", "ext_param2__c", "ext_param3__c", "ext_param4__c", 
        "ext_param5__c", "item_barcode__c", "item_brand_code__c", "item_category_code__c", 
        "item_category_name__c", "item_color_code__c", "item_color_name__c", 
        "item_department_code__c", "item_department_name__c", "item_group__c", 
        "item_quantity__c", "item_remarks2__c", "item_remarks3__c", "item_remarks4__c", 
        "item_remarks5__c", "item_serial_no__c", "item_service_tax__c", "item_size_code__c", 
        "item_size_name__c", "item_status__c", "item_sub_category_code__c", 
        "item_sub_category_name__c", "item_tax__c", "voucher_code__c", "voucher_type__c", 
        "voucher_value__c", "Docid__c", "Customer_Address_New__c", "customer_mobile__c_backup", 
        "billed_at_addressline2", "billed_at_addressline3", "billed_at_pincode", 
        "delivery_from_branch_store_code", "delivery_from_addressline2", 
        "delivery_from_addressline3", "bill_refference_no", "bill_refference_date", 
        "customer_addressline2", "customer_addressline3", "customer_pincode", 
        "customer_GSTN_no", "customer_PAN_no", "customer_state", "delivery_to_Name", 
        "delivery_to_addressline1", "delivery_to_addressline2", "delivery_to_addressline3", 
        "delivery_to_city", "delivery_to_state", "delivery_to_pincode", "delivery_to_phone_no1", 
        "delivery_to_phone_no2", "delivery_to_GSTN_no", "delivery_to_state_code", 
        "delivery_to_PAN_no", "item_sno", "item_name", "item_brand_name", "item_product_name", 
        "customer_phone_no2", "cusId_error", "customer_mobile__c2", "Customer_Code_New", 
        "bill_modify_date", "bill_modify_time", "Customer_Code__c_new", "Customer_Code__c", 
        "bill_remarks2", "bill_remarks2__c", "item_remarks1__c", "ageOfDevice", 
        "emp_id", "emp_name"
    ]

    for row in rows:

        # 1. Decimal Fields (DoubleType)
        for f in decimal_fields:
            val = row.get(f)
            if val is None or val == "":
                row[f] = None
            else:
                try:
                    row[f] = float(str(val))
                except:
                    row[f] = 0.0

        # 2. Integer Fields
        for f in integer_fields:
            val = row.get(f)
            if isinstance(val, str):
                try:
                    row[f] = int(val)
                except ValueError:
                    row[f] = 0
            elif val is None:
                row[f] = 0

        # 3. String Fields
        for f in string_fields:
            val = row.get(f)
            if val is None:
               row[f] = ""
            else:
                row[f] = str(val)

        # 4. Timestamp Fields
        for f in timestamp_fields:
            val = row.get(f)

            if val is None or val == "":
                    row[f] = None
                    continue

            if isinstance(val, datetime):
                continue

            parsed = None
            formats = [
                    "%Y-%m-%d %H:%M:%S",
                    "%Y-%m-%dT%H:%M:%S",
                    "%d/%m/%Y %H:%M:%S",
                    "%Y-%m-%d",
            ]

            for fm in formats:
                    try:
                            parsed = datetime.strptime(val, fm)
                            break
                    except:
                            pass

            row[f] = parsed if parsed else None

    return rows




FIELD_TYPE_MAP = {
    # REQUIRED
    "pri_id": (LongType(), pa.int64(), True),
    "Item_Code__c": (LongType(), pa.int64(), False),
    "IsDeleted": (LongType(), pa.int64(), False),

    # FAST SEARCH (CRITICAL)
    "customer_mobile__c": (LongType(), pa.int64(), False),

    # DATE FIELDS
    "Bill_Date__c": (DateType(), pa.date32(), False),
    "CreatedDate": (DateType(), pa.date32(), False),
    "updated_At": (DateType(), pa.date32(), False),
    "created_At": (DateType(), pa.date32(), False),
    "bill_datetime": (DateType(), pa.date32(), False),

    # DOUBLE FIELDS
    "Invoice_Amount__c": (DoubleType(), pa.float64(), False),
    "bill_tax__c": (DoubleType(), pa.float64(), False),
    "bill_grand_total__c": (DoubleType(), pa.float64(), False),
    "Bill_Grant_Total__c": (DoubleType(), pa.float64(), False),
    "item_gross_rate": (DoubleType(), pa.float64(), False),
    "item_taxable_Amount": (DoubleType(), pa.float64(), False),
    "bill_total_trade_deduction": (DoubleType(), pa.float64(), False),
    "bill_net_amount__c": (DoubleType(), pa.float64(), False),
    "bill_gross_amount__c": (DoubleType(), pa.float64(), False),
    "item_discount__c": (DoubleType(), pa.float64(), False),
    "bill_total_trade_addition": (DoubleType(), pa.float64(), False),
    "item_discount_per__c": (DoubleType(), pa.float64(), False),
    "item_gross_amount__c": (DoubleType(), pa.float64(), False),
    "item_cgst_perc": (DoubleType(), pa.float64(), False),
    "item_sgst_perc": (DoubleType(), pa.float64(), False),
    "item_igst_perc": (DoubleType(), pa.float64(), False),
    "item_cgst": (DoubleType(), pa.float64(), False),
    "item_igst": (DoubleType(), pa.float64(), False),
    "item_net_amount__c": (DoubleType(), pa.float64(), False),
    "item_sgst": (DoubleType(), pa.float64(), False),
    "bill_cancel_amount__c": (DoubleType(), pa.float64(), False),
    "bill_discount__c": (DoubleType(), pa.float64(), False),

}

def generate_field_id(name: str) -> int:
    """
    Stable, deterministic field_id based on field name
    """
    return abs(hash(name)) % 1_000_000 + 1

def infer_schema_from_record(record: dict):
    iceberg_fields = []
    arrow_fields = []

    for name in record.keys():

        # Explicit mapping
        if name in FIELD_TYPE_MAP:
            ice_type, arrow_type, required = FIELD_TYPE_MAP[name]
        elif name == "customer_mobile__c":
            ice_type, arrow_type, required = LongType(), pa.int64(), False

        else:
            # Default (SAFE)
            ice_type = StringType()
            arrow_type = pa.string()
            required = False

        field_id = generate_field_id(name)

        iceberg_fields.append(
            NestedField(
                field_id=field_id,
                name=name,
                field_type=ice_type,
                required=required
            )
        )

        arrow_fields.append(
            pa.field(name, arrow_type, nullable=not required)
        )

    return Schema(*iceberg_fields), pa.schema(arrow_fields)

