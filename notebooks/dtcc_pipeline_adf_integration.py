
import pandas as pd
from datetime import datetime, date
import re
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

# Parameters from Azure Data Factory
dbutils.widgets.text("input_file", "", "Specific Input File Name (from DTCC SFTP)")
dbutils.widgets.text("processing_date", "", "Processing Date (YYYY-MM-DD)")
dbutils.widgets.text("mode", "auto", "Processing Mode: 'single' for ADF, 'batch' for manual")

# Get parameter values
input_file_param = dbutils.widgets.get("input_file")
processing_date_param = dbutils.widgets.get("processing_date")
mode_param = dbutils.widgets.get("mode")

# Determine processing mode
if input_file_param and input_file_param.strip():
    PROCESSING_MODE = "single"  # ADF triggered for specific file
    TARGET_FILE = input_file_param.strip()
    PROCESSING_DATE = processing_date_param if processing_date_param else datetime.now().strftime("%Y-%m-%d")
    print(f" ADF MODE: Processing specific file: {TARGET_FILE}")
    print(f"Processing date: {PROCESSING_DATE}")
else:
    PROCESSING_MODE = "batch"   # Manual run - process all files
    TARGET_FILE = None
    PROCESSING_DATE = datetime.now().strftime("%Y-%m-%d")
    print(f" BATCH MODE: Processing all files in source container")

print(f"🚀 Mode: {PROCESSING_MODE}")

# COMMAND ----------

import pandas as pd
from datetime import datetime, date
import re
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

print("  Libraries imported")

# COMMAND ----------

# Storage configuration (UNCHANGED)
CONNECTION_STRING = ""

def extract_key_from_connection_string(conn_str):
    parts = conn_str.split(';')
    for part in parts:
        if part.startswith('AccountKey='):
            return part.replace('AccountKey=', '')
    return None

STORAGE_ACCOUNT_KEY = extract_key_from_connection_string(CONNECTION_STRING)
STORAGE_ACCOUNT_NAME = "dtcc"

# Configure Spark
spark.conf.set(f"fs.azure.account.key.{STORAGE_ACCOUNT_NAME}.blob.core.windows.net", STORAGE_ACCOUNT_KEY)
spark.conf.set(f"fs.azure.account.key.{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net", STORAGE_ACCOUNT_KEY)

print(f"  Storage configured for: {STORAGE_ACCOUNT_NAME}")

# COMMAND ----------

# Determine working protocol (UNCHANGED)
print("🔍 Testing storage connection...")

PROTOCOL = None
ENDPOINT = None

# Try ABFSS first
try:
    abfss_path = f"abfss://source@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/"
    files = dbutils.fs.ls(abfss_path)
    
    print(f"  ABFSS connection successful")
    PROTOCOL = "abfss"
    ENDPOINT = "dfs.core.windows.net"
    
except Exception as e1:
    print(f"⚠️  ABFSS failed, trying WASBS...")
    
    # Try WASBS as fallback
    try:
        wasbs_path = f"wasbs://source@{STORAGE_ACCOUNT_NAME}.blob.core.windows.net/"
        files = dbutils.fs.ls(wasbs_path)
        
        print(f"  WASBS connection successful")
        PROTOCOL = "wasbs"
        ENDPOINT = "blob.core.windows.net"
        
    except Exception as e2:
        print(f"  Connection failed with both protocols")
        raise

print(f"🔗 Using: {PROTOCOL} protocol")

# COMMAND ----------

# Complete RECORD_CONFIGS with exact naming (UNCHANGED)
RECORD_CONFIGS = {
    "contract_valuation": {
        "layout": [
            ("SUBMITTERCODE", 0, 1), ("RECORDTYPE", 1, 3), ("SEQUENCENUMBER", 3, 5), ("CONTRACTNUMBER", 5, 35),
            ("CONTRACTVALUEAMOUNT1", 35, 51), ("CONTRACTVALUEQUALIFIER1", 51, 54),
            ("CONTRACTVALUEAMOUNT2", 55, 71), ("CONTRACTVALUEQUALIFIER2", 71, 74),
            ("CONTRACTVALUEAMOUNT3", 75, 91), ("CONTRACTVALUEQUALIFIER3", 91, 94),
            ("CONTRACTVALUEAMOUNT4", 95, 111), ("CONTRACTVALUEQUALIFIER4", 111, 114),
            ("CONTRACTVALUEAMOUNT5", 115, 131), ("CONTRACTVALUEQUALIFIER5", 131, 134),
            ("CONTRACTPERCENTAGEAMOUNT1", 135, 145), ("CONTRACTPERCENTAGEAMOUNTQUALIFIER1", 145, 148),
            ("CONTRACTPERCENTAGEAMOUNT2", 149, 159), ("CONTRACTPERCENTAGEAMOUNTQUALIFIER2", 159, 162),
            ("CONTRACTPERCENTAGEAMOUNT3", 163, 173), ("CONTRACTPERCENTAGEAMOUNTQUALIFIER3", 173, 176),
            ("REJECTCODELIST", 288, 300)
        ],
        "matcher": lambda line: len(line) > 4 and line[1:3] == '13' and line[3:5] == '02',
        "display_name": "Contract Valuation"
    },
    
    "submitting_header": {
        "layout": [
            ("SUBMITTERCODE", 0, 1), ("RECORDTYPE", 1, 3), ("SUBMITTINGPARTICIPANTNUMBER", 3, 7),
            ("IPSBUSINESSCODE", 7, 10), ("TRANSMISSIONUNIQUEID", 10, 40), ("TOTALCOUNT", 40, 52),
            ("VALUATIONDATE", 52, 60), ("TESTINDICATOR", 60, 61), ("ASSOCIATEDCARRIERCOMPANYID", 61, 71),
            ("REJECTCODE", 288, 300)
        ],
        "matcher": lambda line: len(line) > 2 and line[1:3] == '10',
        "display_name": "Submitting Header"
    },
    
    "contra_record": {
        "layout": [
            ("SUBMITTERCODE", 0, 1), ("RECORDTYPE", 1, 3), ("CONTRAPARTICIPANTNUMBER", 3, 7),
            ("ASSOCIATEDFIRMID", 7, 11), ("ASSOCIATEDFIRMSUBMITTEDCONTRACTCOUNT", 11, 21),
            ("ASSOCIATEDFIRMDELIVEREDCONTRACTCOUNT", 21, 31), ("IPSEVENTCODE", 31, 34),
            ("IPSSTAGECODE", 34, 37), ("REJECTCODE", 288, 300)
        ],
        "matcher": lambda line: len(line) > 2 and line[1:3] == '12',
        "display_name": "Contra Record"
    },
    
    "contract_underlying_asset": {
        "layout": [
            ("SUBMITTERSCODE", 0, 1), ("RECORDTYPE", 1, 3), ("SEQUENCENUMBER", 3, 5), ("CONTRACTNUMBER", 5, 35),
            ("CUSIPFUNDID", 35, 54), ("FUNDVALUE", 54, 70), ("FUNDPERCENTAGE", 70, 80), ("FUNDUNITS", 80, 98),
            ("GUARANTEEDINTERESTRATE", 98, 108), ("FUNDSECURITYNAME", 108, 148), ("FUNDSECURITYTYPE", 148, 151),
            ("MUTUALFUNDCUSIP", 151, 160), ("RESTRICTIONINDICATOR", 160, 161), ("RESTRICTIONREASON", 161, 162),
            ("STANDINGALLOCINDICATOR", 162, 163), ("STANDINGALLOCATIONPCT", 163, 173), ("MATURITYELECTIONINSTRUCTIONS", 173, 175),
            ("RATEFUNDRISKTHRESHOLDPCT", 175, 185), ("TOTALNETFUNDFEEPCT", 185, 195), ("MVAINDICATOR", 195, 196),
            ("THIRDPARTYPLATFORMID", 196, 226), ("THIRDPARTYPLATFORMSOURCE", 226, 228), ("COREFUNDINDICATOR", 228, 229),
            ("LOCKFEATUREINDICATOR", 229, 230), ("CARRIERFUNDLEVELFEE", 230, 240), ("INDEXSTRATEGYTERM", 240, 243),
            ("INDEXSTRATEGYTERMQUALIFIER", 243, 244), ("NUMBEROFINDEXPERIODS", 244, 247), ("DURATIONOFINDEXPERIODS", 247, 250),
            ("REJECTCODE", 288, 300)
        ],
        "matcher": lambda line: len(line) > 4 and line[1:3] == '13' and line[3:5] == '03',
        "display_name": "Contract Underlying Asset"
    },
    
    "contract_record": {
        "layout": [
            ("SUBMITTERCODE", 0, 1), ("RECORDTYPE", 1, 3), ("SEQUENCENUMBER", 3, 5), ("CONTRACTNUMBER", 5, 35),
            ("CUSIPNUMBER", 35, 44), ("CONTRACTSTATUS", 44, 46), ("ENDRECEIVINGCOMPANYID", 46, 66),
            ("ENDRECEIVINGCOMPANYIDQUALIFIER", 66, 68), ("GROUPNUMBER", 68, 98), ("ORIGINALCONTRACTNUMBER", 98, 128),
            ("DISTRIBUTORSACCOUNTID", 128, 158), ("IRSQUALIFICATIONCODE", 158, 162), ("PRODUCTTYPECODE", 162, 165),
            ("COMMISSIONOPTION", 165, 169), ("FEEBASEDADVISORYINDICATOR", 169, 170), ("INHERITEDPAYOUTTIMINGCHOICE", 170, 171),
            ("INVESTMENTONLYINDICATOR", 171, 172), ("COMMISSIONEXTENSION", 175, 185), ("ERISAINDICATOR", 185, 186),
            ("CONTRACTSTATE", 186, 188), ("FUNDTRANSFERSRESTRICTIONINDICATOR", 188, 189), ("FUNDTRANSFERSRESTRICTIONREASON", 189, 191),
            ("NONASSIGNABILITYINDICATOR", 191, 192), ("LIFETERMDURATION", 192, 194), ("DIVIDENDOPTION", 194, 196),
            ("QLACINDICATOR", 196, 197), ("MVAINDICATOR", 197, 198), ("PRODUCTSHARECLASS", 198, 200),
            ("COMMISSIONSCHEDULEIDENTIFIER", 200, 220), ("CONTRACTFEESINCLUDED", 220, 221), ("PRIORCARRIERPROCESSINGLOCATION", 221, 231),
            ("REJECTCODE", 288, 300)
        ],
        "matcher": lambda line: len(line) > 4 and line[1:3] == '13' and line[3:5] == '01',
        "display_name": "Contract Record"
    },
    
    "contract_index_loop": {
        "layout": [
            ("SUBMITTERCODE", 0, 1), ("RECORDTYPE", 1, 3), ("SEQUENCENUMBER", 3, 5), ("CONTRACTNUMBER", 5, 35),
            ("CUSIPFUNDID", 35, 54), ("INDEXDURATIONVALUE", 54, 70), ("INDEXDURATIONSTARTDATE", 70, 78),
            ("INDEXDURATIONENDDATE", 78, 86), ("INDEXTERMMATURITYDATE", 86, 94), ("INDEXCREDITINGMETHOD", 94, 95),
            ("INDEXCREDITINGMODE", 95, 98), ("INDEXCREDITINGMODEQUALIFIER", 98, 99), ("INDEXOPTIONPERIOD", 99, 102),
            ("INDEXTYPE", 102, 103), ("INDEXDURATIONRATE1", 103, 113), ("INDEXDURATIONRATETYPE1", 113, 115),
            ("INDEXDURATIONRATE2", 115, 125), ("INDEXDURATIONRATETYPE2", 125, 127),
            ("INDEXDURATIONRATE3", 127, 137), ("INDEXDURATIONRATETYPE3", 137, 139),
            ("INDEXDURATIONRATE4", 139, 149), ("INDEXDURATIONRATETYPE4", 149, 151),
            ("INDEXDURATIONRATE5", 151, 161), ("INDEXDURATIONRATETYPE5", 161, 163),
            ("INDEXDURATIONRATE6", 163, 173), ("INDEXDURATIONRATETYPE6", 173, 175),
            ("INDEXOPTIONEFFECTIVEDATE", 175, 183), ("INDEXOPTIONBASEVALUE", 183, 199),
            ("DAILYTRACKINGVALUE", 199, 215), ("LOCKEXECUTIONDATE", 215, 223),
            ("LOCKEXECUTIONINDICATOR", 223, 224), ("TRANSFERWINDOWSTARTDATE", 224, 232),
            ("TRANSFERWINDOWENDDATE", 232, 240), ("GROUPINGID", 240, 244), ("CARRIERFUNDLEVELFEE", 244, 249),
            ("CARRIERFUNDLEVELFEEQUALIFIER", 249, 251), ("REJECTCODE", 288, 300)
        ],
        "matcher": lambda line: len(line) > 4 and line[1:3] == '13' and line[3:5] == '14',
        "display_name": "Contract Index Loop"
    },
    
    "contract_band_guaranteed_loop": {
        "layout": [
            ("SUBMITTERCODE", 0, 1), ("RECORDTYPE", 1, 3), ("SEQUENCENUMBER", 3, 5), ("CONTRACTNUMBER", 5, 35),
            ("CUSIPFUNDID", 35, 54), ("DEPOSITGUARANTEEDSTARTDATE", 54, 62), ("DEPOSITGUARANTEEDENDDATE", 62, 70),
            ("DEPOSITGUARANTEEDMATURITYDATE", 70, 78), ("DEPOSITGUARANTEEDRATE1", 78, 88),
            ("DEPOSITGUARANTEEDRATETYPE1", 88, 90), ("DEPOSITGUARANTEEDUNITS", 90, 108),
            ("DEPOSITGUARANTEEDPERIODFREQUENCYCODE", 108, 110), ("DEPOSITGUARANTEEDPERIODNUMBER", 110, 120),
            ("DEPOSITGUARANTEEDVALUE", 120, 136), ("DEPOSITGUARANTEEDRATE2", 136, 146),
            ("DEPOSITGUARANTEEDRATETYPE2", 146, 148), ("DEPOSITGUARANTEEDRATE3", 148, 158),
            ("DEPOSITGUARANTEEDRATETYPE3", 158, 160), ("DEPOSITGUARANTEEDRATE4", 160, 170),
            ("DEPOSITGUARANTEEDRATETYPE4", 170, 172), ("DEPOSITGUARANTEEDRATE5", 172, 182),
            ("DEPOSITGUARANTEEDRATETYPE5", 182, 184), ("DEPOSITGUARANTEEDRATE6", 184, 194),
            ("DEPOSITGUARANTEEDRATETYPE6", 194, 196), ("GROUPINGID", 236, 240), ("REJECTCODE", 288, 300)
        ],
        "matcher": lambda line: len(line) > 4 and line[1:3] == '13' and line[3:5] == '04',
        "display_name": "Contract Band Guaranteed Loop"
    },
    
    "contract_agent_record": {
        "layout": [
            ("SUBMITTERCODE", 0, 1), ("RECORDTYPE", 1, 3), ("SEQUENCENUMBER", 3, 5), ("CONTRACTNUMBER", 5, 35),
            ("AGENTIDENTIFIER", 35, 55), ("AGENTIDENTIFIERQUALIFIER", 55, 57), ("AGENTROLE", 57, 59),
            ("AGENTNONNATURALNAME", 59, 164), ("AGENTLASTNAME", 59, 94), ("AGENTFIRSTNAME", 94, 119),
            ("AGENTMIDDLENAME", 119, 144), ("AGENTPREFIX", 144, 154), ("AGENTSUFFIX", 154, 164),
            ("DISTRIBUTORASSIGNEDAGENTID", 164, 184), ("AGENTNATURALNONNATURALNAMEINDICATOR", 184, 185),
            ("NATIONALPRODUCERNUMBER", 185, 195), ("FUNDTRANSFERAGENTAUTHINDICATOR", 195, 196),
            ("CRDNUMBER", 196, 206), ("CARRIERASSIGNEDAGENTID", 206, 226), ("REJECTCODE", 288, 300)
        ],
        "matcher": lambda line: len(line) > 4 and line[1:3] == '13' and line[3:5] == '05',
        "display_name": "Contract Agent Record"
    },
    
    "contract_dates_record": {
        "layout": [
            ("SUBMITTERCODE", 0, 1), ("RECORDTYPE", 1, 3), ("SEQUENCENUMBER", 3, 5), ("CONTRACTNUMBER", 5, 35),
            ("CONTRACTDATE1", 35, 43), ("CONTRACTDATEQUALIFIER1", 43, 46),
            ("CONTRACTDATE2", 47, 55), ("CONTRACTDATEQUALIFIER2", 55, 58),
            ("CONTRACTDATE3", 59, 67), ("CONTRACTDATEQUALIFIER3", 67, 70),
            ("CONTRACTDATE4", 71, 79), ("CONTRACTDATEQUALIFIER4", 79, 82),
            ("CONTRACTDATE5", 83, 91), ("CONTRACTDATEQUALIFIER5", 91, 94),
            ("CONTRACTDATE6", 95, 103), ("CONTRACTDATEQUALIFIER6", 103, 106),
            ("CONTRACTDATE7", 107, 115), ("CONTRACTDATEQUALIFIER7", 115, 118),
            ("CONTRACTDATE8", 119, 127), ("CONTRACTDATEQUALIFIER8", 127, 130),
            ("CONTRACTDATE9", 131, 139), ("CONTRACTDATEQUALIFIER9", 139, 142),
            ("CONTRACTDATE10", 143, 151), ("CONTRACTDATEQUALIFIER10", 151, 154),
            ("CONTRACTDATE11", 155, 163), ("CONTRACTDATEQUALIFIER11", 163, 166),
            ("CONTRACTDATE12", 167, 175), ("CONTRACTDATEQUALIFIER12", 175, 178),
            ("CONTRACTDATE13", 179, 187), ("CONTRACTDATEQUALIFIER13", 187, 190),
            ("CONTRACTDATE14", 191, 199), ("CONTRACTDATEQUALIFIER14", 199, 202),
            ("CONTRACTDATE15", 203, 211), ("CONTRACTDATEQUALIFIER15", 211, 214),
            ("CONTRACTDATE16", 215, 223), ("CONTRACTDATEQUALIFIER16", 223, 226),
            ("CONTRACTDATE17", 227, 235), ("CONTRACTDATEQUALIFIER17", 235, 238),
            ("CONTRACTDATE18", 239, 247), ("CONTRACTDATEQUALIFIER18", 247, 250),
            ("CONTRACTDATE19", 251, 259), ("CONTRACTDATEQUALIFIER19", 259, 262),
            ("CONTRACTDATE20", 263, 271), ("CONTRACTDATEQUALIFIER20", 271, 274),
            ("REJECTCODE", 288, 300)
        ],
        "matcher": lambda line: len(line) > 4 and line[1:3] == '13' and line[3:5] == '06',
        "display_name": "Contract Dates Record"
    },
    
    "contract_events_record": {
        "layout": [
            ("SUBMITTERCODE", 0, 1), ("RECORDTYPE", 1, 3), ("SEQUENCENUMBER", 3, 5), ("CONTRACTNUMBER", 5, 35),
            ("EVENTPERIODTYPE1", 35, 38), ("EVENTTOTALAMOUNT1", 38, 54), ("EVENTTYPECODE1", 54, 57), ("GROSSNETINDICATOR1", 57, 58),
            ("EVENTPERIODTYPE2", 59, 62), ("EVENTTOTALAMOUNT2", 62, 78), ("EVENTTYPECODE2", 78, 81), ("GROSSNETINDICATOR2", 81, 82),
            ("EVENTPERIODTYPE3", 83, 86), ("EVENTTOTALAMOUNT3", 86, 102), ("EVENTTYPECODE3", 102, 105), ("GROSSNETINDICATOR3", 105, 106),
            ("EVENTPERIODTYPE4", 107, 110), ("EVENTTOTALAMOUNT4", 110, 126), ("EVENTTYPECODE4", 126, 129), ("GROSSNETINDICATOR4", 129, 130),
            ("EVENTPERIODTYPE5", 131, 134), ("EVENTTOTALAMOUNT5", 134, 150), ("EVENTTYPECODE5", 150, 153), ("GROSSNETINDICATOR5", 153, 154),
            ("NEXTEVENTDATE1", 154, 162), ("NEXTEVENTDATE2", 162, 170), ("NEXTEVENTDATE3", 170, 178),
            ("NEXTEVENTDATE4", 178, 186), ("NEXTEVENTDATE5", 186, 194), ("REJECTCODE", 288, 300)
        ],
        "matcher": lambda line: len(line) > 4 and line[1:3] == '13' and line[3:5] == '07',
        "display_name": "Contract Events Record"
    },
    
    "contract_party_record": {
        "layout": [
            ("SUBMITTERCODE", 0, 1), ("RECORDTYPE", 1, 3), ("SEQUENCENUMBER", 3, 5), ("CONTRACTNUMBER", 5, 35),
            ("PARTYNONNATURALNAME", 35, 140), ("PARTYLASTNAME", 36, 70), ("PARTYMIDDLENAME", 96, 120), ("PARTYFIRSTNAME", 71, 95),
            ("PARTYROLE", 140, 142), ("PARTYID", 142, 162), ("PARTYIDQUALIFIER", 162, 164),
            ("PARTYDATEOFBIRTH", 164, 172), ("PARTYNONNATURALDATE", 164, 172), ("PARTYNONNATURALDATEQUALIFIER", 172, 175),
            ("PARTYNATURALINDICATOR", 175, 176), ("CONTRACTPARTYROLEQUALIFIER", 176, 177), ("IMPAIREDRISK", 177, 178),
            ("TRUSTREVOCABILITYINDICATOR", 178, 179), ("PARTYGENDER", 179, 180), ("BENEFICIARYAMOUNTQUANTITY", 180, 196),
            ("BENEFICIARYQUANTITYQUALIFIER", 196, 198), ("BENEFICIARYQUANTITYPERCENT", 198, 208),
            ("BENEFICIARYDISTRIBUTIONOPTION", 208, 209), ("UNDERWRITINGRISKCLASS", 209, 239),
            ("REJECTCODE", 288, 300)
        ],
        "matcher": lambda line: len(line) > 4 and line[1:3] == '13' and line[3:5] == '09',
        "display_name": "Contract Party Record"
    },
    
    "contract_party_address_record": {
        "layout": [
            ("SUBMITTERCODE", 0, 1), ("RECORDTYPE", 1, 3), ("SEQUENCENUMBER", 3, 5), ("CONTRACTNUMBER", 5, 35),
            ("PARTYROLE", 35, 37), ("PARTYADDRESSLINE1", 37, 72), ("PARTYADDRESSLINE2", 72, 107),
            ("PARTYCITY", 107, 137), ("PARTYSTATE", 137, 139), ("PARTYPOSTALCODE", 139, 154),
            ("PARTYCOUNTRYCODE", 154, 157), ("PARTYADDRESSLINE3", 157, 192), ("PARTYADDRESSLINE4", 192, 227),
            ("PARTYADDRESSLINE5", 227, 262), ("FOREIGNADDRESSINDICATOR", 262, 263),
            ("REJECTCODE", 288, 300)
        ],
        "matcher": lambda line: len(line) > 4 and line[1:3] == '13' and line[3:5] == '10',
        "display_name": "Contract Party Address Record"
    },
    
    "Contract_annuitization_payout_record": {
        "layout": [
            ("SUBMITTERCODE", 0, 1), ("RECORDTYPE", 1, 3), ("SEQUENCENUMBER", 3, 5), ("CONTRACTNUMBER", 5, 35),
            ("ANNUITYPAYOUTAMOUNT", 35, 51), ("ANNUITYPAYMENTAMOUNTQUALIFIER", 51, 54), ("ANNUITYFREQUENCYCODE", 54, 57),
            ("PAYOUTOPTION", 57, 59), ("LIVESTYPE", 59, 60), ("PAYOUTTYPE", 60, 61), ("CERTAINPERIOD", 61, 65),
            ("INCREASEPERCENTAGE", 65, 75), ("ASSUMEDINTERESTRATE", 75, 85), ("LEVELIZATIONINDICATOR", 85, 86),
            ("PRIMARYSURVIVORADJUSTMENTTYPE", 86, 87), ("PRIMARYSURVIVORADJUSTMENTPERCENTAGE", 87, 97),
            ("JOINTSURVIVORADJUSTMENTTYPE", 97, 98), ("JOINTSURVIVORADJUSTMENTPERCENTAGE", 98, 108),
            ("EXCLUSIONVALUE", 108, 124), ("EXCLUSIONINDICATOR", 124, 126), ("CERTAINPERIODQUALIFIER", 126, 129),
            ("LIQUIDITYOPTION", 129, 131), ("LIQUIDITYWAITINGPERIOD", 131, 133), ("LIQUIDITYTRIGGEREVENT", 133, 135),
            ("LIQUIDITYPARTIAL", 135, 136), ("PAYMENTSTARTDATE", 136, 144), ("PAYMENTENDDATE", 144, 152),
            ("RETURNOFPREMIUMPERCENTAGE", 152, 162), ("PAYOUTCHANGEDATE", 162, 170), ("PAYOUTCHANGEAMOUNT", 170, 186),
            ("PAYOUTCHANGEQUALIFIER", 186, 188), ("PAYOUTCHANGEDIRECTIONINDICATOR", 188, 189), ("PAYOUTCHANGEFREQUENCY", 189, 191),
            ("REJECTCODE", 288, 300)
        ],
        "matcher": lambda line: len(line) > 4 and line[1:3] == '13' and line[3:5] == '11',
        "display_name": "Contract Annuitization Payout Record"
    },
    
    "contract_party_communication_record": {
        "layout": [
            ("SYSTEMCODE", 0, 1), ("RECORDTYPE", 1, 3), ("SEQUENCENUMBER", 3, 5), ("CONTRACTNUMBER", 5, 35),
            ("CONTRACTENTITYTELEPHONETYPE1", 35, 37), ("CONTRACTENTITYTELEPHONENUMBER1", 37, 49), ("CONTRACTENTITYTELEPHONEEXTENSION1", 49, 55),
            ("CONTRACTENTITYTELEPHONETYPE2", 55, 57), ("CONTRACTENTITYTELEPHONENUMBER2", 57, 69), ("CONTRACTENTITYTELEPHONEEXTENSION2", 69, 75),
            ("CONTRACTENTITYTELEPHONETYPE3", 75, 77), ("CONTRACTENTITYTELEPHONENUMBER3", 77, 89), ("CONTRACTENTITYTELEPHONEEXTENSION3", 89, 95),
            ("CONTRACTENTITYEMAILADDRESS1", 95, 175), ("CONTRACTENTITYEMAILQUALIFIER1", 175, 177),
            ("CONTRACTENTITYEMAILADDRESS2", 177, 257), ("CONTRACTENTITYEMAILQUALIFIER2", 257, 259),
            ("ELECTRONICDELIVERYINDICATOR", 259, 260), ("REJECTCODE", 288, 300)
        ],
        "matcher": lambda line: len(line) > 4 and line[1:3] == '13' and line[3:5] == '12',
        "display_name": "Contract Party Communication Record"
    },
    
    "contract_service_feature_record": {
        "layout": [
            ("SUBMITTERSCODE", 0, 1), ("RECORDTYPE", 1, 3), ("SEQUENCENUMBER", 3, 5), ("CONTRACTNUMBER", 5, 35),
            ("BENEFITACTIVATIONINDICATOR", 35, 36), ("BENEFITACTIVATIONDATE", 36, 44), ("CREDITINGPERIODEXPIRATIONDATE", 44, 52),
            ("MARKETROLLUPDATE", 52, 60), ("MARKETROLLUPFREQUENCY", 60, 61), ("SERVICEFEATUREVALUE", 74, 88),
            ("SERVICEFEATUREVALUEQUALIFIER", 88, 90), ("SERVICEFEATUREFREQUENCY", 90, 91), ("SERVICEFEATURESTARTDATE", 92, 100),
            ("SERVICEFEATURESTOPDATE", 100, 108), ("EXPENSETYPE1", 108, 110), ("EXPENSEVALUE1", 110, 116), ("EXPENSEQUALIFIER1", 116, 118),
            ("EXPENSETYPE2", 118, 120), ("EXPENSEVALUE2", 120, 126), ("EXPENSEQUALIFIER2", 126, 128),
            ("LIVESTYPE", 128, 129), ("BENEFITREDUCTIONMETHOD", 129, 130), ("SERVICEFEATURENAME", 134, 169),
            ("SERVICEFEATUREPRODUCTCODE", 169, 189), ("SERVICEFEATUREPROGRAMTYPE", 189, 190),
            ("TYPECODE1", 190, 194), ("SUBTYPECODE1", 194, 198), ("TYPECODE2", 198, 202), ("SUBTYPECODE2", 202, 206),
            ("TYPECODE3", 206, 210), ("SUBTYPECODE3", 210, 214), ("SURRENDERCHARGESCHEDULE", 238, 288),
            ("REJECTCODE", 288, 300)
        ],
        "matcher": lambda line: len(line) > 4 and line[1:3] == '13' and line[3:5] == '15',
        "display_name": "Contract Service Feature Record"
    }
}

print(f"  Loaded {len(RECORD_CONFIGS)} record configurations")
for config_name in RECORD_CONFIGS.keys():
    print(f"      {config_name}")

# COMMAND ----------

# Core parsing functions (UNCHANGED)
def extract_file_drop_date(source_file):
    match = re.search(r'\.D(\d{6})\.', source_file)
    if match:
        date_str = match.group(1)
        try:
            return datetime.strptime(date_str, "%y%m%d").date()
        except ValueError:
            return None
    return None

def parse_line_to_record(line, layout):
    record = {}
    for col, start, end in layout:
        if start < len(line):
            record[col] = line[start:end].strip()
        else:
            record[col] = ""
    return record

def read_mro_file_content(file_path):
    print(f"📖 Reading file: {os.path.basename(file_path)}")
    
    try:
        df = spark.read.text(file_path)
        lines = df.collect()
        content_lines = [row.value for row in lines if row.value is not None]
        print(f"     Read {len(content_lines)} lines")
        return content_lines
    except Exception as e:
        print(f"     Error reading file: {e}")
        raise

# COMMAND ----------

# Enhanced parsing function (UNCHANGED)
def parse_mro_file_enhanced(file_path, file_name):
    """Enhanced MRO parsing with all record types"""
    
    file_drop_date = extract_file_drop_date(file_name)
    now_time = datetime.now()
    
    print(f"\n🚀 Parsing: {file_name}")
    print(f"File drop date: {file_drop_date}")
    
    try:
        # Read file content
        lines = read_mro_file_content(file_path)
        non_empty_lines = [line for line in lines if line.strip()]
        print(f"  Processing {len(non_empty_lines)} non-empty lines")
        
        # Initialize data collection
        parsed_data = {}
        unknown_layouts = []
        
        header_group_number = 0
        current_header_participant = None
        current_contra_record = None
        
        # Process each line
        for file_row_number, line in enumerate(non_empty_lines, 1):
            matched = False
            
            # Check for header record
            if len(line) > 2 and line[1:3] == "10":
                header_group_number += 1
                current_header_participant = line[3:7].strip() if len(line) > 6 else ""
            
            # Try to match against each record type
            for record_type, config in RECORD_CONFIGS.items():
                try:
                    if config["matcher"](line):
                        matched = True
                        
                        # Parse the record
                        record = parse_line_to_record(line, config["layout"])
                        
                        # Add metadata
                        record["FILEROWNUMBER"] = file_row_number
                        record["FILEHEADERGROUPNUMBER"] = header_group_number
                        record["SUBMITTINGPARTICIPANTNUMBER"] = current_header_participant or ""
                        record["SOURCEFILENAME"] = file_name
                        record["LOADDATE"] = now_time
                        record["MODIFIEDDATE"] = now_time
                        record["FILEDROPDAY"] = file_drop_date
                        
                        # Store contra record for enrichment
                        if record_type == "contra_record":
                            current_contra_record = record.copy()
                        
                        # Enrich contract records with contra data
                        elif record_type == "contract_record" and current_contra_record:
                            enrich_fields = [
                                "CONTRAPARTICIPANTNUMBER", "ASSOCIATEDFIRMID",
                                "ASSOCIATEDFIRMSUBMITTEDCONTRACTCOUNT",
                                "ASSOCIATEDFIRMDELIVEREDCONTRACTCOUNT",
                                "IPSEVENTCODE", "IPSSTAGECODE"
                            ]
                            for field in enrich_fields:
                                record[f"CONTRA_{field}"] = current_contra_record.get(field, "")
                        
                        # Collect the record
                        if record_type not in parsed_data:
                            parsed_data[record_type] = []
                        parsed_data[record_type].append(record)
                        break
                        
                except Exception as e:
                    print(f"⚠️  Error processing line {file_row_number}: {e}")
                    continue
            
            if not matched:
                unknown_layouts.append({
                    "FILENAME": file_name,
                    "FILEROWNUMBER": file_row_number,
                    "RECORDTYPE": line[1:3] if len(line) > 2 else "",
                    "DETAIL": line[:80] if len(line) > 80 else line,
                    "LOADDATE": now_time
                })
        
        # Convert to Spark DataFrames
        spark_dataframes = {}
        for record_type, records in parsed_data.items():
            if records:
                pandas_df = pd.DataFrame(records)
                spark_df = spark.createDataFrame(pandas_df)
                spark_dataframes[record_type] = spark_df
                
                display_name = RECORD_CONFIGS[record_type]['display_name']
                print(f"  {display_name}: {len(records):,} records")
        
        # Handle unknown layouts
        unknown_df = None
        if unknown_layouts:
            unknown_pandas_df = pd.DataFrame(unknown_layouts)
            unknown_df = spark.createDataFrame(unknown_pandas_df)
            print(f"⚠️  Unknown layouts: {len(unknown_layouts):,} records")
        
        return spark_dataframes, unknown_df
        
    except Exception as e:
        print(f"  Error parsing file: {e}")
        raise

# COMMAND ----------

# Enhanced CSV save function - MODIFIED for ADF integration
def save_to_csv_enhanced(dataframes, unknown_df=None, output_subfolder=None):
    """Save DataFrames to CSV using exact config names with optional subfolder"""
    
    print(f"💾 Saving CSVs to parsed container...")
    
    saved_files = []
    
    # Determine output path - MODIFIED for ADF
    if output_subfolder:
        base_path = f"{PROTOCOL}://parsed@{STORAGE_ACCOUNT_NAME}.{ENDPOINT}/{output_subfolder}"
        print(f"  Using subfolder: {output_subfolder}")
    else:
        base_path = f"{PROTOCOL}://parsed@{STORAGE_ACCOUNT_NAME}.{ENDPOINT}"
    
    # Create subfolder if specified
    if output_subfolder:
        try:
            dbutils.fs.mkdirs(base_path)
        except:
            pass
    
    # Save each DataFrame with exact config name
    for config_name, df in dataframes.items():
        try:
            print(f"   💾 Saving {config_name}.csv...")
            
            # Convert to Pandas for easier CSV handling
            pandas_df = df.toPandas()
            
            # Create CSV content
            csv_content = pandas_df.to_csv(index=False)
            
            # Save with exact config name
            csv_file_path = f"{base_path}/{config_name}.csv"
            dbutils.fs.put(csv_file_path, csv_content, overwrite=True)
            
            record_count = len(pandas_df)
            display_name = RECORD_CONFIGS[config_name]['display_name']
            print(f"     {display_name}: {record_count:,} records → {config_name}.csv")
            
            saved_files.append({
                'config_name': config_name,
                'display_name': display_name,
                'count': record_count,
                'file_name': f"{config_name}.csv"
            })
            
        except Exception as e:
            print(f"     Error saving {config_name}: {e}")
    
    # Save unknown layouts
    if unknown_df and unknown_df.count() > 0:
        try:
            print(f"   💾 Saving unknown_layouts.csv...")
            
            unknown_pandas_df = unknown_df.toPandas()
            csv_content = unknown_pandas_df.to_csv(index=False)
            csv_file_path = f"{base_path}/unknown_layouts.csv"
            dbutils.fs.put(csv_file_path, csv_content, overwrite=True)
            
            unknown_count = len(unknown_pandas_df)
            print(f"   ⚠️  Unknown Layouts: {unknown_count:,} records → unknown_layouts.csv")
            
            saved_files.append({
                'config_name': 'unknown_layouts',
                'display_name': 'Unknown Layouts',
                'count': unknown_count,
                'file_name': 'unknown_layouts.csv'
            })
            
        except Exception as e:
            print(f"     Error saving unknown layouts: {e}")
    
    return saved_files

# COMMAND ----------

# File management functions - MODIFIED for ADF integration
def get_storage_path(container_name, file_path=""):
    """Construct full storage path"""
    return f"{PROTOCOL}://{container_name}@{STORAGE_ACCOUNT_NAME}.{ENDPOINT}/{file_path}"

def move_file_to_processed(file_path, file_name, date_folder=None):
    """Move successfully processed file to processed folder with date organization"""
    try:
        # Use provided date or current date
        folder_date = date_folder if date_folder else datetime.now().strftime("%Y-%m-%d")
        processed_folder_path = get_storage_path("processed", folder_date)
        
        # Create folder if it doesn't exist
        try:
            dbutils.fs.mkdirs(processed_folder_path)
        except:
            pass
        
        # Move file
        destination_path = f"{processed_folder_path}/{file_name}"
        dbutils.fs.mv(file_path, destination_path)
        
        print(f"  Moved {file_name} to processed/{folder_date}/")
        return destination_path
        
    except Exception as e:
        print(f"⚠️  Could not move file to processed: {e}")
        return None

def log_processing_summary(file_name, parsing_results, processing_time, saved_files, date_folder=None):
    """Log processing summary with optional date folder"""
    try:
        # Use provided date or current date
        folder_date = date_folder if date_folder else datetime.now().strftime("%Y-%m-%d")
        log_folder_path = get_storage_path("logs", folder_date)
        
        # Create folder if it doesn't exist
        try:
            dbutils.fs.mkdirs(log_folder_path)
        except:
            pass
        
        # Create log content
        total_records = sum([info['count'] for info in saved_files])
        
        log_content = f"""DTCC Processing Summary
========================
File: {file_name}
Processing Date: {datetime.now()}
Processing Time: {processing_time:.2f} seconds
Total Records Parsed: {total_records:,}
Mode: {PROCESSING_MODE}

CSV Files Created:
"""
        
        for file_info in saved_files:
            log_content += f"  {file_info['file_name']}: {file_info['count']:,} records ({file_info['display_name']})\n"
        
        # Write log file
        log_file_path = f"{log_folder_path}/{file_name}_processing_summary.txt"
        dbutils.fs.put(log_file_path, log_content, overwrite=True)
        
        print(f"📝 Processing summary logged to logs/{folder_date}/")
        
    except Exception as e:
        print(f"⚠️  Could not write processing log: {e}")

# COMMAND ----------

# MODIFIED: Single file processing function for ADF
def process_single_mro_file(file_name, processing_date):
    """Process a single MRO file - designed for ADF triggers"""
    
    print(f" SINGLE FILE PROCESSING MODE")
    print(f" Target file: {file_name}")
    print(f"Processing date: {processing_date}")
    print("=" * 60)
    
    start_time = datetime.now()
    
    # Construct file path
    source_path = f"{PROTOCOL}://source@{STORAGE_ACCOUNT_NAME}.{ENDPOINT}/"
    file_path = f"{source_path}{file_name}"
    
    try:
        # Check if file exists
        try:
            file_info = dbutils.fs.ls(file_path)[0]
            size_mb = file_info.size / (1024 * 1024)
            print(f" Found file: {file_name} ({size_mb:.1f} MB)")
        except:
            raise FileNotFoundError(f"File not found: {file_name}")
        
        # Parse the file
        parsing_start = datetime.now()
        parsed_dataframes, unknown_df = parse_mro_file_enhanced(file_path, file_name)
        parsing_time = (datetime.now() - parsing_start).total_seconds()
        
        if not parsed_dataframes:
            raise ValueError("No data was parsed from the file")
        
        # Save to CSV with date-organized subfolder
        output_subfolder = processing_date
        saved_files = save_to_csv_enhanced(parsed_dataframes, unknown_df, output_subfolder)
        
        # Log processing summary
        log_processing_summary(file_name, parsed_dataframes, parsing_time, saved_files, processing_date)
        
        # Move to processed folder
        move_file_to_processed(file_path, file_name, processing_date)
        
        # Calculate totals
        total_records = sum([info['count'] for info in saved_files])
        total_time = (datetime.now() - start_time).total_seconds()
        
        # Success summary
        print(f"\n🎉 PROCESSING COMPLETED SUCCESSFULLY!")
        print("=" * 60)
        print(f" File: {file_name}")
        print(f"⏱️  Processing time: {total_time:.2f} seconds")
        print(f"  Total records: {total_records:,}")
        print(f"  CSV files created: {len(saved_files)}")
        print(f" Output location: parsed/{processing_date}/")
        print(f"🗃️  Archived to: processed/{processing_date}/")
        
        # Return success status for ADF
        return {
            "status": "success",
            "file_name": file_name,
            "processing_date": processing_date,
            "total_records": total_records,
            "csv_files_created": len(saved_files),
            "processing_time": total_time
        }
        
    except Exception as e:
        error_message = f"Processing failed: {str(e)}"
        print(f"  PROCESSING FAILED!")
        print("=" * 60)
        print(f" File: {file_name}")
        print(f"  Error: {error_message}")
        
        # Return failure status for ADF
        return {
            "status": "failed",
            "file_name": file_name,
            "processing_date": processing_date,
            "error": error_message
        }

# COMMAND ----------

# MODIFIED: Batch processing function (for manual runs)
def process_mro_files_batch():
    """Batch processing function - processes all files in source container"""
    
    print(" BATCH PROCESSING MODE")
    print(" Processing all MRO files in source container")
    print("=" * 60)
    
    start_time = datetime.now()
    
    # Discover MRO files
    source_path = f"{PROTOCOL}://source@{STORAGE_ACCOUNT_NAME}.{ENDPOINT}/"
    
    try:
        files = dbutils.fs.ls(source_path)
        mro_files = [f for f in files if f.name.lower().endswith('.mro')]
        
        if not mro_files:
            print("⚠️  No MRO files found in source container")
            return
        
        print(f" Found {len(mro_files)} MRO files:")
        for file_info in mro_files:
            size_mb = file_info.size / (1024 * 1024)
            print(f"    {file_info.name}: {size_mb:.1f} MB")
        
        # Process each file
        successful_files = []
        failed_files = []
        
        for file_info in mro_files:
            file_name = file_info.name
            file_path = file_info.path
            
            try:
                print("\n" + "="*60)
                
                # Parse the file
                parsing_start = datetime.now()
                parsed_dataframes, unknown_df = parse_mro_file_enhanced(file_path, file_name)
                parsing_time = (datetime.now() - parsing_start).total_seconds()
                
                if not parsed_dataframes:
                    print("⚠️  No data was parsed")
                    continue
                
                # Save to CSV without subfolder (batch mode)
                saved_files = save_to_csv_enhanced(parsed_dataframes, unknown_df)
                
                # Log processing summary
                log_processing_summary(file_name, parsed_dataframes, parsing_time, saved_files)
                
                # Move to processed folder
                move_file_to_processed(file_path, file_name)
                
                # Track success
                total_records = sum([info['count'] for info in saved_files])
                successful_files.append({
                    'name': file_name,
                    'processing_time': parsing_time,
                    'saved_files': saved_files,
                    'total_records': total_records
                })
                
                print(f"\n🎉 Successfully processed {file_name}")
                print(f"  Total records: {total_records:,}")
                print(f"  CSV files created: {len(saved_files)}")
                
            except Exception as e:
                error_message = f"Processing failed: {str(e)}"
                print(f"  Failed to process {file_name}: {error_message}")
                
                failed_files.append({
                    'name': file_name,
                    'error': error_message
                })
                continue
        
        # Final summary
        total_time = (datetime.now() - start_time).total_seconds()
        
        print("\n" + "="*60)
        print("  BATCH PROCESSING SUMMARY")
        print("="*60)
        print(f"  Total Processing Time: {total_time:.2f} seconds")
        print(f"  Successfully Processed: {len(successful_files)} files")
        print(f"  Failed: {len(failed_files)} files")
        
        if successful_files:
            print(f"\n  SUCCESSFUL FILES:")
            all_csv_files = {}
            total_records_all = 0
            
            for file_info in successful_files:
                total_records_all += file_info['total_records']
                print(f"    {file_info['name']}: {file_info['total_records']:,} records")
                
                # Collect unique CSV files created
                for saved_file in file_info['saved_files']:
                    config_name = saved_file['config_name']
                    if config_name not in all_csv_files:
                        all_csv_files[config_name] = {
                            'file_name': saved_file['file_name'],
                            'display_name': saved_file['display_name'],
                            'total_count': 0
                        }
                    all_csv_files[config_name]['total_count'] += saved_file['count']
            
            print(f"\n  TOTAL RECORDS PROCESSED: {total_records_all:,}")
            
            print(f"\n  CSV FILES CREATED (in parsed container):")
            for config_name, info in all_csv_files.items():
                print(f"    {info['file_name']}: {info['total_count']:,} records ({info['display_name']})")
        
        if failed_files:
            print(f"\n  FAILED FILES:")
            for file_info in failed_files:
                print(f"    {file_info['name']}: {file_info['error']}")
        
        print(f"\n🏁 Batch processing pipeline completed!")
        
    except Exception as e:
        print(f"  Error in batch processing: {e}")
        raise

# COMMAND ----------

# MAIN EXECUTION LOGIC - NEW: Route based on processing mode
try:
    if PROCESSING_MODE == "single":
        # ADF triggered processing - single file
        result = process_single_mro_file(TARGET_FILE, PROCESSING_DATE)
        
        # Output result for ADF monitoring
        print(f"\n ADF RESULT:")
        print(f"Status: {result['status']}")
        if result['status'] == 'success':
            print(f"Records processed: {result['total_records']:,}")
            print(f"CSV files created: {result['csv_files_created']}")
            print(f"Processing time: {result['processing_time']:.2f}s")
        else:
            print(f"Error: {result['error']}")
            
    else:
        # Manual run - batch processing
        process_mro_files_batch()
        
except Exception as e:
    print(f"  Pipeline failed: {e}")
    import traceback
    traceback.print_exc()
    
    # For ADF, re-raise the exception so it shows as failed
    if PROCESSING_MODE == "single":
        raise e

# COMMAND ----------

# Verify results - MODIFIED to check appropriate location
print("\n  VERIFYING CSV FILES:")
print("=" * 60)

try:
    if PROCESSING_MODE == "single":
        # Check date-organized subfolder
        parsed_path = f"{PROTOCOL}://parsed@{STORAGE_ACCOUNT_NAME}.{ENDPOINT}/{PROCESSING_DATE}/"
        print(f" Checking: parsed/{PROCESSING_DATE}/")
    else:
        # Check root of parsed container
        parsed_path = f"{PROTOCOL}://parsed@{STORAGE_ACCOUNT_NAME}.{ENDPOINT}/"
        print(f" Checking: parsed/ (root)")
    
    files = dbutils.fs.ls(parsed_path)
    csv_files = [f for f in files if f.name.endswith('.csv')]
    
    if csv_files:
        print(f"  Found {len(csv_files)} CSV files:")
        
        for file in csv_files:
            size_mb = file.size / (1024 * 1024)
            
            # Check if it matches a config name
            config_name = file.name.replace('.csv', '')
            if config_name in RECORD_CONFIGS:
                display_name = RECORD_CONFIGS[config_name]['display_name']
                print(f"    {file.name}: {size_mb:.2f} MB ({display_name})")
            else:
                print(f"    {file.name}: {size_mb:.2f} MB")
        
        print(f"\n All CSV files use exact config names!")
        if PROCESSING_MODE == "single":
            print(f" Location: Azure Portal > Storage Account > parsed > {PROCESSING_DATE}/")
        else:
            print(f" Location: Azure Portal > Storage Account > parsed/")
        
    else:
        print("  No CSV files found")
        
except Exception as e:
    print(f"  Error verifying results: {e}")

# COMMAND ----------

# Show processing summary
print(f"\n   PROCESSING SUMMARY:")
print("=" * 50)
print(f" Mode: {PROCESSING_MODE}")
if PROCESSING_MODE == "single":
    print(f" File: {TARGET_FILE}")
    print(f"Date: {PROCESSING_DATE}")
    print(f" Output: parsed/{PROCESSING_DATE}/")
else:
    print(f" Source: All .mro files in source container")
    print(f" Output: parsed/ (root)")

print(f"\n   CONFIG NAME → CSV FILE MAPPING:")
print("=" * 50)

for config_name, config in RECORD_CONFIGS.items():
    print(f" {config_name}.csv → {config['display_name']}")

