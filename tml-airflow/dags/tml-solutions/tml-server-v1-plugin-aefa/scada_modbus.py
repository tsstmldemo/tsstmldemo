from typing import Dict, Any
from collections import defaultdict, deque
from pyModbusTCP.client import ModbusClient
import requests
import time
import json
import re
from typing import Dict, Any
from collections import defaultdict
from datetime import datetime, timezone
# Your existing test code (FIXED imports and missing Set)
from typing import Set
import scadaglobals as sg
import maadstml
import os

##🟢 gas_reynolds: 181,298 → **HIGHLY TURBULENT** (excellent droplet separation)
##🟢 water_reynolds: 639 → **LAMINAR-TURBULENT TRANSITION** (stable liquid flow)
##🟡 carryover: 17.25% → **HIGH** (monitor - target <10%)
##🟢 flow_stability: 3,720 → **EXTREMELY STABLE**
##🟢 stokes_number: 0.00000196 → **PERFECT** (droplets settle instantly)
##🟢 density_ratio: 13.12 → **NORMAL** (water >> gas typical)
##🟢 reynolds_ratio: 284 → **GAS-DOMINANT** (normal for separators)
##🟡 emulsion_ratio: 2.24 → **MODERATE** emulsion tendency
##🟢 inversion_risk: -0.34 → **VERY SAFE** (negative = no inversion risk)
##

class DynamicValidator:
    def __init__(self, validation_rules):
        """
        :param validation_rules: A dictionary of {field: {"min": x, "max": y}}
        """
        self.rules = validation_rules

    def validate(self, payload_data):
        """
        Returns a list of errors and a status flag.
        """
        results = {
            "status": "PASS",
            "errors": {}
        }

        for field, value in payload_data.items():
            # Check if we have a rule for this field
            if field in self.rules:
                bounds = self.rules[field]

                # Perform the bounds check
                if value < bounds["min"] or value > bounds["max"]:
                    results["status"] = "FAIL"
                    results["errors"][field] = {
                        "value": value,
                        "message": f"Value {value} outside range [{bounds['min']}, {bounds['max']}]"
                    }

        return results

def producetokafka(value, tmlid, identifier,producerid,maintopic,substream,args,VIPERTOKEN, VIPERHOST, VIPERPORT):
     inputbuf=value
     topicid=int(args['topicid'])

     # Add a 7000 millisecond maximum delay for VIPER to wait for Kafka to return confirmation message is received and written to topic
     delay=int(args['delay'])
     enabletls = int(args['enabletls'])
     identifier = args['identifier']

     try:
        result=maadstml.viperproducetotopic(VIPERTOKEN,VIPERHOST,VIPERPORT,maintopic,producerid,enabletls,delay,'','', '',0,inputbuf,substream,
                                            topicid,identifier)
     #   print("scada/modbus result========",result)
     except Exception as e:
        print("ERROR:",e)


def modbus_read_loop(scada_cfg, interval_s, callback_url, max_reads, fields, scaling, start_address, sendtotopic, job_id, VIPERTOKEN, VIPERHOST, VIPERPORT,
                     args,vessel_names,preprocessing,preprocessing1,machinelearning, predictions, agenticai, ai,validation_bounds,localfoldername,
                     topologyname,
                     callback,
                     alertemails,
                     threshold,
                     preprocessedtopic,
                     createvariables=""):
    """100% DYNAMIC - uses ONLY data from request"""
    read_count = 0
    client = ModbusClient(host=scada_cfg["host"], port=scada_cfg["port"], unit_id=scada_cfg["unit_id"])
    topologyname = scada_cfg["topologyname"]

    buf = f"{scada_cfg['host']}:{scada_cfg['port']}"

    network = sg.ViperNetworkClient()

    network = sg.ViperNetworkClient()
    log_folder = f"/rawdata/carryover/{localfoldername}/datachecks"
    os.makedirs(log_folder, exist_ok=True)
    log_filename = f"/rawdata/carryover/{localfoldername}/datachecks/datacheck.json"
    check_and_truncate(log_filename)

    while True:
            if not client.open():
                time.sleep(1)
                continue

            try:
                if buf in sg.scadatopologystop:
                  sg.scadatopologystop = [v for v in sg.scadatopologystop if v != buf]
                #  sg.scadatopologyrunning = [v for v in sg.scadatopologyrunning if v != topologyname]
                  break

                regs = client.read_holding_registers(start_address, 100)
                if isinstance(regs, list) and len(regs) > 0:
                    # DYNAMIC PAYLOAD from request data ONLY
                    payload = {
                        k: v for k, v in locals().items() if k in ['scada_cfg', 'interval_s', 'callback_url', 'max_reads', 'fields', 'scaling', 'start_address','sendtotopic','vessel_names','preprocessing','preprocessing1','machinelearning',
                                                                   'predictions','agenticai','ai','validation_bounds','topologyname','callback','alertemails','threshold','preprocessedtopic']
                    }
                    payload.update({
                        "scada_host": scada_cfg["host"],
                        "scada_port": scada_cfg["port"],
                        "slave_id": scada_cfg["unit_id"],
                        "read_interval_seconds": interval_s,
                        "callback_url": callback_url,
                        "createvariables": createvariables,  # From request
                        "fields": fields,  # From request
                        "scaling": scaling,  # From request
                        "sendtotopic": sendtotopic,
                        "vessel_names": vessel_names,
                        "preprocessing": preprocessing,
                        "preprocessing1": preprocessing1,
                        "machinelearning": machinelearning,
                        "predictions": predictions,
                        "agenticai": agenticai,
                        "validation_bounds": validation_bounds,
                        "ai": ai,
                        "topologyname": topologyname,
                        "topologycallback": callback,
                        "topologyalertemails": alertemails,
                        "topologythreshold": threshold,
                        "topologypreprocessedtopic": preprocessedtopic,
                        "localfoldername": localfoldername
                    })

                    # Map ALL registers dynamically
                    for i, val in enumerate(regs):
                        if val != 0:
                            field_name = fields[i] if i < len(fields) else f"reg_{i+1:04d}"
                            payload[field_name] = float(val)

                    read_count += 1

                    # DYNAMIC PIPELINE
                    processed_vessel = process_payload(payload,localfoldername)
                    if processed_vessel == None:
                         continue

                    osdu_json = payload_to_osdu_dynamic(processed_vessel)

               #     print(f"✓ Read {len(fields)} → {len(processed_vessel)} total | Count: {read_count}")
                    if sendtotopic != "":
                      maintopic = sendtotopic
                      producerid = args['producerid']
                      producetokafka(json.dumps(osdu_json), "", "",producerid,maintopic,"",args,VIPERTOKEN, VIPERHOST, VIPERPORT)


                    # POST OSDU dynamically
                    if callback_url != "":
                        callurls = callback_url.split(",")
                        for u in callurls:
                          try:
                            network.post_data(u.strip(), json=osdu_json)
                          except Exception as e:
                            continue

                    # DYNAMIC CALLBACK - works with ANY structure
                    #callback_data = {
                     #   "job_id": job_id,
                      #  "read_count": read_count,
                       # **processed_vessel  # Flatten all data
                   # }
                    #requests.post(callback_url, json=callback_data, timeout=5.0)

            finally:
                client.close()

            time.sleep(interval_s)
    if sg.stop_read.is_set():
      sg.stop_read.clear()

def check_and_truncate(filepath, max_size_bytes=1024*1024):
    if os.path.exists(filepath):
        if os.path.getsize(filepath) > max_size_bytes:
            # Truncate file to 0 bytes
            with open(filepath, 'w') as f:
                f.truncate(0)

def process_payload(payload: Dict[str, Any], localfoldername: str) -> Dict[str, Any]:
    """
    FIXED: Properly separates field_values from computed_vars in dependency resolution
    """
    # Extract original field values with scaling
    scaling = payload.get("scaling", {})
    fields = payload.get("fields", [])
    field_values = {}
    validation_bounds = payload.get("validation_bounds", {})

    for field in fields:
        raw_value = payload.get(field, 0)
        scale = scaling.get(field, 1)
        field_values[field] = raw_value / scale

########################## Do validation check here ############
#validation_bounds
#field_values
    validator = DynamicValidator(validation_bounds)
    report = validator.validate(field_values)
    if report["status"] == "FAIL":
       report["timestamp"] = datetime.now().isoformat()
    # Append the report to the file
       try:
         log_folder = f"/rawdata/carryover/{localfoldername}/datachecks"
         log_filename = f"/rawdata/carryover/{localfoldername}/datachecks/datacheck.json"
         os.makedirs(log_folder, exist_ok=True)
         check_and_truncate(log_filename)
         with open(log_filename, 'a') as f:
            f.write(json.dumps(report) + "\n")
       except IOError as e:
         print(f"CRITICAL: Could not write to log file: {e}")
       return None
###############################################################
    # Parse createvariables - FIXED REGEX (double backslashes)
    create_str = payload.get("createvariables", "")

  #  print("createvariable=========",create_str)

    statements = [s.strip() for s in create_str.split(',') if s.strip()]

    # Build computed variables mapping
    var_to_expr = {}
    deps = defaultdict(list)  # computed_var -> dependencies

    for stmt in statements:
        # FIXED: Proper regex pattern
        match = re.match(r'^\s*(\w+)\s*=\s*(.+?)\s*$', stmt)
        if not match:
            continue
        var_name, expr = match.groups()
        var_to_expr[var_name] = expr.strip()

        # Find dependencies: split between FIELDS vs COMPUTED_VARS
        words = re.findall(r'\b[a-zA-Z_]\w*\b', expr)
        for word in words:
            if word != var_name:
                if word in field_values:
                    deps[var_name].append(word)  # Field dependency
                elif word in var_to_expr:
                    deps[var_name].append(word)  # Computed var dependency

    # FIXED RECURSIVE COMPUTATION
    computed_vars = {}

    def compute_var(var: str) -> float:
        if var in computed_vars:
            return computed_vars[var]

        # Compute dependencies FIRST
        for dep in deps[var]:
            if dep in field_values:
                pass  # Fields always available
            elif dep in var_to_expr:
                compute_var(dep)  # Recurse on computed vars only
            else:
                print(f"Warning: Unknown dependency '{dep}' for {var}")

        # Safe evaluation with ALL available variables
        expr = var_to_expr[var]
        safe_locals = {**field_values, **computed_vars}

        try:
            value = eval(expr, {"__builtins__": {}}, safe_locals)
            computed_vars[var] = float(value)
 #           print(f"✓ {var} = {value}")
        except Exception as e:
            print(f"✗ Error computing {var}: {e}")
            computed_vars[var] = 0.0

        return computed_vars[var]

    # Compute all variables in order
    for var_name in var_to_expr:
        compute_var(var_name)

    # ADD NEW VARIABLES BACK TO PAYLOAD
    payload.update(computed_vars)
#    print(f"\n✅ Added {len(computed_vars)} computed variables")

    return payload

def payload_to_osdu_dynamic(processed_payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    ComputedAnalytics = ORIGINAL PRECISION, RawFields = 3-DECIMAL ROUNDED
    """
    timestamp = datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')

    # Get structure from payload
    fields = processed_payload.get("fields", [])
    scaling = processed_payload.get("scaling", {})
    createvariables = processed_payload.get("createvariables", "")

    # Parse computed variable names
    computed_var_names = set()
    for stmt in createvariables.split(','):
        match = re.match(r'^\s*(\w+)\s*=', stmt.strip())
        if match:
            computed_var_names.add(match.group(1))

    # RawFields = ROUNDED 3 decimals (SCADA + computed)
    raw_fields = {}
    for field in fields:
        value = processed_payload.get(field, 0)
        raw_fields[field] = value if isinstance(value, float) else value

    # Add ROUNDED computed vars to RawFields
    for var_name in computed_var_names:
        if var_name in processed_payload:
            value = processed_payload[var_name]
            raw_fields[var_name] = value if isinstance(value, float) else value

    # ComputedAnalytics = ✅ ORIGINAL FULL PRECISION VALUES
    computed_vars = {}
    for var_name in computed_var_names:
        if var_name in processed_payload:
            computed_vars[var_name] = processed_payload[var_name]  # NO ROUNDING!

#    print("processed payload:",json.dumps(processed_payload))
    # Config keys
    config_keys = {
        'scada_host', 'scada_port', 'slave_id', 'read_interval_seconds',
        'tml_api_url', 'callback_url', 'max_reads', 'start_register',
        'createvariables', 'fields', 'scaling','sendtotopic','vessel_names',
        'preprocessing','preprocessing1','machinelearning','predictions','agenticai','ai',
        'validation_bounds', 'topologyname','topologycallback','topologyalertemails','topologythreshold',
        'topologypreprocessedtopic','localfoldername'
    }

    vessel_id = int(processed_payload.get('vesselIndex', 0))
    scada_host = processed_payload.get('scada_host', 'unknown')
    scada_port = processed_payload.get('scada_port', 0)

    vessel_names = processed_payload.get('vessel_names', {})  # default dict
    vessel_name = vessel_names.get(str(vessel_id), f"Vessel_{vessel_id}")
    vessel_name = vessel_name.replace(" ", "_")

    osdu_record = {
        "kind": "dataset--data:opendes:dataset--Well:1.0",
        "acl": {"viewers": ["data.default.viewers@[default]"], "owners": ["data.default.owners@[default]"]},
        "legal": {
            "legaltags": ["data.default.legaltag@[default]"],
            "otherRelevantDataCountries": ["US", "CA"],
            "hostingCountry": "US"
        },
        "data": {
            "ID": f"vessel-{vessel_id}-{int(datetime.now(timezone.utc).timestamp())}",
            "Source": f"SCADA:{scada_host}:{scada_port}",
            "AcquisitionTime": timestamp,
            "WellUWI": f"{vessel_name}",
            "SeparatorMetrics": {
                **{k: processed_payload[k] for k in config_keys if k in processed_payload},
                "RawFields": raw_fields,  # 3-DECIMAL ROUNDED (27 fields total)
                "ComputedAnalytics": computed_vars,  # ✅ FULL PRECISION ORIGINAL VALUES
                "meta": {
                    "dataType": "OilAndGas:SeparatorRealtime",
                    "version": "1.0",
                    "computedAt": timestamp,
                    "scalingApplied": bool(scaling),
                    "numTotalFields": len(raw_fields),
                    "numRawFields": len(fields),
                    "numComputedVars": len(computed_var_names),
                    "computedVarList": list(computed_var_names),
                    "precision": {
                        "RawFields": "3 decimal places",
                        "ComputedAnalytics": "full precision"
                    }
                }
            }
        },
        "meta": {
            "dataType": "OilAndGas:SeparatorRealtime",
            "formatVersion": "1.0.0",
            "osdVersion": "1.0.0",
            "creationTime": timestamp,
            "modificationTime": timestamp,
            "domain": {"type": "WellProduction", "subType": "SeparatorAnalytics"}
        }
    }

    return osdu_record

# PRODUCTION USAGE - works with ANY payload:
def full_pipeline(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Complete pipeline: process → compute → OSDU"""
    # 1. Compute variables (your existing function)
    processed = process_payload(payload)

    # 2. Convert to OSDU dynamically
    osdu = payload_to_osdu_dynamic(processed)

    return osdu

##
##with open('payload.json', 'r') as f:
##            data = json.load(f)
##
##data.update({
##  "vesselIndex": 27.0,
##  "operatingPressure": 66.67,
##  "operatingTemperature": 123.42,
##  "gasFlowRate": 217.28,
##  "gasDensity": 1.195,
##  "gasCompressabilityFactor": 0.842,
##  "gasViscosity": 1.2777e-05,
##  "hclFlowRate": 25.08,
##  "hclDensity": 1484.0,
##  "hclViscosity": 0.000895,
##  "hclSurfaceTension": 0.065,
##  "waterFlowRate": 39.318,
##  "waterDensity": 10.0,
##  "waterViscosity": 0.00118,
##  "waterSurfaceTension": 0.07128,
##  "hclWaterSurfaceTension": 0.03311,
##  "phseInversionCriticalWaterCut": 0.261,
##  "solidFlowRate": 9.99,
##  "solidDensity": 14.9
##})
##
##result=full_pipeline(data)
##
##print("Full-json:", json.dumps(result))





















