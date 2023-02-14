#!/usr/bin/python3
# -*- coding: utf-8 -*-
# pylint: disable=line-too-long

import argparse
from rich import print
import os

from snowflake.snowpark import Session
from snowflake.snowpark.types import *
from snowflake.snowpark.functions import udf
import snowpark_extensions

print("[cyan]Snowpark Extensions Extras")
print("[cyan]Register readexcel udf")
print("[cyan]=============================")
arguments = argparse.ArgumentParser()
arguments.add_argument("--udfname",default="readexcel",help="Register an udf that can then be used to read the excel content")
arguments.add_argument("--stage",help="stage where the udf will be registered",required=True)
arguments.add_argument("--imports",help="imports for the udf, the input file(s) should be here",required=True)
arguments.add_argument("--connection",dest="connection_args",nargs="*",required=True,help="Connect options, for example snowsql, snowsql connection,env")


args = arguments.parse_args()
print(args)
session = None
try:
    if len(args.connection_args) >= 1:
        first_arg = args.connection_args[0]
        rest_args = args.connection_args[1:]
        if first_arg == "snowsql":
            session = Session.builder.from_snowsql(*rest_args).create()
        elif first_arg == "env":
            session = Session.builder.from_env().create()
        else:
            connection_args={}
            for arg in args.connection_args:
                key, value = arg.split("=")
                connection_args[key] = value
            session = Session.builder.configs(connection_args).create()
except Exception as e:
    print(e)
    print("[red] An error happened while trying to connect")
    exit(1)
if not session:
    print("[red] Not connected. Aborting")
    exit(2)

imports = []
if ";" in args.imports:
    imports.extend(args.imports.split(";"))
else:
    imports.append(args.imports)


session.udf.register_from_file(file_path="readexcel.py", func_name="excel_to_json"
, name = args.udfname
, return_type = VariantType()
, input_types = [StringType(),BooleanType(),StringType(),StringType()]
, is_permanent = True
, replace = True
, packages=['openpyxl']
, imports=imports
, stage_location = args.stage
)

filename = os.path.basename(args.imports)
print(f"""
UDF {args.udfname} was registered. You can now use it like:
SELECT {args.udfname}('sample.xlsx',True,'Sheet1','A1:M5')
or 
SELECT {args.udfname}('sample.xlsx',True,NULL,NULL)
""")