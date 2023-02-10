#!/usr/bin/python3
# -*- coding: utf-8 -*-
# pylint: disable=line-too-long

import argparse
from rich import print
import os

from snowflake.snowpark import Session
from snowflake.snowpark.types import *
from snowflake.snowpark.functions import sproc
from snowflake.snowpark.dataframe import _generate_prefix
import snowpark_extensions

print("[cyan]Snowpark Extensions Extras")
print("[cyan]Notebook Runner")
print("[cyan]=============================")
arguments = argparse.ArgumentParser()
arguments.add_argument("--notebook",help="Jupyter Notebook to run")
arguments.add_argument("--procname",default="run_notebook",help="Register an stored proc that can then be used to run notebooks")
arguments.add_argument("--register_and_exit",help="If yes registers the proc permanently and exits",default="no")
arguments.add_argument("--stage",help="stage",default="NOTEBOOK_RUN")
arguments.add_argument("--packages",help="packages",default="")
arguments.add_argument("--imports" ,help="imports" ,default="")
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

if not args.register_and_exit.upper()=="YES":
    session.sql(f"CREATE STAGE IF NOT EXISTS {args.stage} ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')").show()
    print(f"Uploading notebook to stage {args.stage}")
    session.file.put(f"file://{args.notebook}",f'@{args.stage}',auto_compress=False,overwrite=True)
    print(f"Notebook uploaded")

packages=["snowflake-snowpark-python","nbconvert","nbformat","ipython","jinja2==3.0.3","plotly"]
packages.extend(set(filter(None, args.packages.split(','))))
print(f"Using packages [magenta]{packages}")
imports=[]
if args.imports:
    imports.extend(args.imports.split(','))
is_permanent=False
proc_stage=None
if args.register_and_exit.upper()=="YES":
    is_permanent=True
    print(f"Registering proc [cyan] {args.procname}")
    if args.stage:
        proc_stage = args.stage
    else:
        print("Stage must be provide to register the proc. Use --stage stagename")
        exit(1)
if not is_permanent:
    args.procname = _generate_prefix(args.procname)

session.sproc.register_from_file(file_path="notebook_run.py",
return_type=StringType(),
input_types=[StringType(),StringType(),BooleanType()],
func_name="run_notebook",name=args.procname,replace=True,
is_permanent=is_permanent,packages=packages,imports=[],stage_location=proc_stage)


if not args.register_and_exit.upper()=="YES":
    print(f"STAGE: [cyan]{args.stage}")
    result = session.call(args.procname,args.stage,args.notebook,False)
    print(f"Results have been written to {result}")
    downloaded_results = session.file.get(result,"file://.")
    print("Downloading results to local folder")
    for downloaded_result in downloaded_results:
        print(f"Downloaded {downloaded_result.file}")
        target_file = downloaded_result.file.replace(".gz","")
    print("Done!")
else:
    print(f"""
PROCEDURE {args.procname} was registered. You can now use it like:

CALL {session.get_fully_qualified_current_schema()}.{args.procname}('stage_name','filename.ipynb',TRUE)

Using true, will return an url that can be used to download the html results.
NOTE: for an internal stage it has be create like CREATE STAGE STAGE_NAME ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE');
if not the download files will be corrupted.

""")

