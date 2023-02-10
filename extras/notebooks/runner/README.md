# Notebook Runner

The notebook runner is a small example that allows you to run a notebook from within snowflake.

The runner script will:
1. connect to snowflake 
2. upload the notebook, 
3. publish a storeproc, 
4. run the store procedure, 
5. save the results of the notebook and 
6. then download the results as an html

You can run it like:

```bash
python runner.py --notebook example1.ipynb --connection user=user1 account=account1 password=xxxx database=database1 schema=schema1 warehouse=warehouse1
```

This script call also be used to publish a permanent stored proc that can then be used to run any notebook that is already on an stage,
or to schedule a task to run a notebook.

For example, you can run:

```bash
python runner.py --notebook example1.ipynb --register_and_exit yes  --connection user=user1 account=account1 password=xxxx database=database1 schema=schema1 warehouse=warehouse1
```

And then use:
```
CALL RUN_NOTEBOOK('notebook_run','example1.ipynb',true);
```
Using `true` the function will return an url like:
https://sfc-ds1-customer-stage.s3.us-west-2.amazonaws.com/jzy4-s-ssca1174/stages/b0a26133-7bd7-43ec-8bb1-bb1ed94296d3/example1.ipynb_02_09_2023_18_39_08.html?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20230210T023908Z&X-Amz-SignedHeaders=host&X-Amz-Expires=3599&X-Amz-Credential=AKIAIJY43JJYLVHDLGOA%2F20230210%2Fus-west-2%2Fs3%2Faws4_request&X-Amz-Signature=a6633af1228bc2d9f1251fffed436fbd789e6a00b9a3ce1b0a86a948c51e695e 