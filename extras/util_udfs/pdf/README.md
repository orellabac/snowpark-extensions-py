# An UDF to read a pdf file from snowflake using python


You can run it like:

```bash
python register_udf.py --stage mystage --udfname excelread --imports @mystage/sample.pdf --connection user=user1 account=account1 password=xxxx database=database1 schema=schema1 warehouse=warehouse1
```
or 

```
python register_udf.py --stage mystage --udfname pdfread --imports @mystage/sample.pdf --connection snowsql
```

After registration you can run it from snowflake like:

```sql
SELECT "DEMODB"."PUBLIC".pdfread('sample.pdf')
```                         ;