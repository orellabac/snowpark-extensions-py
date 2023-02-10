# An UDF to read a pdf from snowflake using python

This code comes from [Ranjeeta Pegu Article](https://medium.com/@ranjeetapegu/parsing-storing-pdf-file-in-snowflake-using-snowpark-python-api-1bf2e188e480)

You can run it like:

```bash
python register_udf.py --stage mystage --udfname pdfread --imports @mystage/sample.pdf --connection user=user1 account=account1 password=xxxx database=database1 schema=schema1 warehouse=warehouse1
```
or 

```
python register_udf.py --stage mystage --udfname pdfread --imports @mystage/sample.pdf --connection snowsql
```

After registration you can run it from snowflake like:

```sql
SELECT "DEMODB"."PUBLIC".pdfread('sample.pdf')
```                         ;