from snowflake.snowpark import DataFrame, Row, DataFrameWriter
from snowflake.snowpark.types import StructType
from snowflake.snowpark import context
from typing import Any, Union, List, Optional
from snowflake.snowpark.functions import lit
from snowflake.snowpark.dataframe import _generate_prefix

if not hasattr(DataFrameWriter,"___extended"):
    
    DataFrameWriter.___extended = True