def excel_to_json(file_path:str, use_first_row:bool=True,sheet_name:str=None, range:str=None):
    import openpyxl, json, os, sys
    from datetime import datetime
    import_dir = sys._xoptions.get("snowflake_import_directory") or ""
    excel_file = os.path.join(import_dir, file_path)
    wb = openpyxl.load_workbook(excel_file)
    if sheet_name is not None:
        ws = wb[sheet_name]
    else:
        ws = wb.worksheets[0]
    data = {}
    first_row = None
    first_row_cells = None
    for row in ws.iter_rows(values_only=False):
        if first_row is None:
            first_row = row
            first_row_cells = [str(x.value).strip() for x in first_row]
            continue
        row_data = {}
        for col_index, cell in enumerate(row, 1):
            if use_first_row:
                row_data[first_row_cells[col_index-1]] = cell.value
            else:
                row_data[col_index] = cell.value
        data[ws.row_dimensions[row[0].row].index] = row_data
    if range is not None:
        start, end = range.split(':')
        start_col, start_row = openpyxl.utils.cell.coordinate_from_string(start)
        end_col, end_row = openpyxl.utils.cell.coordinate_from_string(end)
        start_row, end_row = int(start_row), int(end_row)
        data = {k: v for k, v in data.items() if start_row <= k <= end_row}
    # def json_serializer(obj):
    #     if isinstance(obj, datetime):
    #         return obj.strftime('%Y-%m-%d %H:%M:%S')
    #     else:
    #         str(obj)
    #return json.dumps(data, indent=4, default=json_serializer)
    return data