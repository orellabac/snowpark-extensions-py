def readpdf(filename:str):
    import PyPDF2
    import os
    import sys
    IMPORT_DIRECTORY_NAME = "snowflake_import_directory"
    import_dir = sys._xoptions[IMPORT_DIRECTORY_NAME]
    file = os.path.join(import_dir, filename)
    pdfFileObj = open(file, 'rb')
    pdfReader = PyPDF2.PdfFileReader(pdfFileObj)
    pageObj = pdfReader.getPage(0)
    page_content = pageObj.extractText()
    x=str(page_content)
    return x