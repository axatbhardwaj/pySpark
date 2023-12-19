from docx import Document
from io import BytesIO

def replace_text_in_docx(doc, target_text, replacement_text):
    for paragraph in doc.paragraphs:
        if target_text in paragraph.text:
            paragraph.text = paragraph.text.replace(target_text, replacement_text)

    for table in doc.tables:
        for row in table.rows:
            for cell in row.cells:
                if target_text in cell.text:
                    cell.text = cell.text.replace(target_text, replacement_text)

# Replace 'your_document.docx' with the actual path to your DOCX file
input_docx_file_path = '/home/axat/Downloads/Findal copy.docx'

# Open the existing DOCX file
with open(input_docx_file_path, 'rb') as file:
    docx_stream = BytesIO(file.read())
    doc = Document(docx_stream)

# Specify the text to be replaced and the replacement text
target_text = 'Lorem'
replacement_text = 'sumit'

# Use the function to replace the text in the original DOCX document
replace_text_in_docx(doc, target_text, replacement_text)

# Save the changes to the existing file
doc.save(input_docx_file_path)


