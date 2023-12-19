#appraoch 2 with bytes and formatting

from docx import Document
from io import BytesIO

def find_and_replace_text(doc, target_text, replacement_text):
    locations = []

    # Iterate through paragraphs
    for i, paragraph in enumerate(doc.paragraphs):
        if target_text in paragraph.text:
            index = paragraph.text.find(target_text)
            locations.append(f"Paragraph {i + 1}, Index: {index}")
            for run in paragraph.runs:
                if target_text in run.text:
                    run.text = run.text.replace(target_text, replacement_text)

    # Iterate through tables, rows, and cells
    for i, table in enumerate(doc.tables):
        for j, row in enumerate(table.rows):
            for k, cell in enumerate(row.cells):
                for paragraph in cell.paragraphs:
                    if target_text in paragraph.text:
                        index = paragraph.text.find(target_text)
                        locations.append(f"Table {i + 1}, Row {j + 1}, Cell {k + 1}, Index: {index}")
                        for run in paragraph.runs:
                            if target_text in run.text:
                                run.text = run.text.replace(target_text, replacement_text)

    return locations

# Replace 'your_document.docx' with the actual path to your DOCX file
input_docx_file_path = '/home/axat/Downloads/Findal copy copy.docx'

# Open the existing DOCX file as bytes
with open(input_docx_file_path, 'rb') as file:
    docx_bytes = BytesIO(file.read())
    doc = Document(docx_bytes)

# Specify the text to find and replace
target_text = 'sumit'
replacement_text = 'xzat'

# Find the location of the text in the document and replace it
text_locations = find_and_replace_text(doc, target_text, replacement_text)

# Save the changes to the existing file
with open(input_docx_file_path, 'wb') as file:
    doc.save(file)

# Print the locations
if text_locations:
    print(f"Locations of '{target_text}':")
    for location in text_locations:
        print(location)
else:
    print(f"'{target_text}' not found in the document.")
