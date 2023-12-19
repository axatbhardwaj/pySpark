
import fitz
import re

# Define a regex pattern for identifying URLs
url_pattern = re.compile(
    r'https?://[^\s/$.?#].[^\s]*',
    re.IGNORECASE
)

def mask_sensitive_data(page, text_instances):
    for inst in text_instances:
        # Create a rectangle to cover the sensitive text
        rect = fitz.Rect(inst[:4])
        # You can adjust the color and opacity as needed
        page.draw_rect(rect, color=(0, 0, 0), fill=(0, 0, 0), overlay=True)

def process_pdf(file_path):
    doc = fitz.open(file_path)
    new_doc = fitz.open()

    for page_num in range(doc.page_count):
        page = doc[page_num]
        text = page.get_text()

        # Find instances of sensitive data (URLs in this case)
        sensitive_data_instances = re.finditer(url_pattern, text)

        for match in sensitive_data_instances:
            # Mask each instance
            mask_sensitive_data(page, page.search_for(match.group()))
            
            # Print the matched URL to the console
            print(f"Page {page_num + 1}, Matched URL: {match.group()}")

        # Add the current page to the new document
        new_page = new_doc.new_page(width=page.rect.width, height=page.rect.height)
        new_page.show_pdf_page(page.rect, doc, page_num)

    new_file_path = file_path.replace('.pdf', '_masked.pdf')
    new_doc.save(new_file_path)
    new_doc.close()
    doc.close()

    print(f"Masked PDF saved as {new_file_path}")

file_path = '/home/axat/personal/pySpark/data/blockout.pdf'
process_pdf(file_path)

