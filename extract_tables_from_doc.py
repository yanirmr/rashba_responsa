import os
import csv
import sys
import glob
import win32com.client
from docx import Document


def doc_to_docx(doc_file, docx_file):
    # Initialize Word.Application
    word = win32com.client.Dispatch("Word.Application")
    word.Visible = False

    # Open the .doc file and save it as .docx
    doc = word.Documents.Open(doc_file)
    doc.SaveAs(docx_file, FileFormat=16)  # 16 = wdFormatDocumentDefault
    doc.Close()

    # Close Word
    word.Quit()


def docx_table_to_csv(docx_file, output_folder):
    # Load the document
    doc = Document(docx_file)

    # Iterate through tables in the document
    for i, table in enumerate(doc.tables):
        # Prepare the output CSV file
        output_file = os.path.join(output_folder, f"{os.path.splitext(os.path.basename(docx_file))[0]}_table_{i}.csv")

        # Write table data to the CSV file
        with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)

            for row in table.rows:
                writer.writerow([cell.text for cell in row.cells])


# Set input folder and output folder
input_folder = "doc_files"
output_folder = "csv_files"

# Check if running on Windows
if sys.platform != "win32":
    print("This script requires the win32com library, which is only available on Windows.")
    sys.exit(1)

# Process each Word file in the input folder
for filename in os.listdir(input_folder):
    if filename.endswith(".doc"):
        current_dir = os.getcwd()

        doc_file = os.path.join(current_dir, input_folder, filename)
        docx_file = os.path.join(current_dir, input_folder, f"{os.path.splitext(filename)[0]}.docx")

        # Convert .doc to .docx
        print(doc_file)
        doc_to_docx(doc_file, docx_file)

        # Extract tables to CSV files
        docx_table_to_csv(docx_file, output_folder)

        # Optionally, remove the .docx file after processing
    #    os.remove(docx_file)
