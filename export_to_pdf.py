import pandas as pd
from reportlab.lib.pagesizes import letter
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.pdfgen import canvas

pdfmetrics.registerFont(TTFont("FreeSans", "FreeSans.ttf"))


def create_index_pdf(df, pdf_filename):
    pdf = canvas.Canvas(pdf_filename, pagesize=letter)
    width, height = letter

    x = 50  # Horizontal position
    y = height - 50  # Vertical position, starting from top

    for index, row in df.iterrows():
        key = row[0]
        pdf.setFont("FreeSans", 14)
        pdf.drawString(x, y, f'{key}:')
        y -= 20  # Move to next line

        # Loop through each document
        for i in range(1, len(row)):
            if pd.notnull(row[i]):  # Check if value is not null
                doc = df.columns[i]
                value = row[i]
                pdf.setFont("FreeSans", 12)
                pdf.drawString(x + 10, y, f'{doc}: {value}')  # Indent doc details
                y -= 20  # Move to next line
        y -= 10  # Add extra space between keys

        # Check if we're running out of space and need a new page
        if y < 50:
            pdf.showPage()
            y = height - 50  # Reset y

    pdf.save()



def main():
    # Input CSV path from user
    print("HI")
    csv_path = "outputs/rashba_responsa_index_1.2.4.csv"

    # Load the CSV into a DataFrame
    df = pd.read_csv(csv_path, encoding="utf-8")

    # Generate PDF filename based on input CSV filename
    pdf_filename = csv_path.replace(".csv", "_index.pdf")

    # Generate the index PDF
    create_index_pdf(df, pdf_filename)

    print(f"Index PDF generated and saved as {pdf_filename}")

    # Generate LaTeX content from the index data
    latex_output = generate_index_latex(index_data)

    # Save the generated LaTeX content to a .tex file
    tex_filename = "/mnt/data/index_output.tex"
    with open(tex_filename, "w", encoding="utf-8") as tex_file:
        tex_file.write(latex_output)


if __name__ == "__main__":
    main()
