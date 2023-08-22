from collections import defaultdict

import pandas as pd


def extract_index_data_from_csv(csv_path):
    # Load the CSV into a DataFrame
    df = pd.read_csv(csv_path, encoding="utf-8")

    # Generate the index data
    index_data = defaultdict(list)
    headers = df.columns.tolist()
    for _, row in df.iterrows():
        topic = row["דפוס"]
        for i, ref in enumerate(row[2:]):
            if pd.notna(ref):  # If there's a reference in the cell
                index_data[topic].append((headers[i + 2], ref.strip()))

    return index_data


def generate_index_latex(data):
    # LaTeX document setup
    latex_content = [
        "\\documentclass{article}",
        "\\usepackage[utf8]{inputenc}",
        "\\usepackage[english,hebrew]{babel}",  # Support for Hebrew
        "\\usepackage{parskip}",  # Add spaces between paragraphs
        "\\begin{document}",
    ]

    # Convert index data to LaTeX content
    for topic, references in data.items():
        latex_content.append(f"\\textbf{{{topic}}}:")
        for book, ref in references:
            latex_content.append(f"\\hspace{{1em}}{book}: {ref} \\\\")
        latex_content.append("")  # Add space between topics

    # End the LaTeX document
    latex_content.append("\\end{document}")
    return "\n".join(latex_content)


def main():
    # Input CSV path from user
    print("HI")
    csv_path = "outputs/rashba_responsa_index_1.2.4.csv"

    # Extract index data from the CSV
    data = extract_index_data_from_csv(csv_path)

    # Generate LaTeX content from the index data
    latex_output = generate_index_latex(data)

    # Save the generated LaTeX content to a .tex file
    tex_filename = csv_path.replace(".csv", "_index.tex")
    with open(tex_filename, "w", encoding="utf-8") as tex_file:
        tex_file.write(latex_output)

    print(f"LaTeX file generated and saved as {tex_filename}")


if __name__ == "__main__":
    main()
