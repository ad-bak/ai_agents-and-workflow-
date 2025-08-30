import json
import sys
import os
import sqlite3

# Run "uv sync" to install the below packages
from pypdf import PdfReader
from dotenv import load_dotenv
import requests

load_dotenv()

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")


def setup_database():
    conn = sqlite3.connect("documents.db")
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS documents (
            id INTEGER PRIMARY KEY,
            filename TEXT,
            document_type TEXT,
            extracted_data TEXT,
            processed_date TEXT
        )
    """)
    conn.commit()
    return conn


def insert_document_data(conn, filename, document_type, extracted_data):
    from datetime import datetime
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO documents (
            filename, document_type, extracted_data, processed_date
        ) VALUES (?, ?, ?, ?)
    """,
        (
            filename,
            document_type,
            json.dumps(extracted_data),
            datetime.now().isoformat()
        ),
    )
    conn.commit()


def get_pdf_content(pdf_path: str) -> str:
    with open(pdf_path, "rb") as f:
        reader = PdfReader(f)
        text = ""
        for page in reader.pages:
            text += page.extract_text()
    return text


def extract_data_from_pdf(pdf_content: str, filename: str) -> dict:
    document_type = filename.split('.')[-2].split('/')[-1] if '/' in filename else filename.split('.')[0]
    
    prompt = f"""
    You are an expert data extractor who excels at analyzing documents.

    Extract all relevant data from the below document (which was extracted from a PDF document).
    The document appears to be: {document_type}
    
    Make sure to capture all important information including but not limited to:
    - Names, addresses, dates, amounts, numbers
    - Key entities, organizations, people
    - Important metadata and classifications
    - Any structured data present

    <content>
    {pdf_content}
    </content>

    Return your response as a JSON object without any extra text or explanation.
"""
    response = requests.post(
        "https://api.openai.com/v1/responses",
        json={
            "model": "gpt-4o-mini",
            "input": prompt,
            "text": {
                "format": {
                    "name": "document",
                    "type": "json_schema",
                    "schema": {
                        "type": "object",
                        "properties": {
                            "document_type": {
                                "type": "string",
                                "description": "The type or category of the document",
                            },
                            "entities": {
                                "type": "object",
                                "description": "Key entities found in the document (people, organizations, locations)",
                                "additionalProperties": True,
                            },
                            "dates": {
                                "type": "array",
                                "description": "Important dates found in the document",
                                "items": {
                                    "type": "object",
                                    "properties": {
                                        "date": {"type": "string"},
                                        "description": {"type": "string"}
                                    }
                                }
                            },
                            "amounts": {
                                "type": "array",
                                "description": "Monetary amounts or numerical values",
                                "items": {
                                    "type": "object",
                                    "properties": {
                                        "value": {"type": "number"},
                                        "currency": {"type": "string"},
                                        "description": {"type": "string"}
                                    }
                                }
                            },
                            "key_information": {
                                "type": "object",
                                "description": "Other important structured data from the document",
                                "additionalProperties": True,
                            },
                            "summary": {
                                "type": "string",
                                "description": "Brief summary of the document content",
                            },
                        },
                        "additionalProperties": True,
                        "required": ["document_type", "summary"],
                    },
                    "strict": False,
                },
            },
        },
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {OPENAI_API_KEY}",
        },
    )

    response.raise_for_status()
    received_json = (
        response.json().get("output", [{}])[0].get("content", [{}])[0].get("text", "{}")
    )
    return json.loads(received_json)


def main():
    if len(sys.argv) < 2:
        print("Usage: python main.py pdfs/document.pdf")
        return

    path = sys.argv[1]
    pdf_files = []

    if not os.path.exists(path):
        print(f"Error: The path '{path}' does not exist.")
        return

    if os.path.isfile(path):
        if path.lower().endswith(".pdf"):
            pdf_files.append(path)
        else:
            print(f"Error: The file '{path}' is not a PDF file.")
            return
    elif os.path.isdir(path):
        for filename in os.listdir(path):
            if filename.lower().endswith(".pdf"):
                pdf_files.append(os.path.join(path, filename))

    if not pdf_files:
        print("No PDF files found.")
        return

    conn = setup_database()

    for pdf_file in pdf_files:
        print(f"Processing {pdf_file}...")
        try:
            pdf_content = get_pdf_content(pdf_file)
            document_details = extract_data_from_pdf(pdf_content, pdf_file)
            document_type = document_details.get("document_type", "unknown")
            insert_document_data(conn, pdf_file, document_type, document_details)
            print("Extracted Document Details:")
            print(json.dumps(document_details, indent=2))
            print("---------")
        except Exception as e:
            print(f"An error occurred while processing {pdf_file}: {e}")

    conn.close()


if __name__ == "__main__":
    main()
