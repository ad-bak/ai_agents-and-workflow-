import json
import sys
import os
import sqlite3
from typing import List, Optional

# Run "uv sync" to install the below packages
from pypdf import PdfReader
from dotenv import load_dotenv
from openai import OpenAI
from pydantic import BaseModel, Field

load_dotenv()

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))


class DateInfo(BaseModel):
    date: str
    description: str


class AmountInfo(BaseModel):
    value: float
    currency: Optional[str] = None
    description: str


class DocumentData(BaseModel):
    document_type: str = Field(description="The type or category of the document")
    people: List[str] = Field(default_factory=list, description="Names of people found in the document")
    organizations: List[str] = Field(default_factory=list, description="Organizations mentioned in the document")
    locations: List[str] = Field(default_factory=list, description="Locations mentioned in the document")
    dates: List[DateInfo] = Field(default_factory=list, description="Important dates found in the document")
    amounts: List[AmountInfo] = Field(default_factory=list, description="Monetary amounts or numerical values")
    phone_numbers: List[str] = Field(default_factory=list, description="Phone numbers found in the document")
    emails: List[str] = Field(default_factory=list, description="Email addresses found in the document")
    summary: str = Field(description="Brief summary of the document content")


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


def extract_data_from_pdf(pdf_content: str, filename: str) -> DocumentData:
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
    """
    
    completion = client.beta.chat.completions.parse(
        model="gpt-4o-mini",
        messages=[
            {"role": "user", "content": prompt}
        ],
        response_format=DocumentData,
    )
    
    return completion.choices[0].message.parsed


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
            document_type = document_details.document_type
            insert_document_data(conn, pdf_file, document_type, document_details.model_dump())
            print("Extracted Document Details:")
            print(json.dumps(document_details.model_dump(), indent=2))
            print("---------")
        except Exception as e:
            print(f"An error occurred while processing {pdf_file}: {e}")

    conn.close()


if __name__ == "__main__":
    main()
