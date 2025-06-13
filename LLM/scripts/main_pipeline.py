import pandas as pd
import ads  # Assumed to be the ADS library for astronomical data
import operator
import re
import nltk
from nltk import ngrams, word_tokenize, bigrams, trigrams
from nltk.tokenize import word_tokenize
from nltk.stem import WordNetLemmatizer, PorterStemmer, SnowballStemmer
import fnmatch
import requests
import io
from PyPDF2 import PdfReader
import os
import sys

# Add the parent directory of the current script's directory to sys.path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Now you can import modules from the sibling directory
from utils.text_analysis import stopword_loader, count_words, topwords, topbigrams, toptrigrams

from typing import List, Union
import logging
from marker.converters.pdf import PdfConverter
from marker.models import create_model_dict
from marker.output import text_from_rendered
from CasualLM import setup_model, extract_summary
from pathlib import Path


username = "skkamal"
# Directory Configuration (matching your existing structure)
CONTENT_DIR = Path(f"/nobackup/{username}/content")
SUMMARIES_DIR = Path(f"/nobackup/{username}/summaries")
MODELS_DIR = Path(f"/nobackup/{username}/models")
MARKER_DIR = Path(f"/nobackup/{username}/marker_markdown")  # New directory for Marker markdown

for directory in [CONTENT_DIR, SUMMARIES_DIR, MODELS_DIR, MARKER_DIR]:
    directory.mkdir(exist_ok=True)


"""
Module: main_pipeline.py

This module implements the main pipeline for processing a CSV file. It retrieves scientific papers based on 
arXiv identifiers from the CSV, generates summaries for each paper using either a seq2seq or causal language 
model (depending on configuration), concatenates the summaries for entries with multiple identifiers, performs 
n-gram analysis on the combined summaries, and writes the results to a new CSV file.  The summarization step 
relies on separate modules (`seq2seq.py` and `casualLM.py`).
"""

# Configure logging (good practice for larger applications)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
'''
def clean_text(text: str) -> str:
    """
    Function: clean_text

    Cleans and preprocesses the extracted text from a scientific paper.  Removes reference and acknowledgement 
    sections, text within brackets, and lines containing only single words or starting with numbers/symbols.

    Args:
        text (str): The raw text extracted from a scientific paper.

    Returns:
        str: The cleaned and preprocessed text.
    """
    logger.debug("Cleaning text...")
    keywords = ["REFERENCES", "ACKNOWLEDGEMENTS", "References", "Acknowledgements"]
    for keyword in keywords:
        pos = text.find(keyword)
        if pos != -1:
            text = text[:pos]
    
    text = re.sub(r'\([^)]*\)|\[[^\]]*\]', '', text)  # Remove bracketed content
    lines = text.split('\n')
    cleaned_lines = [
        line.strip() 
        for line in lines 
        if line.strip() and not re.match(r'^\s*\w+\s*$', line) and not re.match(r'^[\d\W].*$', line)
    ]
    return ' '.join(cleaned_lines)
    
    ef get_arxiv_text(arxiv_id: str) -> Union[str, None]:
    """
    Function: get_arxiv_text

    Downloads a PDF from arXiv given its ID, extracts the text, and cleans it using the clean_text function.
    Handles potential errors during download and PDF processing.

    Args:
        arxiv_id (str): The arXiv ID (e.g., "arXiv:1234.5678").

    Returns:
        Union[str, None]: The cleaned text extracted from the PDF, or None if an error occurs.
    """
    logger.info(f"Downloading and processing arXiv ID: {arxiv_id}")
    arxiv_id = arxiv_id.split('arXiv:')[-1]
    url = f'https://arxiv.org/pdf/{arxiv_id}.pdf'
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)
        pdf = PdfReader(io.BytesIO(response.content))
        text = ""
        for page in pdf.pages:
            text += page.extract_text() + "\n"
        return clean_text(text)
    except requests.exceptions.RequestException as e:
        logger.error(f"Error downloading PDF {url}: {e}")
        return None
    except Exception as e:
        logger.error(f"Error processing PDF {url}: {e}", exc_info=True)
        return None


def get_summary(arxiv_id: str) -> Union[str, None]:
    """
    Function: get_summary (Simplified)

    This function is a placeholder. In a real application, this would likely call a summarization function, 
    potentially using a language model as in the previous examples.  For this example, it simply calls 
    `get_arxiv_text`.


    Args:
        arxiv_id (str): The arXiv ID.

    Returns:
        Union[str, None]: The summary text, or None if an error occurs.  Currently just returns the raw text.
    """
    return get_arxiv_text(arxiv_id)
'''

def process_pdf_with_marker(pdf_path: Path) -> str:
    """
    Process a PDF using Marker instead of the previous clean_text function.
    
    Args:
        pdf_path (Path): Path to the PDF file
        
    Returns:
        str: Cleaned and processed text from the PDF
    """
    try:
        converter = PdfConverter(
            artifact_dict=create_model_dict(),
            config={
                "strip_existing_ocr": True,  # Remove existing OCR text
                "force_ocr": False,  # Only OCR if needed
            }
        )
        
        rendered = converter(str(pdf_path))
        text, _, _ = text_from_rendered(rendered)
        return text
    except Exception as e:
        logger.error(f"Error processing PDF with Marker: {str(e)}")
        return None


def process_csv(csv_filepath: str, stopwords_path: str) -> pd.DataFrame:
    """
    Function: process_csv

    The main function that processes a CSV file, downloads papers, generates summaries (using a placeholder 
    function), concatenates the summaries, performs n-gram analysis, and adds the results as new columns to the 
    DataFrame.  Handles various error conditions.

    Args:
        csv_filepath (str): Path to the input CSV file.  Must contain an 'Identifier' column with a list of 
                            arXiv IDs in string format (e.g., "['arXiv:1234.5678', 'arXiv:9876.5432']").
        stopwords_path (str): Path to the stopwords file for n-gram analysis.

    Returns:
        pd.DataFrame: The processed DataFrame with added columns for summaries and n-grams.  Returns None if 
                      errors occur during processing.
    """
    try:
        df = pd.read_csv(csv_filepath)
        df['summaries'] = ""
        df['topwords'] = ""
        df['topbigrams'] = ""
        df['toptrigrams'] = ""
        llm_chain = setup_model()
        stopwords = stopword_loader(stopwords_path)

        for index, row in df.iterrows():
            arxiv_ids_str = row['Identifier']
            try:
                arxiv_ids = eval(arxiv_ids_str)
                all_summaries = []
                for arxiv_id in arxiv_ids:
                    pdf_path = Path("/nobackup/skkamal/papers") / f"{arxiv_id.split('arXiv:')[-1]}.pdf"
                    
                    if pdf_path and pdf_path.exists():
                        processed_text = process_pdf_with_marker(pdf_path)
                        if processed_text:
                            marker_text_path = MARKER_DIR / f"MARKER_{arxiv_id}.md"
                            with open(marker_text_path, 'w', encoding='utf-8') as marker_file:
                                marker_file.write(processed_text)
                            summary_output = llm_chain.run(processed_text)
                            summary = extract_summary(summary_output)
                            all_summaries.append(summary)
                            summary_path = SUMMARIES_DIR / f"SUM_{arxiv_id}.txt"
                            with open(summary_path, 'w', encoding='utf-8') as f:
                                f.write(summary)
                    else:
                        logger.warning(f"Failed to find PDF for {arxiv_id}")

                if all_summaries:  # If we have any summaries
                    df.loc[index, 'summaries'] = all_summaries
                    
                    combined_text = ' '.join(all_summaries)
                    top10words = topwords(combined_text, stopwords)
                    top10bigrams = topbigrams(combined_text, stopwords)
                    top10trigrams = toptrigrams(combined_text, stopwords)
                    
                    df.loc[index, ['topwords', 'topbigrams', 'toptrigrams']] = [
                        top10words, top10bigrams, top10trigrams
                    ]
                else:
                    df.loc[index, ['summaries', 'topwords', 'topbigrams', 'toptrigrams']] = [None] * 4
            except (SyntaxError, NameError, TypeError) as e:
                logger.error(f"Error processing row {index}: {e}", exc_info=True)
                df.loc[index, ['summaries', 'topwords', 'topbigrams', 'toptrigrams']] = [None] * 4

        return df
    except FileNotFoundError:
        logger.error(f"CSV file not found: {csv_filepath}")
        return None
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}", exc_info=True)
        return None


if __name__ == "__main__":
    nltk.download('punkt')
    nltk.download('wordnet')
    
    csv_file = './data/small_identifier_sample.csv'  # Replace with your CSV file
    stopwords_file = './data/stopwords.txt' # Path to your stopword file

    processed_df = process_csv(csv_file, stopwords_file)

    if processed_df is not None:
        output_filename = './data/combined_output.csv'
        processed_df.to_csv(output_filename, index=False)
        logger.info(f"Results saved to {output_filename}")
    else:
        logger.error("CSV processing failed.")

