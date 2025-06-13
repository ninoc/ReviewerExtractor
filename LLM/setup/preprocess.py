import os
import sys
import logging
import requests
import pandas as pd
from pathlib import Path
from typing import List
from marker.models import create_model_dict
from huggingface_hub import snapshot_download

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/nobackup/skkamal/logs/preprocess.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Directory Configuration
username = "skkamal"
CONTENT_DIR = Path(f"/nobackup/{username}/content")
SUMMARIES_DIR = Path(f"/nobackup/{username}/summaries")
MODELS_DIR = Path(f"/nobackup/{username}/models")
MARKER_DIR = Path(f"/nobackup/{username}/marker_markdown")
PAPERS_DIR = Path(f"/nobackup/{username}/papers")

def setup_directories():
    """Create all necessary directories if they don't exist."""
    for directory in [CONTENT_DIR, SUMMARIES_DIR, MODELS_DIR, MARKER_DIR, PAPERS_DIR]:
        directory.mkdir(exist_ok=True, parents=True)
        logger.info(f"Ensured directory exists: {directory}")

def download_arxiv_pdf(arxiv_id: str) -> Path:
    """Download a PDF from arXiv and save it to PAPERS_DIR."""
    arxiv_id = arxiv_id.split('arXiv:')[-1]
    pdf_path = PAPERS_DIR / f"{arxiv_id}.pdf"
    
    if pdf_path.exists():
        logger.info(f"PDF already exists: {pdf_path}")
        return pdf_path
        
    url = f'https://arxiv.org/pdf/{arxiv_id}.pdf'
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()
        
        with open(pdf_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        logger.info(f"Successfully downloaded: {pdf_path}")
        return pdf_path
    except Exception as e:
        logger.error(f"Error downloading PDF {url}: {e}")
        return None

def download_pdfs_from_csv(csv_filepath: str) -> None:
    """Download all PDFs listed in the CSV file."""
    try:
        df = pd.read_csv(csv_filepath)
        for index, row in df.iterrows():
            arxiv_ids_str = row['Identifier']
            try:
                arxiv_ids = eval(arxiv_ids_str)
                for arxiv_id in arxiv_ids:
                    download_arxiv_pdf(arxiv_id)
            except (SyntaxError, NameError, TypeError) as e:
                logger.error(f"Error processing row {index}: {e}")
    except Exception as e:
        logger.error(f"Error reading CSV file: {e}")

def download_marker_model():
    """Download and initialize the Marker model."""
    try:
        logger.info("Downloading Marker model...")
        # This will download the model and cache it
        create_model_dict()
        logger.info("Marker model downloaded successfully")
    except Exception as e:
        logger.error(f"Error downloading Marker model: {e}")

def main():
    """Main function to run all preprocessing steps."""
    setup_directories()
    
    # Download Marker model
    download_marker_model()
    
    # Download PDFs if CSV file is provided
    if len(sys.argv) > 1:
        csv_file = sys.argv[1]
        logger.info(f"Processing CSV file: {csv_file}")
        download_pdfs_from_csv(csv_file)
    else:
        logger.info("No CSV file provided. Skipping PDF downloads.")

if __name__ == "__main__":
    main() 
