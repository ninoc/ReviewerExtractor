# Expertise Finder v3.0: Scientific Paper Summarization Pipeline

## Overview

This system processes a CSV file containing author publication information, downloads full paper texts, generates summaries, performs n-gram analysis, and outputs comprehensive results.

## Prerequisites

- Python 3.x
- pip package manager

## Setup Instructions

### 1. Install Required Packages

Install dependencies using the provided `requirements.txt`:

```bash
pip install -r requirements.txt
```

Download NLTK resources:
```python
import nltk
nltk.download('punkt')
nltk.download('wordnet')
```

### 2. Download Pre-trained Models

Edit `model_downloader.py`:
```python
username = "your_username"  # Replace with your username
base_dir = f"/nobackup/{username}/models"
models = [
    "pszemraj/led-large-book-summary",
    "meta-llama/Llama-3.1-8B",
    # Add other model IDs here
]
```

Run the downloader:
```bash
python model_downloader.py
```

### 3. Prepare Input Data

Requirements for input CSV:
- Must contain an "Identifier" column
- Identifier column should contain lists of arXiv IDs
- Use single quotes for arXiv IDs (e.g., `['arXiv:2310.12345']`)

Create `stopwords.txt` with words to exclude from n-gram analysis.

### 4. Configure Model in Jupyter Notebook

In `main_pipeline.ipynb`, directly pass model parameters to the `generate_summary()` function:

```python
# Define paths to your downloaded model and its configuration
model_path = "./models/led-large-book-summary"
config_path = "./model_configs/led-large-book-summary_config.json"

# Prepare the list of papers to summarize
papers = [paper1_text, paper2_text, ...]

# Generate summaries by passing model paths and papers directly
summary = generate_summary(
    model_path=model_path, 
    config_path=config_path, 
    papers=papers
)
```

**Key Parameters:**
- `model_path`: Full path to the downloaded model directory
- `config_path`: Full path to the model's configuration JSON file
- `papers`: List of paper texts to summarize

**Important Considerations:**
- Ensure the paths point to the correct model and configuration files
- The function expects a list of paper texts as input
- Different model types (seq2seq vs. causal) may require different handling in the `generate_summary()` function

### 5. Run the Pipeline

Execute all cells in `main_pipeline.ipynb`.

## Output

The script generates `combined_output.csv` with:
- Original input data
- `summaries`: Concatenated paper summaries
- `topwords`: Top 10 unigrams
- `topbigrams`: Top 10 bigrams
- `toptrigrams`: Top 10 trigrams

## Notes

- Download and processing may take significant time
- Ensure sufficient disk space for model downloads
- Monitor notebook output for progress and errors

## Troubleshooting

- Verify file paths
- Check model download completeness
- Ensure NLTK resources are correctly installed