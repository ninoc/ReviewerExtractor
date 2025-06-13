import torch
import pandas as pd
import os
from pathlib import Path
from transformers import (
    AutoTokenizer, 
    AutoModelForCausalLM, 
    pipeline
)
import sys
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)
import logging
import utils.text_analysis as TA # Assumed to contain n-gram functions
from langchain.chains import LLMChain
from langchain.llms.huggingface_pipeline import HuggingFacePipeline
from langchain.prompts import PromptTemplate

from typing import Union, List,Dict

"""
Module: ADS_CasualLM.ipynb

This Jupyter Notebook-style Python script demonstrates a text summarization application. It takes text files 
from a specified directory, generates summaries using a causal language model (LLM), and performs n-gram 
analysis on the generated summaries. The script uses the Hugging Face Transformers library for model loading, 
Langchain for LLM interaction and prompt management, and a custom TextAnalysis module (not shown here) for 
n-gram extraction.  The script features extensive logging for monitoring progress and handling errors.
"""
def setup_logging():
    """
    Function: setup_logging

    Configures the logging system to output log messages with timestamps to both the console and a file 
    ('model_processing.log').  This is crucial for monitoring the script's execution and identifying potential 
    problems.

    Returns:
        logging.Logger: The configured logger instance.
    """
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('/nobackup/skkamal/logs/model_processing.log'),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

def setup_model(model_path: str = "/nobackup/{username}/models/Llama-3.1-8B") -> LLMChain:
    """
    Function: setup_model

    Loads a causal language model (LLM) for text summarization.  This function handles loading the tokenizer, 
    the model itself, creating a text generation pipeline, and configuring a Langchain LLMChain for prompt 
    management.  It prioritizes loading the model at full precision (no quantization).  It automatically 
    selects the appropriate device (GPU if available, otherwise CPU).

    Args:
        model_path (str, optional): The path to the pre-trained LLM model directory. Defaults to 
                                    "/nobackup/{username}/models/Llama-3.1-8B".

    Returns:
        LLMChain: A Langchain LLMChain object, ready to use for generating summaries.  This object encapsulates 
                   the model, tokenizer, and prompt template, providing a convenient interface.

    Raises:
        Exception: Any exceptions during model loading or pipeline setup are caught, logged, and re-raised.
    """
    logger = logging.getLogger(__name__)
    logger.info(f"Initializing full precision model from path: {model_path}")

    try:
        tokenizer = AutoTokenizer.from_pretrained(model_path)
        tokenizer.pad_token = tokenizer.eos_token #Ensure pad token is set

        model = AutoModelForCausalLM.from_pretrained(
            model_path,
            torch_dtype=torch.float16  # Use float16 for memory efficiency
        )

        device = torch.device("cuda:0" if torch.cuda.is_available() else "cpu")
        logger.info(f"Using device: {device}")
        model.to(device)

        pipe = pipeline(
            "text-generation",
            model=model,
            tokenizer=tokenizer,
            torch_dtype=torch.float16,
            trust_remote_code=True,
            max_new_tokens=8192,  
            do_sample=True,
            top_k=10,
            num_return_sequences=1,
            eos_token_id=tokenizer.eos_token_id
        )
        
        llm = HuggingFacePipeline(pipeline=pipe, model_kwargs={'temperature': 0.3})

        template = """
        Write a summary of the following text delimited by triple backticks.
        Return your response which covers the key points of the text.
        ```{text}```
        SUMMARY:
        """
        prompt = PromptTemplate(template=template, input_variables=["text"])
        llm_chain = LLMChain(prompt=prompt, llm=llm)

        return llm_chain

    except Exception as e:
        logger.error(f"Error in setup_model: {str(e)}", exc_info=True)
        raise


def extract_summary(llm_chain_output: Dict[str, str]) -> str:
    """
    Function: extract_summary

    Extracts the generated summary from the output of the LLM chain. The summary is expected to be marked 
    by "SUMMARY:" in the output text.

    Args:
        llm_chain_output (dict): The output dictionary from the LLMChain.run() method.

    Returns:
        str: The extracted summary text.
    """
    if isinstance(llm_chain_output, dict):
        full_output = llm_chain_output.get('text', '')
    else:
        full_output = llm_chain_output
    summary_parts = full_output.split('SUMMARY:')
    if len(summary_parts) > 1:
        return summary_parts[1].strip()
    return full_output.strip()
def read_text_file(file_path: str) -> Union[str, None]:
    """
    Function: read_text_file

    Reads the content of a text file.  Handles potential UnicodeDecodeErrors by trying both UTF-8 and 
    latin-1 encodings.

    Args:
        file_path (str): The path to the text file.

    Returns:
        Union[str, None]: The file's content as a string, or None if the file cannot be read.
    """
    logger = logging.getLogger(__name__)
    logger.info(f"Attempting to read file: {file_path}")
    
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            content = file.read()
            logger.info(f"Successfully read file with UTF-8 encoding: {file_path}")
            return content
    except UnicodeDecodeError:
        logger.warning(f"UTF-8 decode failed for {file_path}, attempting with latin-1")
        try:
            with open(file_path, 'r', encoding='latin-1') as file:
                content = file.read()
                logger.info(f"Successfully read file with latin-1 encoding: {file_path}")
                return content
        except Exception as e:
            logger.error(f"Failed to read file with latin-1 encoding: {file_path}", exc_info=True)
            return None
    except Exception as e:
        logger.error(f"Error reading file {file_path}: {str(e)}", exc_info=True)
        return None


def process_directory(llm_chain: LLMChain, directory_path: str = 'content') -> Union[pd.DataFrame, None]:
    """
    Function: process_directory

    Processes all '.txt' files in the specified directory, generates summaries for each using the provided 
    LLMChain, and returns the results as a Pandas DataFrame.  It handles potential errors during file 
    reading and summary generation.

    Args:
        llm_chain (LLMChain): The Langchain LLMChain object used for summary generation.
        directory_path (str, optional): The path to the directory containing the text files. Defaults to 'content'.

    Returns:
        Union[pd.DataFrame, None]: A Pandas DataFrame containing the filenames and their generated summaries, 
                                    or None if no text files are found or an error occurs.
    """
    logger = logging.getLogger(__name__)
    logger.info(f"Processing directory: {directory_path}")
    
    Path(directory_path).mkdir(exist_ok=True)
    txt_files = [f for f in os.listdir(directory_path) if f.endswith('.txt')]
    
    if not txt_files:
        logger.warning(f"No text files found in {directory_path}")
        return None
    
    logger.info(f"Found {len(txt_files)} text files to process")
    summaries = []
    
    for file_name in txt_files:
        file_path = os.path.join(directory_path, file_name)
        logger.info(f"Processing file: {file_name}")
        
        content = read_text_file(file_path)
        if content:
            try:
                logger.debug(f"Generating summary for {file_name}")
                summary_output = llm_chain.run(content)
                summary = extract_summary(summary_output)
                summaries.append({
                    'file_name': file_name,
                    'Summary': summary
                })
                logger.info(f"Successfully processed {file_name}")
            except Exception as e:
                logger.error(f"Error processing {file_name}: {str(e)}", exc_info=True)
        else:
            logger.error(f"Could not read content from {file_name}")
    
    return pd.DataFrame(summaries) if summaries else None
if __name__ == "__main__":
    logger = setup_logging()
    logger.info("Starting text processing application")

    try:
        stopwords_file = "stopwords.txt" #Path to your stopwords file
        llm_chain = setup_model()
        df = process_directory(llm_chain)

        if df is not None:
            df = process_summaries(df, stopwords_file)
            output_path = './LLM/summaries/meta_summaries.csv'
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            df.to_csv(output_path, index=False)
            logger.info(f"Successfully processed {len(df)} files and saved to {output_path}")

    except Exception as e:
        logger.exception("Fatal error in main execution") #Log the full traceback
    finally:
        torch.cuda.empty_cache()
        logger.info("Application completed.")
