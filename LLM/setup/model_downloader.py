import os
import json
from transformers import (
    AutoModelForCausalLM, 
    AutoModelForSeq2SeqLM, 
    AutoConfig,
    PreTrainedModel
)
from typing import Dict, Any, Union, Type
from pathlib import Path
from huggingface_hub import snapshot_download
import logging
import torch
import concurrent.futures
import pandas as pd

username = "ielhaime"
models = [
    "pszemraj/led-large-book-summary",
    "meta-llama/Llama-3.1-8B" 
    # Add more models here...
]

base_dir = f"/nobackup/{username}/models"

def download_models(models: list, base_dir: str) -> None:
    """
    Download models and save their configurations.
    """
    os.makedirs(base_dir, exist_ok=True)
    
    for model_name in models:
        model_dir = os.path.join(base_dir, model_name.split("/")[-1])
        os.makedirs(model_dir, exist_ok=True)
        
        # Download model
        snapshot_download(repo_id=model_name, local_dir=model_dir, cache_dir=model_dir)
        logging.info(f"Downloaded {model_name} to {model_dir}")

download_models(models,base_dir)

