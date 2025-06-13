from typing import Dict, List
import numpy as np
from transformers import AutoTokenizer, AutoModel
from rouge_score import rouge_scorer
from bert_score import BERTScorer
import torch
import re

"""
Module: benchmark.py (Jupyter Notebook Version)

This module provides functionality for evaluating the quality of scientific text summaries using a combination 
of ROUGE and BERTScore metrics, along with a novelty score based on n-gram analysis. The primary function, 
`evaluate_scientific_summary`, calculates these metrics and returns a comprehensive evaluation score.  This 
is designed for use within a Jupyter Notebook environment.
"""

class ScientificMetricsEvaluator:
    """
    Class: ScientificMetricsEvaluator

    This class encapsulates the logic for evaluating scientific text summaries using multiple metrics. It 
    initializes with pre-configured scoring models and tokenizers and offers methods to preprocess text, 
    calculate ROUGE scores, BERTScores, and a novelty score based on n-grams.  The results are combined to 
    provide a holistic evaluation score.
    """
    def __init__(self):
        """
        Method: __init__

        Initializes the evaluator by setting up ROUGE and BERTScore scoring mechanisms, and loading the 
        necessary tokenizer.  The BERTScore uses the 'adsabs/astroBERT' model, which is specifically 
        trained for astronomical and astrophysical literature.
        """
        self.rouge_scorer = rouge_scorer.RougeScorer(
            ['rouge1', 'rouge2', 'rougeL', 'rougeLsum'], #ROUGE types to calculate
            use_stemmer=True # Enables stemming to improve matching
        )
        
        self.bert_scorer = BERTScorer(
            model_type="adsabs/astroBERT", #Pre-trained model for scientific text
            num_layers=9,                  # Number of layers from the BERT model to use
            batch_size=32,                 # Batch size for BERTScore calculation
            nthreads=4,                    # Number of threads for parallel processing
            all_layers=False,              # Use only the final layer for scoring
            idf=False,                     # Don't use inverse document frequency weighting
            lang='en',                      # Language of the text
            rescale_with_baseline=False    # Don't rescale scores based on a baseline
        )
        
        self.tokenizer = AutoTokenizer.from_pretrained("adsabs/astroBERT") #Tokenizer for chunking
        self.max_length = 512             # Maximum sequence length for BERT
        self.overlap = 50                 # Overlap between chunks (in tokens)


    def _preprocess_text_rouge(self, text: str) -> str:
        """
        Method: _preprocess_text_rouge

        Preprocesses text for ROUGE scoring. Removes non-alphanumeric characters, except for whitespace and 
        common punctuation marks (.,!?-). Preserves whitespace to maintain sentence structure.

        Args:
            text (str): The input text.

        Returns:
            str: The preprocessed text.
        """
        text = re.sub(r'[^\w\s,.!?-]', '', text)
        return ' '.join(text.split()).strip()

    def _preprocess_text_bert(self, text: str) -> str:
        """
        Method: _preprocess_text_bert

        Performs minimal preprocessing for BERTScore.  Removes characters that are not alphanumeric or the 
        specified punctuation marks.

        Args:
            text (str): Input text.

        Returns:
            str: Preprocessed text.
        """
        return re.sub(r'[^\w,.!?-]', '', text)

    def _chunk_text(self, text: str) -> List[str]:
        """
        Method: _chunk_text

        Chunks a long text into smaller, overlapping sequences to accommodate the maximum sequence length 
        limit of the BERT model used in BERTScore.

        Args:
            text (str): The text to chunk.

        Returns:
            List[str]: A list of text chunks.
        """
        tokens = self.tokenizer.encode(text, add_special_tokens=False)
        chunks = []
        for i in range(0, len(tokens), self.max_length - self.overlap):
            chunk_tokens = tokens[i:i + self.max_length]
            chunk_tokens = [self.tokenizer.cls_token_id] + chunk_tokens + [self.tokenizer.sep_token_id]
            chunk = self.tokenizer.decode(chunk_tokens)
            chunks.append(chunk)
        return chunks

    def calculate_rouge(self, reference: str, candidate: str) -> Dict[str, float]:
        """
        Method: calculate_rouge

        Calculates ROUGE scores (rouge1, rouge2, rougeL, rougeLsum) between a reference and a candidate text.

        Args:
            reference (str): The reference text.
            candidate (str): The candidate text (e.g., a generated summary).

        Returns:
            Dict[str, float]: A dictionary of ROUGE scores.
        """
        reference = self._preprocess_text_rouge(reference)
        candidate = self._preprocess_text_rouge(candidate)
        scores = self.rouge_scorer.score(reference, candidate)
        return {k: v.fmeasure for k, v in scores.items()}

    def calculate_bertscore(self, reference: str, candidate: str) -> Dict[str, float]:
        """
        Method: calculate_bertscore

        Calculates BERTScore (precision, recall, F1) using the chunked text.  This method accounts for 
        the maximum sequence length constraints of the BERT model by dividing the text into overlapping 
        chunks before computing the BERTScore.  The scores are then averaged across all chunk pairs.

        Args:
            reference (str): The reference text.
            candidate (str): The candidate text.

        Returns:
            Dict[str, float]: A dictionary of BERTScore (precision, recall, f1).
        """
        reference = self._preprocess_text_bert(reference)
        candidate = self._preprocess_text_bert(candidate)
        reference_chunks = self._chunk_text(reference)
        candidate_chunks = self._chunk_text(candidate)
        chunk_scores = []
        for ref_chunk in reference_chunks:
            for cand_chunk in candidate_chunks:
                P, R, F1 = self.bert_scorer.score([cand_chunk], [ref_chunk])
                chunk_scores.append((float(P[0]), float(R[0]), float(F1[0])))
        if chunk_scores:
            avg_precision = np.mean([score[0] for score in chunk_scores])
            avg_recall = np.mean([score[1] for score in chunk_scores])
            avg_f1 = np.mean([score[2] for score in chunk_scores])
        else:
            avg_precision = avg_recall = avg_f1 = 0.0
        return {'precision': avg_precision, 'recall': avg_recall, 'f1': avg_f1}

    def calculate_ngram_novelty(self, reference: str, candidate: str) -> float:
        """
        Method: calculate_ngram_novelty

        Calculates a novelty score by determining the proportion of n-grams (unigrams, bigrams, trigrams) 
        in the candidate text that are not present in the reference text.  A higher novelty score indicates 
        that the candidate text contains more unique information relative to the reference.

        Args:
            reference (str): The reference text.
            candidate (str): The candidate text.

        Returns:
            float: The novelty score (average across unigrams, bigrams, and trigrams).
        """
        reference = self._preprocess_text_rouge(reference)
        candidate = self._preprocess_text_rouge(candidate)
        def get_ngrams(text, n):
            words = text.split()
            return set(' '.join(words[i:i+n]) for i in range(len(words)-n+1))
        novelty_scores = []
        for n in [1, 2, 3]:
            ref_ngrams = get_ngrams(reference, n)
            cand_ngrams = get_ngrams(candidate, n)
            novel_ngrams = cand_ngrams - ref_ngrams
            if cand_ngrams:
                novelty_scores.append(len(novel_ngrams) / len(cand_ngrams))
        return np.mean(novelty_scores) if novelty_scores else 0.0

    def evaluate_summary(self, reference: str, candidate: str) -> Dict[str, float]:
        """
        Method: evaluate_summary

        Computes all the evaluation metrics (ROUGE, BERTScore, and novelty) and combines them into a single 
        'abstractive_score'.  The weights are specifically chosen to favor summaries that are both semantically 
        similar to the reference and also offer new insights (high novelty).

        Args:
            reference (str): The reference text.
            candidate (str): The candidate summary text.

        Returns:
            Dict[str, float]: A dictionary containing all individual metric scores and the combined 
                              'abstractive_score'.
        """
        rouge_scores = self.calculate_rouge(reference, candidate)
        bert_scores = self.calculate_bertscore(reference, candidate)
        novelty_score = self.calculate_ngram_novelty(reference, candidate)
        
        final_scores = {
            'rouge1': rouge_scores['rouge1'],
            'rouge2': rouge_scores['rouge2'],
            'rougeL': rouge_scores['rougeL'],
            'rougeLsum': rouge_scores['rougeLsum'],
            'bertscore_precision': bert_scores['precision'],
            'bertscore_recall': bert_scores['recall'],
            'bertscore_f1': bert_scores['f1'],
            'ngram_novelty': novelty_score
        }
        
        # Weights optimized for abstractive summaries (high semantic similarity and high novelty)
        weights = {
            'bertscore_f1': 0.5,      # Semantic similarity
            'ngram_novelty': 0.5       # Novelty of information
        }
        
        final_scores['abstractive_score'] = sum(
            final_scores[metric] * weight for metric, weight in weights.items()
        )
        return final_scores

def evaluate_scientific_summary(reference_text: str, summary_text: str) -> Dict[str, float]:
    """
    Function: evaluate_scientific_summary

    A convenience function that wraps the `ScientificMetricsEvaluator` to make it easier to evaluate summaries.

    Args:
        reference_text (str): The original text.
        summary_text (str): The generated summary.

    Returns:
        Dict[str, float]: Dictionary of evaluation metrics.
    """
    evaluator = ScientificMetricsEvaluator()
    return evaluator.evaluate_summary(reference_text, summary_text)

# Example usage (within a Jupyter Notebook cell):
# scores = evaluate_scientific_summary(paper, summary)
# print(scores)
