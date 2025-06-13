import pandas as pd
import ads
import operator
import re
import nltk
from nltk import ngrams, word_tokenize, bigrams, trigrams
from nltk.tokenize import word_tokenize 
from nltk.stem import WordNetLemmatizer, PorterStemmer, SnowballStemmer
import fnmatch


def stopword_loader(directorypath):
    '''
    Loads in stop word text file. 
    directorypath = the location and name of the stopword file (string)
    '''
    txt_file = open(directorypath, "r")
    file_content = txt_file.read()

    content_list = file_content.split("\n")
    txt_file.close()

    stop_words = content_list
    
    return stop_words

################################################################################################################################################################################################################################################################################################################################################################################################

def count_words(text):
    '''
    This function takes in a tokenized text (with each word a iteration in a list) and returns the occurrence of each word and the word itself in a dictionary. It can also be used on n-grams.
    '''
    word_counts = {}
    
    for word in text:
        if word in word_counts:
            word_counts[word] += 1
        else:
            word_counts[word] = 1
            
    return word_counts

################################################################################################################################################################################################################################################################################################################################################################################################

def topwords(abstract, directorypath):
    '''
    Takes in an abstract's text (a single long string) and determines the 10 most common words.
    '''
    stemmer = PorterStemmer()
    lemmer = WordNetLemmatizer()
    
    # Step 1: Load in necessary files.
    stop_words = stopword_loader(directorypath)
    
    # Step 2: Remove numbers.
    letters = re.sub("[^a-zA-Z]+", " ", abstract)
    
    # Step 3: Make it all lowercase.
    lower = letters.lower()
    
    # Step 4: Tokenize the abstract.
    tokenized = word_tokenize(lower)
    
    # Step 5: Remove punctuation and stop words.
    punctuation = [',', ':', ';', '.', "'", '"', '(', ')', '’', 'SUB', 'SUP', 'sub', 'sup', 'l&gt', 'l&lt', 'lt', 'gt', 'ch']
    filtered = [w for w in tokenized if not w in stop_words]
    filtered = [w for w in filtered if not w in punctuation]
    
    # Step 6: Lemm the words.
    lemmed = []
    for word in filtered:
        lemmed_word = lemmer.lemmatize(word)
        lemmed.append(lemmed_word)
        
    # Step 7: Count the words !
    counts = count_words(lemmed)
    
    # Step 8: Order the word counts from highest to lowest.
    sort = sorted(counts.items(), key = operator.itemgetter(1), reverse = True)
    
    # Step 9: Grabbing the top 10.
    top10 = sort[0:10]
    
    return top10

################################################################################################################################################################################################################################################################################################################################################################################################

def topbigrams(abstract, directorypath):
    '''
    Takes in an abstract's text (a single long string) and determines the 10 most common bigrams.
    '''
    stemmer = PorterStemmer()
    lemmer = WordNetLemmatizer()
    
    # Step 1: Load in necessary files.
    stop_words = stopword_loader(directorypath)
    
    # Step 2: Remove numbers
    letters = re.sub("[^a-zA-Z]+", " ", abstract)
    
    # Step 3: Make it all lowercase.
    lower = letters.lower()
    
    # Step 3: Tokenize the abstract.
    tokenized = word_tokenize(lower)
    
    # Step 4: Remove punctuation and stop words.
    punctuation = [',', ':', ';', '.', "'", '"', '(', ')', '’', 'SUB', 'SUP', 'sub', 'sup', 'l&gt', 'l&lt', 'lt', 'gt', 'ch']
    filtered = [w for w in tokenized if not w in stop_words]
    filtered = [w for w in filtered if not w in punctuation]
    
    # Step 5: Lemm the words.
    lemmed = []
    for word in filtered:
        lemmed_word = lemmer.lemmatize(word)
        lemmed.append(lemmed_word)
        
    # Step 6: Split into bigrams
    bigramss = list(bigrams(lemmed))
    
    # Step 7: Count the words !
    counts = count_words(bigramss)
    
    # Step 8: Order the word counts from highest to lowest.
    sort = sorted(counts.items(), key = operator.itemgetter(1), reverse = True)
    
    # Step 9: Grabbing the top 10.
    top10 = sort[0:10]
    
    return top10

################################################################################################################################################################################################################################################################################################################################################################################################

def toptrigrams(abstract, directorypath):
    '''
    Takes in an abstract's text (a single long string) and determines the 10 most common bigrams.
    '''
    
    stemmer = PorterStemmer()
    lemmer = WordNetLemmatizer()
    
    # Step 1: Load in necessary files.
    stop_words = stopword_loader(directorypath)
    
    # Step 2: Remove numbers
    letters = re.sub("[^a-zA-Z]+", " ", abstract)
    
    # Step 3: Make it all lowercase.
    lower = letters.lower()
    
    # Step 3: Tokenize the abstract.
    tokenized = word_tokenize(lower)
    
    # Step 4: Remove punctuation and stop words.
    punctuation = [',', ':', ';', '.', "'", '"', '(', ')', '’', 'SUB', 'SUP', 'sub', 'sup', 'l&gt', 'l&lt', 'lt', 'gt', 'ch']
    filtered = [w for w in tokenized if not w in stop_words]
    filtered = [w for w in filtered if not w in punctuation]
    
    # Step 5: Lemm the words.
    lemmed = []
    for word in filtered:
        lemmed_word = lemmer.lemmatize(word)
        lemmed.append(lemmed_word)
        
    # Step 6: Split into bigrams
    trigramss = list(trigrams(filtered))
    
    # Step 7: Count the words !
    counts = count_words(trigramss)
    
    # Step 8: Order the word counts from highest to lowest.
    sort = sorted(counts.items(), key = operator.itemgetter(1), reverse = True)
    
    # Step 9: Grabbing the top 10.
    top10 = sort[0:10]
    
    return top10

################################################################################################################################################################################################################################################################################################################################################################################################