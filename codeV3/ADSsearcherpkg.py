import requests
from urllib.parse import urlencode
import numpy as np
import TextAnalysis as TA
import itertools
from prompt_toolkit.shortcuts import prompt
from prompt_toolkit.key_binding import KeyBindings
from prompt_toolkit.completion import WordCompleter
from prompt_toolkit.auto_suggest import AutoSuggestFromHistory
from LlamaModelV2 import generate_expertise, get_groq, string_to_list
from rapidfuzz import process, fuzz
import time

import pandas as pd


def do_search(auth_name, inst, t, q):
    """
    Runs ADS search based on specified query built in ads_search.
    
    Returns a dataframe with the results of the search for a given author or institution.
    """
    results = requests.get(
        "https://api.adsabs.harvard.edu/v1/search/query?{}".format(q),
        headers={'Authorization': 'Bearer ' + t}
    )
    json_data = results.json()
    if "response" not in json_data:
        print("ADS API error:", json_data)
        return pd.DataFrame()
    data = results.json()["response"]["docs"]
    pdates = [d['pubdate'] for d in data]
    affiliations = [d['aff'][0] for d in data]
    bibcodes = [d['bibcode'] for d in data]
    f_auth = [d['first_author'] for d in data]
    keysw = [d.get('keyword', []) for d in data]
    titles = [d.get('title', '') for d in data]
    abstracts = [d.get('abstract', '') for d in data]
    ids = [d.get('identifier', []) for d in data]

    df = pd.DataFrame({
        'Input Author': [auth_name] * len(data),
        'Input Institution': [inst] * len(data),
        'First Author': f_auth,
        'Bibcode': bibcodes,
        'Title': titles,
        'Publication Date': pdates,
        'Keywords': keysw,
        'Affiliations': affiliations,
        'Abstract': abstracts,
        'Identifier': ids,
        'Data Type': ['']*len(data)
    })

    if auth_name is None:
        df['Input Author'] = f_auth
    return df

def format_year(year):
    if isinstance(year, (int, np.integer)):
        startd = str(year - 1)
        endd = str(year + 4)
        return f'[{startd} TO {endd}]'
    elif isinstance(year, float):
        year = int(year)
        startd = str(year - 1)
        endd = str(year + 4)
        return f'[{startd} TO {endd}]'
    elif isinstance(year, str):
        if len(year) == 4:
            startd = str(int(year) - 1)
            endd = str(int(year) + 4)
            return f'[{startd} TO {endd}]'
        elif year.startswith("[") and year.endswith("]") and " TO " in year:
            return year  # Return the string as is if it's a year range
        else:
            return year
    else:
        raise ValueError("Year must be an integer, float, or a string representing a year or a year range.")

def get_similar_institution(original, institutions_file="exceptions.xlsx", threshold=75):
    """
    Given an original institution name and a file containing exception mappings,
    first checks if the faulty name is present in the 'replace' column.
    If found, returns the corresponding value from the 'replacement' column.
    Otherwise, returns the most similar name from the 'replacement' column if the
    similarity score meets the threshold.
    
    The file can be an Excel or CSV file.
    """
    try:
        if institutions_file.lower().endswith(('.xlsx', '.xls')):
            df = pd.read_excel(institutions_file)
        else:
            df = pd.read_csv(institutions_file)
        exact_matches = df[df["replace"].astype(str).str.lower() == original.lower()]
        if not exact_matches.empty:
            return exact_matches.iloc[0]["replacement"]
        correct_names = df["replacement"].dropna().tolist()
        best_match, score, _ = process.extractOne(original, correct_names, scorer=fuzz.ratio)
        if score >= threshold:
            return best_match
        else:
            return None
    except Exception as e:
        print("Error reading similar institution file:", e)
        return None



def ads_search(name=None, institution=None, year=None, refereed='property:notrefereed OR property:refereed', \
               token=None, stop_dir=None, second_auth=False,groq_analysis=False,deep_dive=False):
    """
    Builds a query for ADS search based on name, institution, year, second_author. Merges all results and optionally runs groq
    subtopics analysis on the results. 
    
    Returns a dictionary with all authors and corresponding publications that match the search query.
    """
    final_df = pd.DataFrame()
    query_parts = []
    if name:
        if second_auth:
            query_parts.append(f'(author:"^{name}" OR pos(author:"{name}",2))')
        else:
            query_parts.append(f'author:"^{name}"')
    if institution:
        query_parts.append(f'pos(institution:"{institution}",1)')
    if year:
        years = format_year(year)
        query_parts.append(f'pubdate:{years}')

    if not query_parts:
        print("You did not give me enough to search on, please try again.")
        return final_df
    
    query = " AND ".join(query_parts)
    print(f"I will search for papers matching the following criteria:\n{query}\n")

    encoded_query = urlencode({
        "q": query,
        "fl": "title, first_author, bibcode, abstract, aff, pubdate, keyword, identifier",
        "fq": "database:astronomy," + str(refereed),
        "rows": 3000,
        "sort": "date desc"
    })

    try:
        print('I am now querying ADS.\n')
        results = requests.get(
            "https://api.adsabs.harvard.edu/v1/search/query?{}".format(encoded_query),
            headers={'Authorization': 'Bearer ' + token}
        )
        data = results.json()["response"]["docs"]
    except:
        print('Oops, something went wrong.\n')

    df = do_search(name, institution, token, encoded_query)

    if df.empty and institution:
        print('DataFrame is empty! Trying alternative search using affiliation instead of institution.')
        new_query_parts = []
        for part in query_parts:
            # Replace the institution clause with an affiliation clause
            if f'pos(institution:"{institution}",1)' in part:
                new_query_parts.append(f'pos(aff:"{institution}",1)')
            else:
                new_query_parts.append(part)
        new_query = " AND ".join(new_query_parts)
        print(f"Alternative query:\n{new_query}\n")
        encoded_query = urlencode({
            "q": new_query,
            "fl": "title, first_author, bibcode, abstract, aff, pubdate, keyword,identifier",
            "fq": "database:astronomy," + str(refereed),
            "rows": 3000,
            "sort": "date desc"
        })
        df = do_search(name, institution, token, encoded_query)
        if df.empty:
            similar_inst = get_similar_institution(institution, institutions_file="exceptions.xlsx")
            if similar_inst:
                print(f"DataFrame still empty! Finding from exception list: {similar_inst}")
                new_query_parts = []
                for part in query_parts:
                    if f'pos(institution:"{institution}",1)' in part:
                        new_query_parts.append(f'pos(aff:"{similar_inst}",1)')
                    else:
                        new_query_parts.append(part)
                new_query = " AND ".join(new_query_parts)
                print(f"Alternative query using exception list:\n{new_query}\n")
                encoded_query = urlencode({
                    "q": new_query,
                    "fl": "title, first_author, bibcode, abstract, aff, pubdate, keyword, identifier",
                    "fq": "database:astronomy," + str(refereed),
                    "rows": 3000,
                    "sort": "date desc"
                })
                df = do_search(name, similar_inst, token, encoded_query)
                df["Input Institution"] = institution
            else:
                print("No similar institution found using exception list")
    
    if institution and deep_dive:
        if not df.empty:
            unique_authors = df["Input Author"].unique().tolist()
            print(f"Unique authors from institution search: {unique_authors}")
            author_results = []
            for author in unique_authors:
                print(f"Re-running ADS search for author: {author} without institutional constraint")
                data_author = ads_search(
                    name=author,
                    institution=None,
                    year='[2000 TO 2030]',
                    token=token,
                    stop_dir=stop_dir,
                    second_auth=second_auth,
                    groq_analysis=False,
                    deep_dive=False  
                )
                if not data_author.empty:
                    author_results.append(data_author)
            if not author_results:
                print("No author-specific ADS results found after re-running search.")
                return pd.DataFrame()
            df = pd.concat(author_results, ignore_index=True)

    if not df.empty:
        data2 = merge(df)
        data3 = data_type(data2)
        data4 = n_grams(data3, stop_dir)
        if groq_analysis:
            print("Running Groq subtopics analysis on ADS results...")
            data4 = generate_expertise(data4, groq_client=get_groq())
            print("Groq analysis complete.")
        return data4
    else:
        print("No results found.")
        dummy_data = {
            'Input Author': name if name else "None",
            'Input Institution': institution if institution else "None",
            'First Author': "None",
            'Bibcode': "None",
            'Title': "None",
            'Publication Date': "None",
            'Keywords': "None",
            'Affiliations': "None",
            'Abstract': "None",
            'Identifier': "None",
            'Data Type': "None",
            'Top 10 Words': "None",
            'Top 10 Bigrams': "None",
            'Top 10 Trigrams': "None"
        }
        return pd.DataFrame([dummy_data])


def data_type(df):
    """
    Determines whether at least half of the author's publications are in the specified list of journals. 
    
    Returns the dataframe with the 'Data Type' column added with the label 'Clean' or 'Dirty'. 
    """
    journals = ['ApJ','GCN','MNRAS', 'AJ', 'Nature', 'Science', 'PASP', 'AAS', 'arXiv', 'SPIE', 'A&A', 'zndo','yCat','APh', 'PhRvL']
    df['Data Type'] = ''
    for index, row in df.iterrows():
        bibcodes_str = row['Bibcode']
        bibcodes = bibcodes_str.split(', ')
        total_papers = len(bibcodes)
        clean_count = sum(any(journal in bibcode for journal in journals) for bibcode in bibcodes)
        if clean_count >= total_papers / 2:
            data_type_label = 'Clean' # More than half in the list of journals
        else:
            data_type_label = 'Dirty'
        df.at[index, 'Data Type'] = data_type_label
    return df
        
def merge(df):
    """
    Merges all rows under the same author name and concatenates their results.
    
    Returns the resulting merged dataframe.
    """
    df['Publication Date'] = df['Publication Date'].astype(str)
    df['Abstract'] = df['Abstract'].astype(str)
    df['Keywords'] = df['Keywords'].apply(lambda keywords: keywords if keywords else []) # <- Fix for Keywords
    df['Title'] = df['Title'].apply(lambda titles: titles if titles else []) 
    df['Identifier'] = df['Identifier'].apply(lambda ids: ids if ids else []) 
    df.fillna('None', inplace=True)

    merged = df.groupby('Input Author').aggregate({'Input Institution': ', '.join,
                                                 'First Author': ', '.join,
                                                 'Bibcode': ', '.join,
                                                 'Title': lambda x: list(itertools.chain.from_iterable(x)), 
                                                 'Publication Date': ', '.join,
                                                 'Keywords': lambda x: list(itertools.chain.from_iterable(x)), # <- Fix for Keywords
                                                 'Affiliations': ', '.join,
                                                 'Abstract': ', '.join,
                                                 'Data Type': ', '.join,
                                                 'Identifier': lambda x: list(itertools.chain.from_iterable(x))  
                                                 }).reset_index()
    return merged

def n_grams(df, directorypath):
    """
    Calculates the top words, bigrams, and trigrams for through an author's abstracts.
    
    Returns the dataframe including the top 10 words, bigrams, and trigrams.
    """
    top10Dict = {'Top 10 Words': [],
                 'Top 10 Bigrams': [],
                 'Top 10 Trigrams': []}

    for i in df.values:
        abstracts = i[8]
        top10words = TA.topwords(abstracts, directorypath)
        top10bigrams = TA.topbigrams(abstracts, directorypath)
        top10trigrams = [( " ".join(trigram), count) for trigram, count in list(TA.toptrigrams(abstracts, directorypath))]
        top10bigrams = [( " ".join(bigram), count) for bigram, count in list(TA.topbigrams(abstracts, directorypath))]
        top10Dict['Top 10 Words'].append(top10words)
        top10Dict['Top 10 Bigrams'].append(top10bigrams)
        top10Dict['Top 10 Trigrams'].append(top10trigrams)

    top10Df = df
    top10Df['Top 10 Words'] = top10Dict['Top 10 Words']
    top10Df['Top 10 Bigrams'] = top10Dict['Top 10 Bigrams']
    top10Df['Top 10 Trigrams'] = top10Dict['Top 10 Trigrams']

    top10Df = top10Df[['Input Author', 'Input Institution', 'First Author', 'Bibcode', 'Title', 'Publication Date',
                       'Keywords', 'Affiliations', 'Abstract', 'Identifier', 'Top 10 Words', 'Top 10 Bigrams',
                       'Top 10 Trigrams', 'Data Type']]
    return top10Df


def get_user_input(dataframe):
    """
    Gets user input for searching a dataframe.
    
    Returns a dictionary with search parameters for either a name or institution search.
    """
    available_search_types = {
        "name": "Name Search - search by author name",
        "institution": "Institution Search - search by institution"
    }
    
    print("\nWhat type of search do you want to conduct?")
    for key, description in available_search_types.items():
        print(f"-Enter '{key}' for {description}")
    
    while True:
        try:
            search_type = input("\nEnter search type: ('name' or 'institution'):\n").lower()
            if search_type in available_search_types:
                break
            print("Invalid search type. Please enter 'name' or 'institution'.")
        except NameError:
            print("Error getting input. Please try again.")
    
    print(f"You are running '{search_type}' search.\n")
    print("Listed are the available columns from your dataset:", ", ".join(dataframe.columns))
    search_params = {'search_type': search_type}
    
    if search_type == 'name':
        name_input = input("Enter the name of the column that contains the data for 'name' search: ").strip()
        if name_input:
            matching_columns = [col for col in dataframe.columns if col.lower() == name_input.lower()]
            search_params['name_column'] = matching_columns[0] if matching_columns else "Name"
        else:
            search_params['name_column'] = "Name"
        while True:
            include_second = input("Do you want to include search by second author? (y/n) [n]: ").strip().lower() or "n"
            if include_second in ["y", "n"]:
                break
            print("Invalid choice. Please enter 'y' or 'n'.")
        search_params['second_author'] = (include_second == "y")
    
    elif search_type == 'institution':
        inst_input = input("Enter the name of the column that contains the data for 'institution' search: ").strip()
        if inst_input:
            matching_columns = [col for col in dataframe.columns if col.lower() == inst_input.lower()]
            search_params['institution_column'] = matching_columns[0] if matching_columns else "Institution"
        else:
            search_params['institution_column'] = "Institution"
        run_deep = input("Do you want to run a deep dive search (re-run for each author) for institution search? (y/n) [n]: ").strip().lower() or "n"
        search_params['deep_dive'] = (run_deep == "y")
    
        
    year_range = input("Enter the year range for your search (format: [YYYY TO YYYY] or a 4-digit year, default: [2003 TO 2030]): ").strip() or "[2003 TO 2030]"
    search_params['year_range'] = year_range
    
    ref_input = input("Do you want refereed papers only? (y/n) [y]: ").strip().lower() or "y"
    if ref_input == "y":
        search_params['refereed'] = "property:refereed"
    else:
        search_params['refereed'] = "property:notrefereed OR property:refereed"
    
    
    run_groq = input("Do you want to run Groq subtopics analysis on the ADS results? (y/n) [n]: ").strip().lower() or "n"
    search_params['groq_analysis'] = (run_groq == "y")
    return search_params


def run_file_search(filename, token, stop_dir):
    """
    Runs ADS search based on user's search type (name or institution).
    
    For institution searches, after the initial search we extract unique authors from the results
    and re-run ADS search for each author with the institution filter.
    Finally, if Groq analysis is requested, it is applied on the aggregated results.
    """
    dataframe = pd.read_csv(filename)
    final_df = pd.DataFrame()
    count = 0
    search_params = get_user_input(dataframe)
    print("Searching for results...")
    search_type = search_params['search_type']
    # Name search logic, including second author logic
    if search_type == 'name':
        for i in range(len(dataframe)):
            name = dataframe[search_params['name_column']][i]
            second_auth = search_params.get('second_author', False)
            data1 = ads_search(
                name=name,
                institution=None,
                year=search_params['year_range'],
                token=token,
                stop_dir=stop_dir,
                second_auth=second_auth,
                groq_analysis=False,
                deep_dive=False,
                refereed=search_params.get('refereed')
            )
            search_identifier = f"name: {name} (including {'second' if second_auth else 'only first'} author)"
            if not data1.empty:
                data2 = merge(data1)
                data3 = data_type(data2)
                data4 = n_grams(data3, stop_dir)
                final_df = pd.concat([final_df, data4], ignore_index=True)
                count += 1
                print(f"Completed {count} searches - Processed {search_identifier}")
            else:
                print(f"No results found for {search_identifier}")
    elif search_type == 'institution':
        inst_results = []
        for i in range(len(dataframe)):
            inst = dataframe[search_params['institution_column']][i]
            print(f"Processing institution: {inst}")
            data = ads_search(
                name=None,
                institution=inst,
                year=search_params['year_range'],
                token=token,
                stop_dir=stop_dir,
                second_auth=False,
                groq_analysis=False,
                deep_dive=search_params.get('deep_dive', False),
                refereed=search_params.get('refereed')
            )
            if not data.empty:
                inst_results.append(data)
            else:
                print(f"No records found for institution: {inst}")
        if inst_results:
            final_df = pd.concat(inst_results, ignore_index=True)
            print(f"Processed institution search with deep_dive={search_params.get('deep_dive', False)}")
        else:
            print("No records found for any institution search.")
    
    # Run groq analysis on all search results 
    if search_params.get('groq_analysis', False) and not final_df.empty:
        print("Running Groq subtopics analysis on aggregated ADS results...")
        final_df = generate_expertise(final_df, groq_client=get_groq())
        print("Groq analysis complete.")
    
    return final_df


