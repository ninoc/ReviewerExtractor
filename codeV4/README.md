**NEW FEATURES IN V3 (June 2025)**

In version 3 of the ReviewerExtractor we implemented a few optional keywords that expand the codebase functionalities. We also further implement the usage of LLM for ingesting not just the abstract but the whole publication by single authors (see LLM section).
- **[second_auth]**: this optional keyword allows the user to search for researcher's publications using their name as first OR second authors. This can be useful for researchers that mentor early career colleagues and therefore may have a lower volume of first-author papers. (Default = False)
- **[groq_analysis]** : this optional keywords utilizes Groq AI to match the result of the N-grams with the list of [AAS astronomical journal keywords](https://journals.aas.org/keywords-2013/). If requested the output will contain a new column called "Subtopics". **This can be a very slow process** (Default = False)
- **[deep_dive]** : this optional keyword will extract from an institutional search all researchers affiliated to that institution and then runs a researcher search over the 2003-2030 time period and retrive the entire publication history of those indivuals independently of the original affiliation search.
- **Data Type flag**: the final dataframe contain a "Data Type" flag (Clean/Dirty) that help identifying researchers with the majority of publication in astronomical or physics journals (Clean).  
  
**TIPS To Maximize The Code Results**
- Extend the year range of research as people may have published in years past
- When using list of universities or researchers names check the output dataframe:
1) Names may be missing because the input file had them mispelled;
2) Universities may be missing because the way they were searched was not how people wrote their affiliations in their article and so the ADS query cannot find a match (e.g. Cal Poly Pomona vs. California Politechnique Institute). Possible solutions are to try and run single searches on Names / Intitutions using different formats
- Perform simple searches ("CMD+F) on the excel version of the output dataframe: this will allow you to search words in the published abstracts as well and not rely only on the N-grams.
  
**What the User needs:**
The user needs an ADS API Access Token (can be found here:  https://ui.adsabs.harvard.edu/help/api/), which searches the input into ADS. Other libraries needed include: nltk, ads, and pandas. 

Please look at the [Jupyter Notebook v1 of the code](https://github.com/ninoc/ReviewerExtractor/blob/main/codeV1/ExpertiseFinder_Tutorial.ipynb) to learn about all the possible keywords.


**Current files:**
Some files are needed to run the actual search, while others are utilized in post-processing and expertise identification (e.g. N-grams creation): 
- ADSsearcherpkg.py: Python file that has all of the functions used to find the expertises of the authors and produce an organized data frame with each row being an individual author and columns: 'Input Author','Input Institution', 'First Author', 'Bibcode', 'Title', 'Publication Date', 'Keywords', 'Affiliations', 'Abstract', 'Identifier', 'Top10 words', 'Top 10 Bigrams', 'Top 10 Trigrams', 'Data Type'
- TextAnalysis.py: Python file that has all the functions in order to determine the top words, bigrams and trigrams in each publication.
- stopwords.txt: Text file that has a list of the stop words for language processing. 
- ADS_search.ipynb: A notebook that contains the different examples of how to use the ADSsearcherpkg functions with different input cases. These input cases include just an author, just an institution, and a csv file of 3 authors with their corresponding institutions.

