# ReviewerExtractor
This repository contains the main code for the Reviewer Extractor ([Volz et al., 2024](https://ui.adsabs.harvard.edu/abs/2024RNAAS...8...69V/abstract) ).<br>
A new article is forthcoming (Samal et al. 2025) describing the Version 3 new features.
Please utilize [this version of the code](https://github.com/ninoc/ReviewerExtractor/tree/main/codeV3).

**Purpose**

The Reviewer Extractor is a tool that is designed to help NASA Headquarters Astrophysics Program Scientists to 

- Collect data on institutional demographics of researchers across many type of affiliations through the NASA ADS archive based on their publication record
- Identify suitable peer reviews participants based on their derived expertise

Using these tools we tried to reach out to possible reviewers from institutions that normally do not participate, 
and hopefully expand the pool of applicants to NASA Astrophysics. We also were able to identify early career scientists (e.g. postdocs) thanks to the collection
of the indivuals' full publication record.

This repositories contains different versions of the previous work done by Máire Volz see [Github repository](https://github.com/maireav/NASA-Internship).


In the [examples](https://github.com/ninoc/ReviewerExtractor/blob/main/codeV3/ADS_search.ipynb) we provide a methods of finding experts in specific matters independent from their institutions. 

**Under the hood** 

The Expertise Finder accesseses [NASA ADS](https://ui.adsabs.harvard.edu/) through the ADS dedicated API and searches for the specific data according to the user's inputs (some are mandatory and some are optional). The output is a Pandas dataframe that can be downloaded with all the information collected via the API('First Author', 'Bibcode', 'Title', 'Publication Date', 'Keywords', 'Affiliations', and 'Abstract') and the N-grams created by our code, for visual inspection by the user. The user can then easily determine the expertise of each author ADS returned. 

# History
The routines, snippets, and main code were developed by NASA Headquarters interns in the 2022-2023 internship period: 
* The [version 1](https://github.com/ninoc/ReviewerExtractor/tree/main/codeV1) was developed by Máire Volz during her internship at NASA HQ in 2022-2023. 
* The [version 2](https://github.com/ninoc/ReviewerExtractor/tree/main/codeV2) was developed by several other NASA HQ interns, including Mallory Helfenbein (NASA HQ intern 2023).
* The [version 3](https://github.com/ninoc/ReviewerExtractor/tree/main/codeV3) was developed by Iizalaarab Elhaimeur and Sayem Kamal in 2024 and 2025 and it is the **most updated** version of the coee.
* The [LLM summarization model](https://github.com/ninoc/ReviewerExtractor/tree/main/LLM) was developed by Isabelle Hoare (NASA HQ intern 2023), modified in Version 3.
* The [GUIs](https://github.com/ninoc/ReviewerExtractor/tree/main/GUIs) were developed by Kaniyah Harris (NASA HQ intern 2023).

# Code requirements
The user needs an ADS API Access [Token](https://ui.adsabs.harvard.edu/help/api/), which enable unique users to access ADS. 
Other libraries needed include: nltk, ads. See [here](https://github.com/ninoc/ReviewerExtractor/blob/main/codeV3/README.md) for a complete list of requirements to run the Expertise Finder.
Further requirements are listed in the LLM and GUIs directories. These are still **work in progress** and development is still ongoing.

**Current files:**
Some files are needed to run the actual search, while others are utilized in post-processing and expertise identification (e.g. N-grams creation): 
- ADSsearcherpkg: Python file that has all of the functions used to find the expertises of the authors and produce an organized data frame with each row being an individual author and columns: 'Input Author','Input Institution', 'First Author', 'Bibcode', 'Title', 'Publication Date', 'Keywords', 'Affiliations', 'Abstract', 'Data Type'
- TextAnalysis.py: Python file that has all the functions in order to determine the top words, bigrams and trigrams in each publication.
- stopwords.txt: Text file that has a list of the stop words for language processing. 
- ADS_search.ipynb: A notebook that contains the different examples of how to use the ADSsearcherpkg functions with different input cases. These input cases include just an author, just an institution, and a csv file of 3 authors with their corresponding institutions.

# Code citation
The [version 1](https://github.com/ninoc/ReviewerExtractor/tree/main/codeV1) was presented at the American Astronomical Society in June 2023 [Volz et al. 2023](https://ui.adsabs.harvard.edu/abs/2023AAS...24210207V/abstract).

The Version 2 is presented in a publication by [Volz et al., 2024](https://ui.adsabs.harvard.edu/abs/2024RNAAS...8...69V/abstract).
The Version 3 implementation will be describe in an incoming paper (Kamal et al. 2025).

**Credits** 
- Máire Volz
- Mallory Helfenbein
- Isabelle Hoare
- Kaniyah Harris
- Iizalaarab Elhaimeur
- Sayem Kamal
- Antonino Cucchiara, PhD
  
# Contact information
Antonino Cucchiara, PhD.  
NASA HQ  
Email: antonino.cucchiara@nasa.gov
