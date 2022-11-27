from thefuzz import fuzz
import pandas as pd

# ------------ INPUT VARIABLES ------------ #

dataset_1_path = 'data1.csv'
dataset_1_matcher_col = 'Team'
dataset_2_path = 'data2.csv'
dataset_2_matcher_col = 'Query'

required_similarity = 75
run_type = 'all' # either ratio, token_sort_ratio, partial_ratio, or all
output_csv_name = 'output.csv'

# ------------ FUNCTIONS ------------ #

def match_on(term, list_terms, min_score, match_type):
    max_score = -1
    max_name = '' 
    for x in list_terms:
        score = None
        if match_type == 'ratio':
            score = fuzz.ratio(term, x)
        elif match_type == 'token_sort_ratio':
            score = fuzz.token_sort_ratio(term, x)
        elif match_type == 'partial_ratio':
            score = fuzz.partial_ratio(term, x)
        if (score > min_score) & (score > max_score): 
            max_name = x 
            max_score = score 
    return (max_name, max_score) 


def match_terms_df(d1_list, d2_list, min_similarity_score=75, match_type='ratio'):
    terms = []
    for i in d1_list:
        match = match_on(
            term = i, 
            list_terms = d2_list, 
            min_score = min_similarity_score, 
            match_type=match_type
        )
        if match[1] >= min_similarity_score:
            dict_res = {
                'term': i, 
                'match': match[0], 
                'score': match[1],
                'match_on': match_type
            }
            terms.append(dict_res)
    return pd.DataFrame(terms)


# ------------ CORE JOB ------------ #

if __name__ == "__main__":
    df1 = pd.read_csv(dataset_1_path)
    df2 = pd.read_csv(dataset_2_path)

    #Casting the query and attraction columns of both dataframes into lists
    d1_list = list(df1[dataset_1_matcher_col].unique())
    d2_list = list(df2[dataset_2_matcher_col].unique())
    
    # create a function to run job based on "run_type". If "all", run all 3 types
    if run_type == 'all':
        df_ratio = match_terms_df(d1_list, d2_list, required_similarity, 'ratio')
        df_token_sort_ratio = match_terms_df(d1_list, d2_list, required_similarity, 'token_sort_ratio')
        df_partial_ratio = match_terms_df(d1_list, d2_list, required_similarity, 'partial_ratio')
        df = pd.concat([df_ratio, df_token_sort_ratio, df_partial_ratio])
        
    else:
        df = match_terms_df(d1_list, d2_list, required_similarity, run_type)
    
    #Exporting the results to a csv file    
    df.to_csv(output_csv_name, index=False)