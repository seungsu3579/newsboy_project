import pandas as pd
import time
from tqdm import tqdm
from scipy.spatial import distance
import numpy as np

from database import controller
from word2vec.model import WordToVector

word2vec = WordToVector()

def get_articles_df():
    data = controller.select_keyword()
    vectors = []
    keywords = []
    indexes = []
    for idx, keyword in data:
        if keyword is None:
            continue
        keyword_list = keyword[1:-1].split(",")
        if type(word2vec.vectorize_word_list(keyword_list)) is float:
            continue
        indexes.append(idx)
        keywords.append(keyword_list)
        w_vector = (word2vec.vectorize_word_list(keyword_list)).tolist()
        vectors.append(w_vector)

    df = pd.DataFrame({'news_id': indexes, 'keywords': keywords, 'vectors': vectors})

    return df

def clustering(df, pivot):
    start_time = time.time()
    cosine_dict = {}
    search = {}
    for i_index in tqdm(range(len(df))):
        start_time = time.time()
        stack = []
        result = {}
        if i_index in search:
            continue
        for j_index in range(len(df)):
            if i_index == j_index:
                continue
            if j_index in cosine_dict:
                continue
            if j_index in search:
                continue
            cosine = 1 - distance.cosine(df['vectors'][i_index], df['vectors'][j_index])
            if cosine <= pivot:
                continue
            stack.append(j_index)
            
        while len(stack) != 0:
            index_value = stack.pop()
            for k_index in range(len(df)):
                
                if index_value == k_index:
                    continue
                    
                if k_index in stack:
                    continue
                    
                if i_index == k_index:
                    continue
                    
                if k_index in result:
                    continue
                    
                if k_index in cosine_dict:
                    continue
                    
                if k_index in search:
                    continue
                    
                cosine = 1 - distance.cosine(df['vectors'][index_value], df['vectors'][k_index])
                if cosine <= pivot:
                    continue
                result.update({k_index: 0})
                search.update({k_index: 0})
            result.update({index_value: 0})
            search.update({index_value: 0})
                
        cosine_dict[i_index] = list(result.keys())
        
    return cosine_dict

def find_representative(df, clustered):
    representatives = []
    for key, value in clustered.items():
        clustered[key].append(key)
        sums = np.array([0.0]*300)
        for i in clustered[key]:
            sums += np.array(df['vectors'][i])
        check = sums/len(clustered[key])
        cosine = {}
        for j in clustered[key]:
            cosine[j] = 1- distance.cosine(check, df['vectors'][j])
        cosine_new = list(cosine.items())
        cosine_new.sort(key=lambda x:x[1],reverse=True)
        representatives.append(df['news_id'][cosine_new[0][0]])
    return representatives