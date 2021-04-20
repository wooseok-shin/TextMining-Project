import numpy as np
import parmap # pip install parmap
import re, pickle
import pandas as pd
from hanspell import spell_checker
from pykospacing import spacing


def multicore_cpu(df, func, n_cores, spell=True):
    df_split = np.array_split(df, n_cores)
    if spell:
        for idx_df, df in enumerate(df_split):
            df.reset_index(drop=True, inplace=True)

    df = pd.concat(parmap.map(func, df_split, pm_pbar=True, pm_processes=n_cores))
    return df


def load_data(base_path, pkl_lst):
    data = []
    for idx_name, file_name in enumerate(pkl_lst):
        with open(f'{base_path}/{file_name}.pickle', 'rb') as fr:
            temp = pickle.load(fr)
        data.extend(temp)
    return pd.DataFrame(data)


def preprocess(base_path, pkl_lst):
    data = load_data(base_path, pkl_lst)
    print('Corpus length:', len(data))
    print('Preprocessing...')
    # 공지글이면 댓글 활용
    for i in range(len(data)):
        if data.loc[i, 'category'] == '공지':
            if not data.loc[i, 'comments']:
                data.loc[i, 'content'] = ''
            else:
                data.loc[i, 'content'] = ' '.join(list(pd.DataFrame(data.loc[i, 'comments'])['comment']))

    data['contents'] = data['title'] + data['content']
    data['date'] = data['date'].apply(lambda x: str(x).split(' ')[0].replace('-', ''))
    data['contents'] = data['contents'].apply(lambda x: str(x).replace('\n', ' '))

    data.drop(['article_no', 'author', 'category', 'comments', 'title', 'content', 'likes', 'dislikes', 'views'], axis=1, inplace=True)

    # compiler
    han = re.compile(('[^ ㄱ-ㅣ가-힣]+'))
    kor = re.compile((r'[|ㄱ-ㅎ|ㅏ-ㅣ]+'))
    space = re.compile(r'\s\s+')

    compilers = [kor, han, space]
    for idx_compiler, compiler in enumerate(compilers):
        data['contents'] = data['contents'].apply(lambda x: compiler.sub(' ', x))
    data['contents'] = data['contents'].str.strip()

    # eliminate the row that has only spaces for Komoran
    for idx_row, row in enumerate(data['contents']):
        if len(row) == 1:
            data.drop(idx_row, inplace=True)

    data.reset_index(drop=True, inplace=True)
    return data


def spacing_doc(DataFrame):
    DataFrame['contents'] = DataFrame['contents'].apply(lambda x: spacing(x))
    return DataFrame


def spell_check(arr):
    date_lst = []
    checked_lst = []
    for idx_article, article in enumerate(arr['contents']):
        try:
            result = spell_checker.check(article)
            if result.result == True:
                checked_lst.append(result.checked)
            else:
                checked_lst.append(article)

            date_lst.append(arr['date'][idx_article])

        except:
            print('Passing index', idx_article)
            pass

    checked_data = pd.DataFrame({'date': date_lst, 'contents': checked_lst})
    return checked_data