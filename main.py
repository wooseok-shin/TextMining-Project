import os
import time
from konlpy.tag import Komoran
from utils import *
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--cpu_core', type=int, default=int(os.cpu_count() / 2), help='the number of cpu core')
parser.add_argument('--token_cnt', type=int, default=5, help='the threshold to filter non-meaningful documents')
parser.add_argument('--base_path', type=str, default='./concat', help='base folder name')
parser.add_argument('--save_path', type=str, default='saved/', help='save folder name')
parser.add_argument('--token_dict', type=str, default='userdic.txt', help='user dictionary for tokenizing')
parser.add_argument('--file_name', nargs='*', type=str,
                    default=['rawtext_Maple_official', 'Maple_issues', 'Maple_opinions'], help='input data')

args = parser.parse_args()


def main(base_path, pkl_lst):
    DataFrame = preprocess(base_path, pkl_lst)
    print('Spacing the document...')
    DataFrame = multicore_cpu(DataFrame, spacing_doc, n_cores=args.cpu_core, spell=False)
    print('Spell checking...')
    checked_data = multicore_cpu(DataFrame, spell_check, n_cores=args.cpu_core, spell=True)
    checked_data.reset_index(drop=True, inplace=True)

    # tokenizing
    print('Tokenizing the document...')
    komoran = Komoran(userdic=args.token_dict)
    checked_data['tokenized_contents'] = checked_data['contents'].apply(lambda x: komoran.morphs(x))

    # filter documents
    checked_data['doc_length'] = checked_data['tokenized_contents'].apply(lambda x: len(x))
    final_data = checked_data.loc[checked_data['doc_length'] > args.token_cnt]
    final_data.reset_index(drop=True, inplace=True)

    # save the output data
    os.makedirs(args.save_path, exist_ok=True)
    with open(os.path.join(args.save_path, 'preprocessed_data.pickle'), 'wb') as f:
        pickle.dump(final_data, f)


if __name__ == '__main__':
    print(vars(args))
    start = int(time.time())
    main(args.base_path, args.file_name)
    print('Total time: ', int(time.time())-start)
