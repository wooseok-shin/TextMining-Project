import re
import os
import pickle
import argparse
from hanspell import spell_checker
from pykospacing import spacing
from tqdm import tqdm

def spacing_spell_checker(pkl_path, save_path, file_name):

    with open(os.path.join(pkl_path, (file_name + '.pickle')), 'rb') as f:
        tmp_corpus = pickle.load(f)

    for article in tqdm(tmp_corpus):
        
        # Remove characters that are not Korean
        # Korean Spacing | https://github.com/haven-jeon/PyKoSpacing
        # Korean Spell checker | https://github.com/ssut/py-hanspell
        
        # title
        article['title'] = re.sub("[^ㄱ-ㅎㅏ-ㅣ가-힣 ]", "", article['title'])
        article['title'] = spacing(article['title'])
        result = spell_checker.check(article['title'])
        article['title'] = result.checked

        # content
        article['content'] = re.sub("[^ㄱ-ㅎㅏ-ㅣ가-힣 ]", "", article['content'])
        article['content'] = spacing(article['content'])
        result = spell_checker.check(article['content'])
        article['content'] = result.checked

        # comments
        for subcontent in article['comments']:
            subcontent['comment'] = re.sub("[^ㄱ-ㅎㅏ-ㅣ가-힣 ]", "", subcontent['comment'])
            subcontent['comment'] = spacing(subcontent['comment'])
            
            try :
                result = spell_checker.check(subcontent['comment'])
                if result.result == True :
                    subcontent['comment'] = result.checked
            except :
                pass
    
        break

    os.makedirs(save_path, exist_ok=True)   # 저장 경로가 없으면 생성
    with open(os.path.join(save_path, (file_name + '_checked.pickle')), 'wb') as f:
        pickle.dump(tmp_corpus, f)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--pkl_path', type=str, help='path of the pickle file')
    parser.add_argument('--save_path', type=str, help='path of the save file')
    parser.add_argument('--file_name', type=str, help='File name')
    args = parser.parse_args()
    print(vars(args))
    '''
    ex)
    python preprocessing.py --pkl_path=../your/path/ --save_path=../data/checked/ --file_name=Maple_opinions
    '''
    spacing_spell_checker(pkl_path=args.pkl_path, save_path=args.save_path, file_name=args.file_name)
