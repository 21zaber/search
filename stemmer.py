from nltk.stem import ISRIStemmer, LancasterStemmer, PorterStemmer, RSLPStemmer
from nltk.stem import WordNetLemmatizer, RegexpStemmer, SnowballStemmer
import nltk
nltk.download('wordnet')

from utils import log

CACHE_SIZE = 10**5

stemmer_dict = {
    'isris': ISRIStemmer,
    'lancaster': LancasterStemmer,
    'porter': PorterStemmer,
#    'rslps': RSLPStemmer,
    'snowball': SnowballStemmer,
#    'regexp': RegexpStemmer,
    'wordnet': WordNetLemmatizer,
}

class Stemmer:
    def __init__(self, method='default', use_cache=True):
        log("Stemming method", method)
        self.method = method
        if method == 'default':
            return
        if method == 'snowball':
            self.st = stemmer_dict[method]('english')
        else:
            self.st = stemmer_dict[method]()
        self.cache = {}
        self.use_cache = use_cache
    
    def stem(self, token):
        if self.method == 'default':
            return token
        if self.use_cache and token in self.cache:
            return self.cache[token]

        if self.method == 'wordnet':
            resp = self.st.lemmatize(token)
        else:
            resp = self.st.stem(token)

        if self.use_cache:
            self.cache[token] = resp
            if len(self.cache) > CACHE_SIZE:
                self.cache = {}
        return resp

