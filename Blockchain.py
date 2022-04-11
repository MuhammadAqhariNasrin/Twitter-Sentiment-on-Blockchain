from textblob import TextBlob
from pyspark import SparkConf, SparkContext
import re
import string


def abb_en(line):
   abbreviation_en = {
    'u': 'you',
    'thr': 'there',
    'asap': 'as soon as possible',
    'lv' : 'love',    
    'c' : 'see'
   }
   
   abbrev = ' '.join (abbreviation_en.get(word, word) for word in line.split())
   return (abbrev)

def resolve_emoticon(line):
    emoticon = {':-)':'smile',
                ':)':'sad',
                ':))':'very happy',
                ':)':'happy',
                ':((':'very sad',
                ':(':'sad',
                ':-P':'tounge',
                ':-o':'gasp',
                '>:-)':'angry'}
    for key in emoticon:
        line = line.replace(key,emoticon[key])
    return line
        

def remove_features(data_str):
   
    url_re = re.compile(r'https?://(www.)?\w+\.\w+(/\w+)*/?')    
    mention_re = re.compile(r'@|#(\w+)')  
    RT_re = re.compile(r'RT(\s+)')
    num_re = re.compile(r'(\d+)')
    
    data_str = str(data_str)
    data_str = RT_re.sub(' ', data_str)  
    data_str = data_str.lower()  
    data_str = url_re.sub(' ', data_str)   
    data_str = mention_re.sub(' ', data_str)  
    data_str = num_re.sub(' ', data_str)
    return data_str

def sentiment(tweet):
    polarity = TextBlob(tweet).sentiment.polarity
    
    if polarity > 0:
       polarity = "Positive"
    elif polarity < 0:
        polarity = "Negative"   
    else:
        polarity = "Neutral"
    return polarity
  
   
#Write your main function here
def main(sc, filename):
    data = sc.textFile(filename)\
    .map(lambda x:x.split(","))\
    .filter(lambda x: len(x) ==8)\
    .filter(lambda x: len(x[0])>1)
    
    data_2 = data.map(lambda x:x[1]).map(lambda x: remove_features(x)).map(lambda x: abb_en(x)).map(lambda x: sentiment(x))
    
    result = data.zip(data_2)\
    .map(lambda x:str(x).replace("'"," "))\
    .map(lambda x: str(x).replace('"',' '))
    
    result.saveAsTextFile('Bitcoin_result3')
    
    print(result.take(2))
   
   

  
   

if __name__ == "__main__":
    conf = SparkConf().setMaster("local[1]").setAppName("My Spark Application")
    sc = SparkContext(conf=conf)
    
    filename = 'bitcoin.csv'
    

  
    main(sc,filename)

    sc.stop()
