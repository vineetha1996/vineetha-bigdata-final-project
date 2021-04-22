# Big Data Final Project - Pyspark Text Processing - Word count 
## Author:

<td align="center"><a href="https://github.com/vineetha1996"><img src="https://avatars.githubusercontent.com/u/59989572?v=4" width="100px;" alt=""/><br /><sub><b>Vineetha Yenugula</b></sub></a><br /></td>

[My LinkedIn Profile](https://www.linkedin.com/in/vineetha-yenugula-84a88b19a/)

## My Text data:

[The Dazzling Miss Davison by Florence Warden](https://www.gutenberg.org/files/6130/6130-0.txt)

## Tools and Languages used:

Tools: Databricks, Pyspark, Pandas, Seaborn, Pyspark, Pandas, Regex, MatPlotLib, Spark, NLP

Languages: Python

## Prerequisites:

<ul>
<li>Should have Databricks community edition account</li>
<li>Basics knowledge in Python, Pyspark, NLP, Map, FlatMap</li>
</ul>

## Installations:

<ul>
<li>wordCloud</li>
<li>nltk</li>
</ul>

## Process:

Inorder to get data from the Url we need to import urllib.request
```
import urllib.request
```
Use Below command to get data from the desired url
```
urllib.request.urlretrieve("https://www.gutenberg.org/files/65117/65117-0.txt", "/tmp/dazzling.txt")
```

Command to move file from temp folder to data folder
```
dbutils.fs.mv("file:/tmp/dazzling.txt", "dbfs:/data/dazzling.txt")
```

command to transfer data file inti spark
```
dazzlingDavisionRDD = sc.textFile("dbfs:/data/dazzling.txt")
```

### Cleaning Data:

Inorder to filter out data with punctuations, stopwords, non-letter characters and to convert each letter to lowercase use below commands

```
# clean out punctuation 
# Forcing all words to lowercase
# filter out stopwords
# Remove all non-letter characters from each word token.
# map() to intermediate key-value pairs (word, 1)

import re

# flatmap() each line to messy tokens
dazzlingDavisionMessyTokensRDD = dazzlingDavisionRDD.flatMap(lambda line: line.lower().strip().split(" "))

dazzlingDavisionCleanTokensRDD = dazzlingDavisionMessyTokensRDD.map(lambda letter: re.sub(r'[^A-Za-z]', '', letter))

# filter out stopwords
from pyspark.ml.feature import StopWordsRemover
remover = StopWordsRemover()
stopwords = remover.getStopWords()
dazzlingDavisionRDD = dazzlingDavisionCleanTokensRDD.filter(lambda myLettersW: myLettersW not in stopwords)

# remove empty spaces
dazzlingDavisionRemoveEmptySpacesRDD = dazzlingDavisionRDD.filter(lambda x: x != "")
```

### Processing Data:

```
# mapping words to immediate key value pairs
dazzlingDavisionPairsRDD = dazzlingDavisionRemoveEmptySpacesRDD.map(lambda word: (word,1))
```

```
# tranforming pairs to word count
dazzlingDavisionWordCountRDD = dazzlingDavisionPairsRDD.reduceByKey(lambda acc, value: acc + value)
```

```
# sorting words in descending order
dazzlingDavisionResults = dazzlingDavisionWordCountRDD.map(lambda x: (x[1], x[0])).sortByKey(False).take(10)
```

```
# Print results
print(dazzlingDavisionResults)
```

### Charting

```
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
 
source = 'No need to light a night-light on a light night like tonight.'
title = 'Top Words in ' + source
xlabel = 'Count'
ylabel = 'Words'

# create Pandas dataframe from list of tuples
df = pd.DataFrame.from_records(dazzlingDavisionResults, columns =[xlabel, ylabel]) 

# Printing df
print(df)

plt.figure(figsize=(10,3))
sns.barplot(xlabel, ylabel, data=df, palette="vlag").set_title(title)
```

OutPut Image:












## References: 

[Seaborn](https://seaborn.pydata.org/tutorial/color_palettes.html)
[nltk error](https://stackoverflow.com/questions/54937516/name-nltk-is-not-defined)










