# big-data

Big data coursework
===================
This is the coursework of bigdata in university of Glasgow, it is a 
the main purpose of this task is to implement a methd which can calculate the pagerank for each link in records
Our solution contains Initialization map-reduce and PageRank map-reduce. 
The whole algorithm has several classes

The whole process can be divided into three steps: Initialization, PageRank, Transformer

For using, please type:  hadoop path/to/input/file path/to/output/file iteration(times) time(In ISO8601 format) 


# Initialization
This step was implemented to process the get the record titles, timestamps and all the outlinks
There are three files in this step:
1.titleJob: contains the map-reduce to process data
2.MyInputFormat
3. MyRecordReader
4. 
In first step, we set the doucument as input 
...


Initialization-map
In the mapping part, we compaire the token with the String"REVISION", then we can find the title by 

tokenizer.nextToken. Secondly, the self-loop links removed for each record.
the output for mapping will be set in <title, timestamp outlink1 outlink2 ...>
the format of output for map

| key | value |
| ----- | ----- |
| title1 | timestamp outlink2 |
| title2 | timestamp outlink2 |


Initialization-reduce
For the reduce part, there are two parts in this part. Firstly, the timestamp will be compaired to take out the 

out-date record. Secondly, we remove the timestamp and repace it by 1 for pagerank loop in PageRank step. The 

output of reduce is formatted in <title, 1 outlink1 outlink2 ...>
tips:the time for user input need to be formated in standard format, otherwise the program will crash.
the output of this process is 

| key | value |
| ----- | ----- |
| title1 | 1 outlink1 outlink2 |
| title2 | 1 outlink1 outlink2 |


# PageRank
The class it contains are ...
The score can be expressed to: PR=(1-damping factor)+damping factors*sum(PR/number of outlinks)
In the pagerank step, the rank score can be calculated for each link. We also set a loop to converge the score. 
eg. the input format is <A 1 B C D>

PageRank-map 
the purpose of maping step is to compute all the score for each record. 
it can shows all the score in that record for each outlink and all the outlinks it has
the example of output in mapping is <B @0.15> <C @0.15> <D @0.15> <A &B C D>

after suffle, the pairs can be combined, for example A <&B C D, @0.15>

PageRank-reduce 
in this step, all the score for a link will be calculated 
if the value contains "@" then parse the String into float and calculate
if the value contains "&" then extract the String below
the output of this step is the title of the link(key) and its pagerank score, followed by all its outlinks(value).

tips: we need to consider the link which do not have any record. these link are only appears in outlink.We also 

need to calculate their score. The 



iteration
the Pagerank mapreduce process need to iterated for several times to converge the score. 



final output:<title, pagerank score>
