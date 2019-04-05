from pyspark import SparkContext

sc = SparkContext()

# define your string
s = 'This is my some kind of test text. It may not be best as best or as good as fine or fine but but is reapeting the the'
word_rdd = sc.parallelize( s.split() )
print( word_rdd.map( lambda word: (word.replace( ',','' ).lower(),1) ).reduceByKey( lambda key,value: key+value ).collect() )


# Or read data from a file
file_word_rdd =  sc.textFile( r'word_list.txt' )
print( file_word_rdd.flatMap( lambda line: line.split( ' ' ) ).map( lambda word: (word.replace( ',','' ).lower(),1) ).reduceByKey( lambda a,b: a+b ).collect() )







