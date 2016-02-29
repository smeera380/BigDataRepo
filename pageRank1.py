from __future__ import print_function

import re
import sys
from operator import add
import xml.etree.ElementTree as et

from pyspark import SparkContext



def parseLine(ln):
    """ Parses the WEX Dataset line into PageId, PageName and number of outlinks"""

    xmlline = ln.split('\t')
    st = xmlline[3].encode('ascii','ignore')
    #print('################    Initial XML  ################### ',st)  
    #outFile.write(st +'\n')

    outlinks = []

    st = st.encode("ascii","ignore")
    st = st.replace("\\n","")
    #st = st.replace("\\N","")

    if st == "\\N":
      outlinks = []
      return (xmlline[1],outlinks)

    else:
      try:
	    root = et.fromstring(st)
    	    #print('@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@   Root ',type(root),'#############  ', root)
    
            rootlist = root.findall("./article/paragraph/sentence/bold/link/target")

    	    rootlist = rootlist + root.findall("./article/paragraph/template/params/extension/link/target")
    	    rootlist = rootlist + root.findall("./article/list/listitem/bold/link/target")
    
    	    for i in xrange(0,len(rootlist)):
      		outlinks.append(rootlist[i].text)
      		#print(rootlist[i].text)
    
    	    # Removes the duplicates
    	    outlinks = list(set(outlinks))


      except:
     	    print('Malformed XML') 
     	    print(st, len(st))
            #outFile.write('Exception' + '\n')


      return (xmlline[1],outlinks)





def getRank(outlnks,rnk):
    """Calculates the contributions of each Page to the rank of a URL"""
    numoutlnks = len(outlnks)
    for url in outlnks:
       yield(url, (rnk/numoutlnks))
   


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: pagerank <file> <iterations>", file=sys.stderr)
        exit(-1)
    #outFile = open('/home/smeera380/spark-1.6.0/outFile','a')

    # Initialize the spark context.
    sc = SparkContext(appName="PageRank")

    #lines = sc.textFile(sys.argv[1], 5)
    lines = sc.textFile("s3n://smeeras301/wexdataset")
    #freebase-wex-2010-07-05-articles1.tsv")
    
    # Parsing the WEX Dataset to the format: 'PageId   PageName   Outlinks'	
    links = lines.map(lambda ln:parseLine(ln))



    # Initializing ranks to 1.0
    ranks = links.map(lambda lnks : (lnks[0],1.0))

    # Computing the PageRank with fixed number of iterations
    for i in xrange(0,int(sys.argv[1])):
      #webpages = links.join(ranks)
      
      pagepoints = links.join(ranks).flatMap(lambda outlnks: getRank(outlnks[1][0],outlnks[1][1]))

     
      ranks = pagepoints.reduceByKey(add)
      #.mapValues(lambda rnk : rnk*0.85 + 0.15)
      #.takeOrdered(100,key=lambda r:-r[1])

    print('@@@@@@@@@@@@@@@@@@@@@@@@@@@@@  Ranks')
    print(type(ranks))
    ranks.saveAsTextFile("/home/smeera380/spark-1.6.0/PurePageRank")

    finalRanks= ranks.takeOrdered(100,key=lambda r:-r[1])
    print('@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@  Final Ranks')
    print(type(finalRanks))


    #finalRanks = ranks.collect().takeOrdered(100, key= lambda r:-r[1])
    #print(type(finalRanks))
    for (url,rn) in finalRanks:
       print(url,'\t',rn)

    
    sc.stop()
