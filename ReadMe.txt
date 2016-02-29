##############  Executing pure spark PageRank Algorithm. ##############
1. Create Amazon ec2 cluster via the spark-ec2 script.
  ./spark-ec2 -k <keyPairName> -i <KeyPairFile> --instance-type="m3.xlarge" -r us-east-1 --slaves 4 launch clustername

   The results attached used m3.xlarge cluster with 5 nodes (1 master and 4 slaves)

2. Transfer the python file to the Master node.
   scp -i <identity file> <python file to be transferred> root@<master host name>:~


3. In the remote master node, go to the directory where spark is installed and execute:
   ./bin/spark-submit --master <spark master url> <Python file path> <wiki data url in quotes> <number of iterations>

   In the attached results, the number of iterations given were 5, as it was observed that the results were getting converged at that point.

4. The results are attached along with the submission file (PageRank-PureSparkResults.txt)


####################  Executing the PageRank Graphx implementation. #################
1. Install sbt, if not already installed.

2. Configure the build.sbt file, with the below details:

   name := "spark-1.6.0"
   version := "1.0"
   scalaVersion := "2.10.4"
   libraryDependencies ++= Seq("org.apache.spark" %% "spark-core" % "1.6.0","org.apache.spark" %% "spark-graphx" % "1.6.0")

3. Create directory structure as required by sbt to build the scala application(GraphxPageRank.scala file).
   -Create directory, say sparkapp
   - cd sparkapp
     mkdir -p src/main/scala
     cp <scala file name> /sparkspp/src/main/scala

4. Now cd to sparkapp and execute the following:
   sbt package
   
   This command would build the scala application, resolving all the dependencies and creates a jar file in : sparkapp/target/scala-2.10

5. Create an Amazon ec2 cluster and transfer the above created jar file to the master node.
   The results attached ran on m3.xlarge 5 node cluster.

   scp -i <identity file> <jar file to be transferred> root@<master host name>:~

6. Go to the directory where spark is installed and execute the jar file using the below command:
   ./bin/spark-submit --class GraphxPageRank sparkapp/target/scala-2.10/spark-1-6-0_2.10-1.0.jar <wikidata url in quotes> <number of iterations>

7. The results have been attached along with the submissions file(GraphXPageRankResults)

#################  Executing the University Ranking Graphx implementation. ##################
1. Follow steps up until 5 (given above), to transfer the built jar file, for the scala application (UniRanking.scala).

2. This application includes an input file, which is the list of Universities from across the world. This list is then filtered from the WEX dataset to get the University ranking. Transfer this input file to the HDFS. This input file has been attached along with the submissions file.

   cd /root/ephemeral-hdfs/bin
   ./hadoop fs -mkdir /home/smeera/asg2/inputs
   ./hadoop fs -put /root/universitylist.txt /home/smeera/asg2/inputs  (assuming we have already transferred the Universitylist.txt file to the Master node via scp command as shown earlier).
   ./hadoop fs -ls /home/smeera/asg2/inputs

3. Now execute the scala application to list the top ranking university , using the WEX dataset dump and the university list input file, using the below command:
 
   ./bin/spark-submit --class UniRanking <path to the jar file> <wikidata url in quotes> <Input university list> <number of iterations>

4. The results of the same have been attached along with the final submission zip file (UniversityRankingResults_reducedversion). The results have been extracted for a smaller version of the WEX dataset(0.5GB) and hence could extract only a few colleges and their ranks.

   
   