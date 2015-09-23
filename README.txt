Execution steps:
1. clean package: "sbt clean package" form the project module
2. run by options:
\spark-1.4.0-bin-hadoop2.6\bin\spark-submit --class "<class name with package info>" --master "<cluster name with number of nodes>" <target jar>
 Example: \spark-1.4.0-bin-hadoop2.6\bin\spark-submit --class "wordcount.WordCount" --master "local[4]"
target\scala-2.11\spark-workout_2.11-1.0.jar
