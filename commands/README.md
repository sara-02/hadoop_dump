### Recompile the bash after making changes
$ . .bashrc

### Command to reset date
$ sudo date -s 'yyyy-mm-dd hr:mm:ss'

### commands to run java8_in.sh  
$ chmod 777 java8_in.sh 

$ ./java8_in.sh 

###BASIC HADOOP COMMANDS

1. $ su - hduser(or whatever is the user name)
2. $ cd $HADOOP_HOME
3. $ bin/hadoop namenode -format
4. $ bin/start-all.sh
5. $ bin/hadoop dfs -ls
6. $ bin/hadoop dfs -copyFromLocal \<source\> \<destination\>
7. $ bin/hadoop jar \<jar_name>\ \<program name>\ \<input path in hdfs\> \<output path in hdfs\>
8. $bin/stop-all.sh

