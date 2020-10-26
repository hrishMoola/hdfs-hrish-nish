# Project 1: POSIX DFS With Probabilistic Routing

Project Design Document - https://docs.google.com/document/d/11vsDLNDCNxHHZwioTigjVFluosOalx0f9OdnLPHTT04/edit

See the project spec here: https://www.cs.usfca.edu/~mmalensek/cs677/assignments/project-1.html

Run the datanode with this
java -cp dfs-1.0.jar edu.usfca.cs.chat.StorageNode  /bigdata/nmehta6/storage1 alpha 8000 orion01 7777 5000000 128000
second last arg is virtual memory in kilobytes. can be made into 128mb
last arg is chunk size in kb


Run the client with this
java -cp dfs-1.0.jar edu.usfca.cs.chat.Client orion01 7777 client1 128000
last arg is chunk size in kb

//store <local filepath> <dfs filepath>


Start Controller with 
java -cp target/dfs-1.0-shaded.jar edu.usfca.cs.chat.Controller 7777

java -cp original-dfs-1.0.jar edu.usfca.cs.chat.Controller 7777

!! Make sure to create the storage folders for datanodes and cache folder for client before start-up
