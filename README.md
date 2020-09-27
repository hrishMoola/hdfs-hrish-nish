# Project 1: POSIX DFS With Probabilistic Routing

See the project spec here: https://www.cs.usfca.edu/~mmalensek/cs677/assignments/project-1.html

Run the datanode with this
java -cp target/dfs-1.0-shaded.jar edu.usfca.cs.chat.StorageNode storage1 alpha 8000 localhost 7777 50
last arg is virtual memory in kilobytes. can be made into 128mb

Run the client with this
java -cp target/dfs-1.0-shaded.jar edu.usfca.cs.chat.Client localhost 7777 client1 10

last arg is chunk size in kb
//store <local filepath> <dfs filepath>


Start Controller with 
java -cp target/dfs-1.0-shaded.jar edu.usfca.cs.chat.Controller 7777