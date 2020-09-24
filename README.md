# Project 1: POSIX DFS With Probabilistic Routing

See the project spec here: https://www.cs.usfca.edu/~mmalensek/cs677/assignments/project-1.html

Run the datanode with this
java -cp target/dfs-1.0-shaded.jar edu.usfca.cs.chat.DataNode storage1 localhost 7777 trial 43


Run the client with this
java -cp target/dfs-1.0-shaded.jar edu.usfca.cs.chat.Client localhost 7777 client1

//store <local filepath> <dfs filepath>