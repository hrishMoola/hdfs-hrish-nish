#!/usr/bin/env bash

# Kill all the components first
./kill_nodes.sh

echo ""
echo "------~~~~~~~~~~~~~~~~~~~~------"
echo "Starting all the components"

echo "Starting storage node #1 at orion02:27042"
ssh orion02 tmux new-session -d -s "storagenode" 'java -Xms256m -Xmx2048m -cp dfs-1.0.jar edu.usfca.cs.chat.StorageNode /bigdata/$(whoami)/sn_orion01 localhost 27042 orion01 27500 5000000 128000 > log1'

echo "Starting storage node #2 at orion03:27043"
ssh orion03 tmux new-session -d -s "storagenode" 'java -Xms256m -Xmx2048m -cp dfs-1.0.jar edu.usfca.cs.chat.StorageNode /bigdata/$(whoami)/sn_orion02 localhost 27043 orion01 27500 5000000 12800 > log2'

echo "Starting storage node #3 at orion04:27044"
ssh orion04 tmux new-session -d -s "storagenode" 'java -Xms256m -Xmx2048m -cp dfs-1.0.jar edu.usfca.cs.chat.StorageNode /bigdata/$(whoami)/sn_orion03 localhost 27044 orion01 27500 5000000 12800 > log3'

echo "Starting storage node #4 at orion05:27045"
ssh orion05 tmux new-session -d -s "storagenode" 'java -Xms256m -Xmx2048m -cp dfs-1.0.jar edu.usfca.cs.chat.StorageNode /bigdata/$(whoami)/sn_orion04 localhost 27045 orion01 27500 5000000 12800 > log4'

echo "Starting storage node #5 at orion06:27046"
ssh orion06 tmux new-session -d -s "storagenode" 'java -Xms256m -Xmx2048m -cp dfs-1.0.jar edu.usfca.cs.chat.StorageNode /bigdata/$(whoami)/sn_orion05 localhost 27046 orion01 27500 5000000 12800 > log5'

echo "Starting storage node #6 at orion07:27047"
ssh orion07 tmux new-session -d -s "storagenode" 'java -Xms256m -Xmx2048m -cp dfs-1.0.jar edu.usfca.cs.chat.StorageNode /bigdata/$(whoami)/sn_orion06 localhost 27047 orion01 27500 5000000 12800 > log6'

echo "Starting storage node #7 at orion08:27048"
ssh orion08 tmux new-session -d -s "storagenode" 'java -Xms256m -Xmx2048m -cp dfs-1.0.jar edu.usfca.cs.chat.StorageNode /bigdata/$(whoami)/sn_orion07 localhost 27048 orion01 27500 5000000 12800 > log7'

echo "Starting storage node #8 at orion09:27049"
ssh orion09 tmux new-session -d -s "storagenode" 'java -Xms256m -Xmx2048m -cp dfs-1.0.jar edu.usfca.cs.chat.StorageNode /bigdata/$(whoami)/sn_orion08 localhost 27049 orion01 27500 5000000 12800 > log8'

echo "Starting storage node #9 at orion10:27050"
ssh orion10 tmux new-session -d -s "storagenode" 'java -Xms256m -Xmx2048m -cp dfs-1.0.jar edu.usfca.cs.chat.StorageNode /bigdata/$(whoami)/sn_orion09 localhost 27050 orion01 27500 5000000 12800 > log9'

echo "Starting storage node #10 at orion11:27051"
ssh orion11 tmux new-session -d -s "storagenode" 'java -Xms256m -Xmx2048m -cp dfs-1.0.jar edu.usfca.cs.chat.StorageNode /bigdata/$(whoami)/sn_orion10 node10 27051 orion01 27500 5000000 128000 > log10'

echo "Starting storage node #11 at orion12:27052"
ssh orion12 tmux new-session -d -s "storagenode" 'java -Xms256m -Xmx2048m -cp dfs-1.0.jar edu.usfca.cs.chat.StorageNode /bigdata/$(whoami)/sn_orion11 node11 27052 orion01 27500 5000000 128000 > log11'