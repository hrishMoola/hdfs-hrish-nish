#!/usr/bin/env bash

# Kill all the components first
kill_nodes.sh

mvn install
mvn package

echo ""
echo "------~~~~~~~~~~~~~~~~~~~~------"
echo "Starting all the components"

echo "Starting storage node #1 at orion02:26042"
ssh orion02 tmux new-session -d -s "storagenode" 'java -Xms256m -Xmx2048m -cp CS677/dfs-1.0.jar edu.usfca.cs.chat.StorageNode /bigdata/$(whoami)/sn_orion02 localhost 26042 orion01 26500 5000000 128000'

echo "Starting storage node #2 at orion03:26043"
ssh orion03 tmux new-session -d -s "storagenode" 'java -Xms256m -Xmx2048m -cp CS677/dfs-1.0.jar edu.usfca.cs.chat.StorageNode /bigdata/$(whoami)/sn_orion03 localhost 26043 orion01 26500 5000000 12800'

echo "Starting storage node #3 at orion04:26044"
ssh orion04 tmux new-session -d -s "storagenode" 'java -Xms256m -Xmx2048m -cp CS677/dfs-1.0.jar edu.usfca.cs.chat.StorageNode /bigdata/$(whoami)/sn_orion04 localhost 26044 orion01 26500 5000000 12800'

echo "Starting storage node #4 at orion05:26045"
ssh orion05 tmux new-session -d -s "storagenode" 'java -Xms256m -Xmx2048m -cp CS677/dfs-1.0.jar edu.usfca.cs.chat.StorageNode /bigdata/$(whoami)/sn_orion05 localhost 26045 orion01 26500 5000000 12800'

echo "Starting storage node #5 at orion06:26046"
ssh orion06 tmux new-session -d -s "storagenode" 'java -Xms256m -Xmx2048m -cp CS677/dfs-1.0.jar edu.usfca.cs.chat.StorageNode /bigdata/$(whoami)/sn_orion06 localhost 26046 orion01 26500 5000000 12800'

echo "Starting storage node #6 at orion07:26047"
ssh orion07 tmux new-session -d -s "storagenode" 'java -Xms256m -Xmx2048m -cp CS677/dfs-1.0.jar edu.usfca.cs.chat.StorageNode /bigdata/$(whoami)/sn_orion07 localhost 26047 orion01 26500 5000000 12800'

echo "Starting storage node #7 at orion08:26048"
ssh orion08 tmux new-session -d -s "storagenode" 'java -Xms256m -Xmx2048m -cp CS677/dfs-1.0.jar edu.usfca.cs.chat.StorageNode /bigdata/$(whoami)/sn_orion08 localhost 26048 orion01 26500 5000000 12800'

echo "Starting storage node #8 at orion09:26049"
ssh orion09 tmux new-session -d -s "storagenode" 'java -Xms256m -Xmx2048m -cp CS677/dfs-1.0.jar edu.usfca.cs.chat.StorageNode /bigdata/$(whoami)/sn_orion09 localhost 26049 orion01 26500 5000000 12800'

echo "Starting storage node #9 at orion10:26050"
ssh orion10 tmux new-session -d -s "storagenode" 'java -Xms256m -Xmx2048m -cp CS677/dfs-1.0.jar edu.usfca.cs.chat.StorageNode /bigdata/$(whoami)/sn_orion10 localhost 26050 orion01 26500 5000000 12800'

echo "Starting storage node #10 at orion11:26051"
ssh orion11 tmux new-session -d -s "storagenode" 'java -Xms256m -Xmx2048m -cp CS677/dfs-1.0.jar edu.usfca.cs.chat.StorageNode /bigdata/$(whoami)/sn_orion11 node10 26051 orion01 26500 5000000 128000'

echo "Starting storage node #11 at orion12:26052"
ssh orion12 tmux new-session -d -s "storagenode" 'java -Xms256m -Xmx2048m -cp CS677/dfs-1.0.jar edu.usfca.cs.chat.StorageNode /bigdata/$(whoami)/sn_orion12 node11 26052 orion01 26500 5000000 128000'