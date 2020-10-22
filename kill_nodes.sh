echo "Killing all the components"


echo "Killing storage node #1 at orion02:26042"
ssh orion02 tmux kill-session -t storagenode

echo "Killing storage node #2 at orion03:26043"
ssh orion03 tmux kill-session -t storagenode

echo "Killing storage node #3 at orion04:26044"
ssh orion04 tmux kill-session -t storagenode

echo "Killing storage node #4 at orion05:26045"
ssh orion05 tmux kill-session -t storagenode

echo "Killing storage node #5 at orion06:26046"
ssh orion06 tmux kill-session -t storagenode

echo "Killing storage node #6 at orion07:26047"
ssh orion07 tmux kill-session -t storagenode

echo "Killing storage node #7 at orion08:26048"
ssh orion08 tmux kill-session -t storagenode

echo "Killing storage node #8 at orion09:26049"
ssh orion09 tmux kill-session -t storagenode

echo "Killing storage node #9 at orion10:26050"
ssh orion10 tmux kill-session -t storagenode

echo "Killing storage node #10 at orion11:26051"
ssh orion11 tmux kill-session -t storagenode

echo "Killing storage node #11 at orion12:26052"
ssh orion12 tmux kill-session -t storagenode