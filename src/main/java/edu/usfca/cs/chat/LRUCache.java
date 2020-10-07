package edu.usfca.cs.chat;

import java.util.*;
import java.util.stream.Collectors;

public class LRUCache {
    //0 hostname, 1 - memory rem
    PriorityQueue<String[]> queue = null;
    Map<String, Integer> nodeAge;
    Map<String, Integer> nodeMemoryConsumption;
//    int globalAge = 0;
    int chunkSize;

    public LRUCache(int chunkSize)
    {
        this.chunkSize = chunkSize;
        nodeAge = new HashMap<>();
        queue = new PriorityQueue<>((a, b) -> {
            if(nodeAge.get(a[0]).equals(nodeAge.get(b[0])))
                return new Integer(b[1]).compareTo(Integer.parseInt(a[1]));
            return nodeAge.get(a[0]).compareTo(nodeAge.get(b[0]));
        });
        nodeMemoryConsumption = new HashMap<>();
    }

    public String get()
    {
        String[] latestNode = queue.poll();
        System.out.println("nodeAge = " + nodeAge);
        System.out.println("latestNode = " + Arrays.toString(latestNode));
        if(latestNode != null){
            int currAge = nodeAge.get(latestNode[0]) + 1;
            int remMory = Integer.parseInt(latestNode[1]) - chunkSize;
            if(remMory >= chunkSize){
                String[] modifiedNode = new String[]{latestNode[0], String.valueOf(remMory)};
                nodeAge.put(modifiedNode[0], currAge);
                queue.offer(modifiedNode);
            }
            return latestNode[0];
        }
        return null;
    }

    public List<String> getWithReplicas()
    {
        List<String> nodes = new ArrayList<>();

        for(int i = 0; i <3; i++){
            String[] latestNode = queue.poll();
//            System.out.println("nodeAge = " + nodeAge);
//            System.out.println("latestNode = " + Arrays.toString(latestNode));
            if(latestNode != null){
                int currAge = nodeAge.get(latestNode[0]) + 1;
                int remMory = Integer.parseInt(latestNode[1]) - chunkSize;
                if(remMory >= chunkSize){
                    String[] modifiedNode = new String[]{latestNode[0], String.valueOf(remMory)};
                    nodeAge.put(modifiedNode[0], currAge);
                    queue.offer(modifiedNode);
                }
                nodes.add(latestNode[0]);
            }
        }
        return nodes;
    }
    public void put(String ip, int remMemory)
    {
        if(nodeAge.containsKey(ip))
            return;
        nodeAge.put(ip, 0);
        queue.add(new String[]{ip, String.valueOf(remMemory)});
    }

    public void addAll(List<DfsMessages.DataNodeMetadata> nodeList){
        nodeList.forEach(node -> this.put(node.getIp(), node.getMemory()));
    }

    public List<String> getUsedNodes(){
        return nodeAge.entrySet().stream().filter(entry -> entry.getValue() > 0).map(Map.Entry::getKey).collect(Collectors.toList());
    }


    public Map<String, Integer> getRemainingMemory(){
//        return ;
        return queue.stream().collect(Collectors.toMap(entry-> entry[0], entry-> Integer.parseInt(entry[1])));
    }

    public static void main(String[] args) {
        LRUCache lruCache = new LRUCache(10);

//        lruCache.put("node1", 30);
        lruCache.put("node1", 100);
        lruCache.put("node2", 20);
        lruCache.put("node3", 20);

        int totalSize = 40;
        while(totalSize > 0) {
            totalSize -= 10;
            List<String> host = lruCache.getWithReplicas();
            if (host == null)
                break;
            System.out.println(host);
            System.out.println("----");
        }
        System.out.println("lruCache = " + lruCache.getRemainingMemory());
    }

}
