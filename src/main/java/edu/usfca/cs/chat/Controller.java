
package edu.usfca.cs.chat;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import edu.usfca.cs.chat.Utils.FileUtils;
import edu.usfca.cs.chat.net.MessagePipeline;
import edu.usfca.cs.chat.net.ServerMessageRouter;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

@ChannelHandler.Sharable
public class Controller
        extends SimpleChannelInboundHandler<DfsMessages.MessagesWrapper> {


    // storage node map with key as IP and DataNodeMetadata
    private ConcurrentMap<String, DfsMessages.DataNodeMetadata> activeStorageNodes;
    private static ConcurrentHashMap<String, Channel> channelMap;
    private static ConcurrentHashMap<String, DfsMessages.HeartBeat> heartBeatMap;


    // routing table key is dir name
    // routing table value is map of Bloom Filter and associated DataNodeMetaData
    private ConcurrentMap<String, ConcurrentMap<BloomFilter, DfsMessages.DataNodeMetadata>> routingTable;

    // todo decide the m and k for master bloom filter
    BloomFilter masterBloomFilter;
    ServerMessageRouter messageRouter;

    // map of node ips to their corresponding bloom filters
    ConcurrentMap<String, BloomFilter> nodeToBF;

//    public static int CHUNK_SIZE = 128; // MB
    public static int CHUNK_SIZE ; // 10kb

    private static volatile FileSystem fileSystem;


    int m = 1000000;
    int k = 20;

    public Controller() {
        activeStorageNodes = new ConcurrentHashMap<>();
        routingTable = new ConcurrentHashMap<>();
        masterBloomFilter = new BloomFilter(m, k);
        channelMap = new ConcurrentHashMap<>();
        heartBeatMap = new ConcurrentHashMap<>();
        nodeToBF = new ConcurrentHashMap<>();
    }

    public void start(int port, int chunkSize) throws IOException {
        messageRouter = new ServerMessageRouter(this);
        messageRouter.listen(port);
        CHUNK_SIZE = chunkSize * 1024;
        System.out.println("Controller started on port " + port + "...");
    }

    public static void main(String[] args)
            throws IOException {
        Controller s = new Controller();
        s.start(Integer.parseInt(args[0]), Integer.parseInt(args[1]));
    }

    public Channel connectToNode(String hostname, Integer port) {
        try {
            EventLoopGroup workerGroup = new NioEventLoopGroup();
            MessagePipeline pipeline = new MessagePipeline(this);

            Bootstrap bootstrap = new Bootstrap()
                    .group(workerGroup)
                    .channel(NioSocketChannel.class)
                    .option(ChannelOption.SO_KEEPALIVE, true)
                    .handler(pipeline);

            System.out.println("Connecting to " + hostname + ":" + port);
            ChannelFuture cf = bootstrap.connect(hostname, port);
            cf.syncUninterruptibly();
            return cf.channel();
        }
        catch (Exception e) {
            System.out.println("Error connecting to node: " + hostname  + ":" + port);
        }
        return null;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        /* A connection has been established */
        InetSocketAddress addr
                = (InetSocketAddress) ctx.channel().remoteAddress();
        System.out.println("Connection established: " + addr);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        /* A channel has been disconnected */
        InetSocketAddress addr
                = (InetSocketAddress) ctx.channel().remoteAddress();
        String nodeAddr = ctx.channel().remoteAddress().toString().substring(1);
        System.out.println("Connection lost: " + nodeAddr);
        // remove from activeStorageNodes map
        removeNodeFromActiveStorage(nodeAddr);
        // get all nodes with replicas
        List<DfsMessages.DataNodeMetadata> replicaNodes = getNodesWithReplicas(nodeAddr);
        // remove down node from routing table after replicas retrieved
        removeNodeFromRoutingTable(nodeAddr);
        // message first that nodeAddr is down for cascading msging
        if(replicaNodes.size() > 0) startFaultTolerance(nodeAddr, replicaNodes);
    }

    private void removeNodeFromActiveStorage(String nodeAddr) {
        activeStorageNodes.forEach((ip, nodeMetadata) -> {
            String addr = nodeMetadata.getIp();
            if(addr.equals(nodeAddr)) {
                // key to delete is hostname
                activeStorageNodes.remove(addr);
                heartBeatMap.remove(addr);
                channelMap.remove(nodeMetadata.getIp());
            }
        });
    }

    private List<DfsMessages.DataNodeMetadata> getNodesWithReplicas(String nodeAddr) {
        Set<DfsMessages.DataNodeMetadata> replicaNodeSet = new HashSet<>();
        List<String> allKeys = new ArrayList<>(routingTable.keySet());

        for(int j = 0; j < allKeys.size(); j++) {
            String dirName = allKeys.get(j);
            ConcurrentMap<BloomFilter, DfsMessages.DataNodeMetadata> nodes = routingTable.get(dirName);
            List<DfsMessages.DataNodeMetadata> allNodes = new ArrayList<>(nodes.values());
            int idx = -1;
            for(int i = 0; i < allNodes.size(); i++) {
                // if down node is part of the list, we copy all the nodes to list of nodes we want to notify
                if(allNodes.get(i).getIp().equals(nodeAddr)) idx = i;
            }
            if (idx != -1) {
                // remove the node that went down from the list
                allNodes.remove(idx);
                replicaNodeSet = new HashSet<>(allNodes);
            }
        }

        return new ArrayList<>(replicaNodeSet);
    }

    private void removeNodeFromRoutingTable(String nodeAddr) {
        System.out.println("routing table before: " + routingTable);
        for(String dir : routingTable.keySet()) {
            BloomFilter bfToRemove = null;
            ConcurrentMap<BloomFilter, DfsMessages.DataNodeMetadata> nodes = routingTable.get(dir);
            for(BloomFilter bf : nodes.keySet()) {
                DfsMessages.DataNodeMetadata node = nodes.get(bf);
                System.out.println("node addr: " + nodeAddr);
                System.out.println("node ip: " + node.getIp());
                if(nodeAddr.equals(node.getIp())) {
                    bfToRemove = bf;
                }
            }
            if(bfToRemove != null) {
                // remove  node  from map
                nodes.remove(bfToRemove);
                // update routing table
                routingTable.put(dir, nodes);
            }
        }
    }

    private DfsMessages.OnNodeDown createOnNodeDownMsg(String ip, List<DfsMessages.DataNodeMetadata> replicaNodes) {
        return DfsMessages.OnNodeDown.newBuilder()
                .setIp(ip)
                .addAllAffectedNodes(replicaNodes)
                .build();
    }

    private void startFaultTolerance(String nodeAddr, List<DfsMessages.DataNodeMetadata> replicaNodes) {
        System.out.println("X-X-X Initiating Fault Tolerance Sequence for Node: X-X-X" + nodeAddr);
        try {
            DfsMessages.DataNodeMetadata node = replicaNodes.get(0);
            String ip = node.getIp().split(":")[0];

            System.out.println("X-X FT: sending first replication msg to " + ip + ':' + node.getPort());
            Channel ch = connectToNode(ip, node.getPort());
            System.out.println("ch is: " + ch);

            DfsMessages.MessagesWrapper wrapper = DfsMessages.MessagesWrapper.newBuilder()
                    .setDataNodeWrapper(DfsMessages.DataNodeMessagesWrapper.newBuilder()
                    .setOnNodeDown(createOnNodeDownMsg(nodeAddr, replicaNodes)).build())
                    .build();

            ch.writeAndFlush(wrapper);
        }
        catch (Exception e) {
            System.out.println("error initiating fault tolerance for node: " + nodeAddr);
        }
    }

    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx)
            throws Exception {
        /* Writable status of the channel changed */
    }

    @Override
    public void channelRead0(
            ChannelHandlerContext ctx, DfsMessages.MessagesWrapper msg) {
        DfsMessages.ControllerMessagesWrapper message = msg.getControllerWrapper();

        int messageType = message.getMsgCase().getNumber();

        System.out.println("INSIDE Ch Read messageType: " + messageType);

        switch(messageType){
            case 1: // File Request - STORE/RETRIEVE/STATUS
                try {
                    System.out.println("Received a file request for " + message.getFileRequest().getFilepath());
                    System.out.println("Request type is  " + message.getFileRequest().getType().name());
                    //get list of nodes client can write to and reply to client with FileResponse
                    if(message.getFileRequest().getType().equals(DfsMessages.FileRequest.Type.STORE))
                        storeFile(ctx, message.getFileRequest());
                    else if(message.getFileRequest().getType().equals(DfsMessages.FileRequest.Type.RETRIEVE))
                        retrieveFile(ctx, message.getFileRequest());
                    else
                        returnHeartBeats(ctx);
                } catch (Exception e) {
                    System.out.println("An error in Controller while reading FileRequest " + e);
                    e.printStackTrace();
                }
                break;
            case 3: // FileChunkHeader - incoming replica maintenence request from storage node directly
                DfsMessages.FileChunkHeader headerMsg = message.getFileChunkHeader();
                int totalChunks = headerMsg.getTotalChunks();
                String nodeDownIp = headerMsg.getNodeIp();
                List<DfsMessages.DataNodeMetadata> replicas = headerMsg.getReplicasList();

                System.out.println("Incoming Request to return nodes to store total chunks: " + totalChunks);
                List<DfsMessages.DataNodeMetadata> availableNodes = getNodesToStoreFile(totalChunks);

                // create msg with all nodes replication can happen on
                DfsMessages.MessagesWrapper wrapper = DfsMessages.MessagesWrapper.newBuilder().setDataNodeWrapper(
                        DfsMessages.DataNodeMessagesWrapper.newBuilder()
                                .setFileChunkHeader(DfsMessages.FileChunkHeader.newBuilder()
                                        .setTotalChunks(0)
                                        .setFilepath("")
                                        .setNodeIp(nodeDownIp)
                                        .addAllMaintenanceNodes(replicas) // these are list of cascading nodes that need to be maintained
                                        .addAllReplicas(availableNodes))).build();

                ctx.channel().writeAndFlush(wrapper);
                break;
            case 5: // Heart Beat
                try {
                    DfsMessages.DataNodeMetadata info = message.getHeartBeat().getNodeMetaData();
                    // update activeStorageNode for info
                    String nodeIp = info.getIp();
                    heartBeatMap.put(nodeIp, message.getHeartBeat());
                    activeStorageNodes.put(nodeIp, info);
                    System.out.println("Node: " + info.getHostname() + " alive at port: " + info.getPort() + " with memory: " + info.getMemory());
                } catch (Exception e) {
                    System.out.println("An error in Controller while reading HeartBeat from node: " + e);
                }
                break;
            case 6: // Intro Message
                try {
                    DfsMessages.DataNodeMetadata IntroMsg = message.getIntroMessage();
                    // print controller
                    printMsg(message);
                    // add to active storage nodes
                    activeStorageNodes.put(IntroMsg.getIp(), IntroMsg);
                    channelMap.put(IntroMsg.getIp(), ctx.channel());
                    nodeToBF.put(IntroMsg.getIp(), new BloomFilter(m, k));
                } catch (Exception e) {
                    System.out.println("An error in Controller while reading DataNodeMetaData");
                }
                break;
            case 8: // UpdateRoutingTable
                System.out.println("T_T_T UPDATING ROUTING TABLE T_T_T");
                try {
                    DfsMessages.UpdateRoutingTable updateMsg = message.getUpdateRoutingTable();
                    List<String> filesUpdated = updateMsg.getDirpathList();
                    System.out.println("Files updated: " + filesUpdated);
                    String nodeIp = updateMsg.getNodeIp();

                    // get node bloomfilter
                    BloomFilter bf = nodeToBF.get(nodeIp);
                    for(String dir : filesUpdated) {
                        // update bloomfilter
                        bf.put(dir.getBytes());
                    }
                } catch (Exception e) {
                    System.out.println("An  error in Controller while reading UpdateRoutingTable");
                }
            case 9: //file system request
                System.out.println("Received filesytem request" + message.getFsRequest());
                String path = message.getFsRequest().getFilepath();
                if (path.endsWith("/"))
                    path = path.substring(path.length() - 1);
                Set<String> paths = new HashSet<>();
                Set<Channel> nodesToQuery = new HashSet<>();
                System.out.println("-------gotfile-----");
                String finalPath1 = path;
                this.routingTable.forEach((k, v)->{
//                    System.out.println(k);
                    if(k.startsWith(finalPath1)){
                        if(k.equals(finalPath1)){
                            v.values().forEach(node-> {
                                nodesToQuery.add(channelMap.get(node.getIp()));
                            });
//                            System.out.println("query table for " + k );
                        } else {
                            int startIndex = finalPath1.length();
                            String rem = k.substring(startIndex);
                            int endIndex = rem.indexOf("/");
                            String finalPath = endIndex == -1 ? rem : rem.substring(0, endIndex);
                            paths.add("\t" + finalPath + "/");
//                            System.out.println("\t" + finalPath+ "/");
                        }
                    }
                });
                System.out.println("---gotfile----");
                if(nodesToQuery.size() > 0){
                    nodesToQuery.forEach(channel -> {
                        channel.writeAndFlush(DfsMessages.MessagesWrapper.newBuilder().setDataNodeWrapper(DfsMessages.DataNodeMessagesWrapper.newBuilder().setFsRequest(message.getFsRequest())).build());
                    });
                    fileSystem = new FileSystem(nodesToQuery.size(), ctx.channel(), paths);
                } else {
                    ctx.channel().writeAndFlush(DfsMessages.MessagesWrapper.newBuilder().setClientWrapper(DfsMessages.ClientMessagesWrapper.newBuilder()
                            .setFsResponse(DfsMessages.FileSystemResponse.newBuilder().setInfo(String.join("\n\t", paths)))).build());
                }
                break;
            case 10: // file system response from datanodes
                System.out.println("Recived filesystem response " + message.getFsResponse());
                fileSystem.incrementAndCheck(Arrays.asList(message.getFsResponse().getInfo().split("\t\n")));
                break;
            default:
                System.out.println("Default switch case in channel read of controller, messageType: " + messageType);
                break;
        }

    }

    private void returnHeartBeats(ChannelHandlerContext ctx) {
        System.out.println("Outputting stuff " + heartBeatMap.values());
        ctx.channel().writeAndFlush(DfsMessages.MessagesWrapper.newBuilder().setClientWrapper(DfsMessages.ClientMessagesWrapper.newBuilder().setNodeStatus(DfsMessages.NodeStatus.newBuilder().addAllNodeMetadata(heartBeatMap.values()))).build());
    }

    private void printMsg(DfsMessages.ControllerMessagesWrapper message) {
        System.out.println("**** New Storage Node connected ****");
        System.out.println("Hostname: " + message.getIntroMessage().getHostname());
        System.out.println("Port name: " + message.getIntroMessage().getPort());
        System.out.println("IP: " + message.getIntroMessage().getIp());
        System.out.println("Available Memory: " + message.getIntroMessage().getMemory());
        System.out.println("X - X - X - X - X - X - X - X - X - X");
    }

    private List<DfsMessages.DataNodeMetadata> getNodesToStoreFile(int chunks) {
        List<DfsMessages.DataNodeMetadata> nodes = new ArrayList<>();

        Set<String> keyset = activeStorageNodes.keySet();
        System.out.println(activeStorageNodes);
        for(String hostname : keyset) {
            DfsMessages.DataNodeMetadata node = activeStorageNodes.get(hostname);
            if(node.getMemory() > CHUNK_SIZE) {
                nodes.add(node);
            }
        }

        // this returns AT-MAX as many nodes as the number of chunks of file

        if(nodes.size() <= chunks) return nodes;
        return nodes.subList(0, chunks);
    }

    private DfsMessages.MessagesWrapper createClientFileResponseMsg(String systemFilePath, String dfsFilePath, List<DfsMessages.DataNodeMetadata> availableNodes, DfsMessages.FileRequest.Type type, boolean shouldOverwrite) {
        return DfsMessages.MessagesWrapper.newBuilder().setClientWrapper((DfsMessages.ClientMessagesWrapper.newBuilder()
                .setFileResponse(DfsMessages.FileResponse.newBuilder()
                .setSystemFilePath(systemFilePath)
                .setDfsFilePath(dfsFilePath)
                        .setType(type.equals(DfsMessages.FileRequest.Type.STORE) ? DfsMessages.FileResponse.Type.STORE : DfsMessages.FileResponse.Type.RETRIEVE )
                .addAllDataNodes(availableNodes)
                .setShouldOverwrite(shouldOverwrite))
                )).build();
    }

    /**
     * checks to see if directory exists on file request for store and retreive
     */
    private ConcurrentMap<BloomFilter, DfsMessages.DataNodeMetadata> getDirBloomFilters(DfsMessages.FileRequest message) {
        String dfsFilePath = message.getFilepath();

        // get absolute parent directory path without filename
        String dirPath = FileUtils.getDirPath(dfsFilePath);

        // check if directory exists in bloom filter
        return routingTable.getOrDefault(dirPath, null);
    }


    private void retrieveFile(ChannelHandlerContext ctx, DfsMessages.FileRequest message) {
        DfsMessages.MessagesWrapper wrapper;
        String dfsFilePath = message.getFilepath();
        ConcurrentMap<BloomFilter, DfsMessages.DataNodeMetadata> bfMap = getDirBloomFilters(message);

        try {
            if(bfMap == null) {
                // todo file does not exist
                wrapper = createClientFileResponseMsg("", dfsFilePath, null, message.getType(), false);
            }
            else {
                    // get all storage nodes corresponding to the bloom filters
                    List<DfsMessages.DataNodeMetadata> availableNodes = new ArrayList<>(bfMap.values());
                    // send FileResponse with storage nodes that potentially have file chunks
                    wrapper = createClientFileResponseMsg("", dfsFilePath, availableNodes, message.getType(), false);
                }
            ctx.channel().writeAndFlush(wrapper);
        }
        catch(Exception e) {
            System.out.println("error in controller during retrieveFile: " + e);
        }
    }

    private void storeFile(ChannelHandlerContext ctx, DfsMessages.FileRequest message) {
        List<DfsMessages.DataNodeMetadata> availableNodes;
        // 1. check if it exists in routing table
        String dfsFilePath = message.getFilepath();
        String systemFilePath = message.getDirectory();


        System.out.println("dfs file path is: " + dfsFilePath);
        System.out.println("system file path is: " + message.getDirectory());

        long size = message.getSize();
        int chunks = message.getNumChunks();
        boolean shouldOverwrite = false;

        // get absolute parent directory path without filename
        String dirPath = FileUtils.getDirPath(dfsFilePath);

        System.out.println("parent dir path is: " + dirPath);

        // check if directory exists in bloom filter
        ConcurrentMap<BloomFilter, DfsMessages.DataNodeMetadata> bfMap = routingTable.getOrDefault(dirPath, null);

        // 2. if no, add to routing table, FS and master bloom filter
        if(bfMap == null) {
            System.out.println("DIR DOES NOT EXIST: " + dirPath);

            ConcurrentMap<BloomFilter, DfsMessages.DataNodeMetadata> map = new ConcurrentHashMap<>();
            // before adding to routing table, figure out active storage node associated with new file

            availableNodes = getNodesToStoreFile(chunks);
            int numNodesAvailable = availableNodes.size();

            if(numNodesAvailable == 0) {
                // todo create and return a new error message to client if all storage nodes are full
            }


            // todo add to FS

            // now that we have space to add file, add directory path to master BF
            byte[] data = dirPath.getBytes();
            masterBloomFilter.put(data);

            int i;
            BloomFilter bf;
            for(i = 0; i < numNodesAvailable; i++) {
                bf = nodeToBF.get(availableNodes.get(i).getIp());
                map.put(bf, availableNodes.get(i));
            }

            // insert entry into routingTable
            routingTable.put(dirPath, map);

        }
        else {
            System.out.println("DIR PATH DOES EXIST: " + dirPath);
            // 3. if yes, get all bloom filters that might have file and return to client with overwrite flag turned on
            availableNodes = new ArrayList<>(bfMap.values());
            // set overWrite flag to true;
            shouldOverwrite = true;
        }

        // send message to clients with all available nodes
        DfsMessages.MessagesWrapper wrapper = createClientFileResponseMsg(systemFilePath, dfsFilePath, availableNodes, message.getType(), shouldOverwrite);
        ctx.channel().writeAndFlush(wrapper);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
    }
}