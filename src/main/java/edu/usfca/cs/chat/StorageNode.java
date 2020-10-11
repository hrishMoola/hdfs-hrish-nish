package edu.usfca.cs.chat;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.protobuf.ByteString;
import edu.usfca.cs.chat.Utils.FileUtils;
import edu.usfca.cs.chat.net.MessagePipeline;
import edu.usfca.cs.chat.net.ServerMessageRouter;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;


import javax.xml.crypto.Data;

import static edu.usfca.cs.chat.Utils.FileUtils.getChunks;
import static edu.usfca.cs.chat.Utils.FileUtils.writeToFile;

@ChannelHandler.Sharable
public class StorageNode
    extends SimpleChannelInboundHandler<DfsMessages.MessagesWrapper> {

    ServerMessageRouter messageRouter;
    private String storagePath;
    private String controllerHostname;    // controller to connect to and send heartbeats to
    private int controllerPort;
    private String hostName;        // host and part where storage node will be listening as a server
    private int hostPort;

    private AtomicInteger tempMemory;
    private int chunkSize;

    private AtomicInteger totalStorageReqs;
    private AtomicInteger totalRetrievalReqs;

    private Channel controllerChannel;
    private String localAddr;

    Map<String, Channel> channelMap;
    Map<String, DfsMessages.FileChunkHeader> fileChunkMetadataMap;
    ScheduledExecutorService executorService;

    private static String REPLICA_DIR = "replica";
    private static String ORIGINAL_DIR = "original";

    public StorageNode(String[] args) {
        this.storagePath = args[0];
        this.hostName = args[1];
        this.hostPort = Integer.parseInt(args[2]);
        this.controllerHostname = args[3]; // sto
        this.controllerPort = Integer.parseInt(args[4]);
        this.tempMemory = new AtomicInteger(Integer.parseInt(args[5]) * 1024);
        this.chunkSize = Integer.parseInt(args[6]) * 1024;

        totalStorageReqs = new AtomicInteger(0);
        totalRetrievalReqs = new AtomicInteger(0);
        channelMap = new HashMap<>();
        fileChunkMetadataMap = new HashMap<>();
        executorService = Executors.newSingleThreadScheduledExecutor();
    }

    public void start() throws IOException {
        messageRouter = new ServerMessageRouter(this);
        messageRouter.listen(this.hostPort);
        System.out.println("Storage node " + this.hostName + " on port " + this.hostPort + "...");

        // before start clear directory contents
        FileUtils.clearDirectoryContents(storagePath);
        // on start connect to controller and send alive notification
        this.connect();
        this.sendIntroMessage();
    }

    public static void main(String[] args)
    throws IOException {
        if (args.length >= 4) {
            StorageNode s = new StorageNode(args);
            s.start();
        }
    }

    private void initiateHeartbeat() {
        Runnable runnable =
                () -> {
            DfsMessages.MessagesWrapper heartBeatWrapper = sendHeartbeat();
            ChannelFuture write = controllerChannel.writeAndFlush(heartBeatWrapper);
            write.syncUninterruptibly();
        };
        executorService.scheduleAtFixedRate(runnable, 5, 30, TimeUnit.SECONDS);
    }

    private DfsMessages.DataNodeMetadata buildDataNodeMetaData() {
        return DfsMessages.DataNodeMetadata.newBuilder()
                .setHostname(hostName)
                .setIp(localAddr)
                .setPort(hostPort)
                .setMemory(tempMemory.get()) // todo figure out directory size
                .build();
    }

    private void sendIntroMessage() {
        DfsMessages.MessagesWrapper wrapper = DfsMessages.MessagesWrapper.newBuilder().setControllerWrapper(DfsMessages.ControllerMessagesWrapper.newBuilder()
                .setIntroMessage(buildDataNodeMetaData())).build();

        ChannelFuture write = controllerChannel.writeAndFlush(wrapper);
        write.syncUninterruptibly();

        // Start a fixed rate thread to send heartbeats
        initiateHeartbeat();
    }

    private DfsMessages.MessagesWrapper sendHeartbeat() {
        return DfsMessages.MessagesWrapper.newBuilder().setControllerWrapper(DfsMessages.ControllerMessagesWrapper.newBuilder()
                .setHeartBeat(DfsMessages.HeartBeat.newBuilder()
                        .setNodeMetaData(buildDataNodeMetaData())
                        .setRetrieveCount(totalRetrievalReqs.intValue())
                        .setStoreCount(totalStorageReqs.intValue()))).build();

    }

    //connect to controller node upon startup
    public void connect() {
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        MessagePipeline pipeline = new MessagePipeline(this);

        Bootstrap bootstrap = new Bootstrap()
                .group(workerGroup)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .handler(pipeline);

        System.out.println("Storage node connecting to " + controllerHostname + ":" + controllerPort);
        ChannelFuture cf = bootstrap.connect(controllerHostname, controllerPort);
        cf.syncUninterruptibly();
        controllerChannel = cf.channel();
        // gets storage node's IP addr to send to controller and also removes the '/' prefix
        localAddr = this.controllerChannel.localAddress().toString().substring(1);
        System.out.println("LOCAL ADDR IS: " + localAddr);
        System.out.println(controllerChannel.remoteAddress().toString());
        System.out.println(hostName + " " + hostPort);
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
        System.out.println("Connection lost: " + addr);

        // todo: Shutdown executor service here?
    }

    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx)
    throws Exception {
        /* Writable status of the channel changed */
    }

    @Override
    public void channelRead0(
            ChannelHandlerContext ctx, DfsMessages.MessagesWrapper msg) {
        DfsMessages.DataNodeMessagesWrapper message = msg.getDataNodeWrapper();
        int messageType = message.getMsgCase().getNumber();
        System.out.println("SN msgtype rcvd: " + messageType);
        switch(messageType){
            case 1: // File Chunk
                //basically store the chunks being provided and send for replication to replicas
                try {
                    if(message.getFileChunk().getType().equals(DfsMessages.FileChunk.Type.REPLICA)){ //replica
                    writeToFile(message.getFileChunk(),storagePath,"/" + REPLICA_DIR + "/");
                    } else {//leader
                        writeToFile(message.getFileChunk(),storagePath,"/" + ORIGINAL_DIR + "/");
                        System.out.println("fileChunkMetadataMap = " + fileChunkMetadataMap.keySet());
                    }
                    if(message.getFileChunk().getType().equals(DfsMessages.FileChunk.Type.LEADER)) {
                        // replicas only for leader not for maintenance leader which is when a node with original file is down and is replicated
                        sendFileToReplicas(message.getFileChunk());
                    }
                    fileChunkMetadataMap.put(message.getFileChunk().getFilepath(), message.getFileChunk().getFilechunkHeader());
                    System.out.println(tempMemory.addAndGet(-this.chunkSize));
                    totalStorageReqs.incrementAndGet();
                    System.out.println("Successfully wrote to the file.");
                } catch (Exception e) {
                    System.out.println("An error occurred while writing to file in storage node.");
                    e.printStackTrace();
                }
                break;
            case 2: // File Ack
                try{
                    String filePath = message.getFileAck().getFilepath();
                    System.out.println("File ACK received in storage node for filepath: " + filePath);
                    //send metadata first if I have it?
                    //then send the chunks
                    DfsMessages.FileAck.Type ackMsgType = message.getFileAck().getType();
                    if (ackMsgType.equals(DfsMessages.FileAck.Type.FILE_RETRIEVAL)){
                        getAndSendChunks(ctx, message.getFileAck().getFilepath());
                        totalRetrievalReqs.incrementAndGet();
                    }
                    else if (ackMsgType.equals(DfsMessages.FileAck.Type.FILE_OVERWRITE)) {
                        String dirPath = message.getFileAck().getFilepath();
                        System.out.println("dirPath to overwrite is: " + dirPath);
                        //add that much memory that we are deleting
                        int numChunks  = (int) fileChunkMetadataMap.entrySet().stream().filter(entry -> entry.getKey().contains(dirPath + "-")).count();
                        tempMemory.addAndGet(numChunks * chunkSize);
                        //clear from metadata map
                        fileChunkMetadataMap.entrySet().removeIf(entry -> entry.getKey().contains(dirPath + "-"));
                        // delete directory from node to make space for new file
                        FileUtils.clearDirectoryContents(storagePath + "/" + ORIGINAL_DIR + "/" + dirPath);
                        FileUtils.clearDirectoryContents(storagePath + "/" + REPLICA_DIR + "/" + dirPath);
                        // todo do we need to overwrite .replica folders here as well?
                    }
                }catch (Exception e){
                    System.out.println("Error in fileAck in storage node: " + hostName + " at port: " + hostPort);
                    e.printStackTrace();
                }
                break;
            case 3: // chunk header from client.
                System.out.println("Received chunk header and replica information");
                DfsMessages.FileChunkHeader header = message.getFileChunkHeader();
                // these are the nodes that replication needs to happen on
                List<DfsMessages.DataNodeMetadata> nodesToReplicateOn = header.getReplicasList();
                // this is the list of nodes whose replication status needs to be maintained sequentially
                List<DfsMessages.DataNodeMetadata> nodesToMaintain = header.getMaintenanceNodesList();

                DfsMessages.AlreadyMaintainedChunks alreadyMaintainedChunks = header.getAlreadyMaintainedChunks();

                initiateReplicationMaintenence(ctx, header, alreadyMaintainedChunks);

                break;
            case 4: //replication status. not currently doing anything
                System.out.println("Replication Status of " + ctx.channel().remoteAddress().toString());
                System.out.println("Chunk num and success" + message.getReplicationStatus().getChunkNum() + " is " + message.getReplicationStatus().getSuccess());
                break;
            case 5: // ON NODE DOWN MSG
                DfsMessages.OnNodeDown nodeDown = message.getOnNodeDown();
                List<DfsMessages.DataNodeMetadata> replicas = message.getOnNodeDown().getAffectedNodesList();

                System.out.println("X X X X NODE DOWN DETECTED X X X X");
                System.out.println(nodeDown.getIp());
                System.out.println(" - - - - - - - - - - - - - - - - - ");
                sendTotalNodeChunksToReplicateMsg(ctx, nodeDown.getIp(), replicas);
                break;
            default:
                System.out.println("whaaaa");
                break;
        }

        //send back ok (?)
        /* Hmm... */

    }

    // need to send total chunks space needed and ip of node that went down so that when the  controller
    // responds it can respond back with the node down ip so we know which node's replication maintenece to initiate
    private DfsMessages.MessagesWrapper getControllerReqForStorage(int totalChunks, String nodeIp, List<DfsMessages.DataNodeMetadata> replicas) {
        return DfsMessages.MessagesWrapper.newBuilder().setControllerWrapper(
                DfsMessages.ControllerMessagesWrapper.newBuilder()
                .setFileChunkHeader(
                DfsMessages.FileChunkHeader.newBuilder()
                .setNodeIp(nodeIp)
                .addAllReplicas(replicas)
                .setTotalChunks(totalChunks)))
                .build();
    }

    private void sendTotalNodeChunksToReplicateMsg(ChannelHandlerContext ctx, String nodeIp, List<DfsMessages.DataNodeMetadata> replicas) {

        int totalChunksSpace = 0;
        // loop through  metadatamap and figure out total replicas and originals to replace
        System.out.println("FILE CHUNK METADATA: ");
        System.out.println(fileChunkMetadataMap);

        for(String key : fileChunkMetadataMap.keySet()) {
            // key is filname-<chunk_number>
            DfsMessages.FileChunkHeader header = fileChunkMetadataMap.get(key);
            List<DfsMessages.DataNodeMetadata> metaDataList = header.getReplicasList();
            for(DfsMessages.DataNodeMetadata node : metaDataList) {
                // if list contains node that went down, add to total chunks to replicate
                if(node.getIp().equals(nodeIp)) totalChunksSpace++;
            }
        }

        // make storage request to controller
        DfsMessages.MessagesWrapper wrapper = getControllerReqForStorage(totalChunksSpace, nodeIp, replicas);

        System.out.println("Sending Request for nodes to start replication on: ");
        System.out.println(wrapper);

        ctx.channel().writeAndFlush(wrapper);
    }

    private Map<String, List<String>> getFilesToBeReplicated(String nodeDownIp, DfsMessages.AlreadyMaintainedChunks alreadyMaintainedChunks) {
        List<String> originals = new ArrayList<>();
        List<String> replicas = new ArrayList<>();

        List<String> completedOrg = new ArrayList<>();
        List<String> completedRep = new ArrayList<>();

        if(alreadyMaintainedChunks != null) {
            completedOrg = alreadyMaintainedChunks.getOriginalChunksList();
            completedRep = alreadyMaintainedChunks.getRepeatedChunksList();
        }

        for(String dir : fileChunkMetadataMap.keySet()) {
            DfsMessages.FileChunkHeader valueHeader = fileChunkMetadataMap.get(dir);
            List<DfsMessages.DataNodeMetadata> replicatedNodes = valueHeader.getReplicasList();

            for(DfsMessages.DataNodeMetadata node : replicatedNodes) {
                if(nodeDownIp.equals(node.getIp())) {
                    if(replicatedNodes.size() == 1) {
                        // check to make sure that the original's replication has not already been done
                        if(!completedOrg.contains(dir)) originals.add(dir);
                    }
                    else {
                        // check to make sure that the replica's replication has not already been done
                        if(!completedRep.contains(dir)) replicas.add(dir);
                    }
                }
            }
        }

        Map<String, List<String>> map = new HashMap<>();
        map.put(ORIGINAL_DIR, originals);
        map.put(REPLICA_DIR, replicas);

        return map;
    }

    private String createPathFromChunkName(String chunkName, String filePresentInDir) {
        int idx = chunkName.lastIndexOf('-');
        String fileName = chunkName.substring(0,idx);
        String chunk = "/chunk" + chunkName.substring(idx);

        return storagePath + "/" + filePresentInDir + "/" + fileName + chunk;
    }

    private void replicateChunks(List<String> chunkList, List<DfsMessages.DataNodeMetadata> nodesToReplicateOn, DfsMessages.FileChunk.Type type) {
        int i = 0;
        int orgIdx = 0;

        while(orgIdx < chunkList.size()) {
            System.out.println("Replicating original file: " + chunkList.get(orgIdx));
            String chunkName = chunkList.get(orgIdx++);
            System.out.println("chunk name: " + chunkName);

            // find file which islocated in replica and create it's original on a new node
            String absFilePath = createPathFromChunkName(chunkName, REPLICA_DIR);
            String checksumPath = createPathFromChunkName(chunkName+"_checksum", REPLICA_DIR);

            System.out.println("checksum file path: " + checksumPath);
            System.out.println("abs path: " + absFilePath);
            File fileChunk = new File(absFilePath);

            System.out.println("File chunk is: " + fileChunk);

            if (i == nodesToReplicateOn.size()) i = 0;
            DfsMessages.DataNodeMetadata replicateTo = nodesToReplicateOn.get(i++);
            System.out.println("connecting to: " + replicateTo.getIp().split(":")[0] + ":" + replicateTo.getPort());
            Channel ch = connectToNode(replicateTo.getIp().split(":")[0], replicateTo.getPort());

            // todo edge case if chunk already exists here, don't replicate

            byte[] bytes = new byte[(int) fileChunk.length()];
            // funny, if can use Java 7, please uses Files.readAllBytes(path)
            try(FileInputStream fis = new FileInputStream(fileChunk)){
                fis.read(bytes);
            }
            catch(Exception e) {
                System.out.println("error converting file chunk to byte array");
            }

            // create filechunk msg
            // todo add sending file checksum
            DfsMessages.MessagesWrapper message = DfsMessages.MessagesWrapper.newBuilder()
                    .setDataNodeWrapper(DfsMessages.DataNodeMessagesWrapper.newBuilder()
                            .setFileChunk(DfsMessages.FileChunk.newBuilder()
                            .setChunks(ByteString.copyFrom(bytes))
                            .setFilepath(chunkName)
                            .setType(type))
                            .build())
                    .build();

            ch.writeAndFlush(message);

        }
    }

    private DfsMessages.DataNodeMetadata getNextNode(List<DfsMessages.DataNodeMetadata> nodesToMaintain) {
        int i;
        int len = nodesToMaintain.size();
        for(i = 0; i < len; i++) {
            DfsMessages.DataNodeMetadata node = nodesToMaintain.get(i);
            if(node.getHostname().equals(this.hostName) && node.getPort() == this.hostPort) {
                if(i + 1 < len) return nodesToMaintain.get(i+1);
            }
        }
        return null;
    }

    private void initiateReplicationMaintenence(ChannelHandlerContext ctx, DfsMessages.FileChunkHeader header, DfsMessages.AlreadyMaintainedChunks alreadyMaintainedChunks) {
        String nodeIp = header.getNodeIp();
        List<DfsMessages.DataNodeMetadata> nodesToReplicateOn = header.getReplicasList();

        // go through filechunkmetadatamap and find all files that need to be replicated
        Map<String, List<String>> map = getFilesToBeReplicated(nodeIp, alreadyMaintainedChunks);

        // divide them between original and replicas that need to be replicated
        List<String> originalChunks = map.get(ORIGINAL_DIR);
        List<String> replicaChunks  = map.get(REPLICA_DIR);

        System.out.println("storagepath: " + storagePath);
        String originalDir = storagePath + "/" + ORIGINAL_DIR + "/";
        System.out.println("original dir: " + originalDir);
        // start replicating originals
        replicateChunks(originalChunks, nodesToReplicateOn, DfsMessages.FileChunk.Type.MAINTENANCE_LDR);

        // start replicating replicas
        replicateChunks(replicaChunks, nodesToReplicateOn, DfsMessages.FileChunk.Type.REPLICA);

        // create a map of file chunks and whether an original or replica was created for them

        List<DfsMessages.DataNodeMetadata> nodesToMaintain = header.getMaintenanceNodesList();

        // send this map to the next storage node on the list
        DfsMessages.DataNodeMetadata node = getNextNode(nodesToMaintain);

        if(node == null) return; // this is the last node to replicate on

        if(alreadyMaintainedChunks != null) {
            //  grow the list with existing replications done by previous nodes
            originalChunks.addAll(alreadyMaintainedChunks.getOriginalChunksList());
            replicaChunks.addAll(alreadyMaintainedChunks.getRepeatedChunksList());
        }

        // Send next node a FileChunkHeader msg with all info for cascading maintenance
        DfsMessages.MessagesWrapper wrapper = DfsMessages.MessagesWrapper.newBuilder()
                .setDataNodeWrapper(
                DfsMessages.DataNodeMessagesWrapper.newBuilder()
                .setFileChunkHeader(
                DfsMessages.FileChunkHeader.newBuilder()
                .addAllReplicas(nodesToReplicateOn)
                .addAllMaintenanceNodes(nodesToMaintain)
                .setNodeIp(nodeIp)
                .setAlreadyMaintainedChunks(
                DfsMessages.AlreadyMaintainedChunks.newBuilder()
                .addAllRepeatedChunks(replicaChunks)
                .addAllOriginalChunks(originalChunks)
                .build())
                .build())
                .build())
                .build();

        System.out.println(wrapper);

        String ip = node.getIp().split(":")[0];
        int port = node.getPort();
        System.out.println("NEXT NODE TO REPLICATE ON: " + ip + ":" + port);
        Channel ch = connectToNode(ip, port);

        ch.writeAndFlush(wrapper);

        // todo add checking the same map at the beginning of this function to make sure no duplicate replication occurs
    }

    private void getAndSendChunks(ChannelHandlerContext ctx, String filepath) throws IOException {
        String storageDirectory = this.storagePath + "/original/";

        File dir = new File(storageDirectory + filepath);
        List<DfsMessages.FileChunk> chunks = new ArrayList<>();
        File[] allFiles = dir.listFiles();

        if(allFiles == null) return;

        for (File eachFile: allFiles) {
            if (!eachFile.getName().contains("checksum") &&  !eachFile.getName().contains("metadata")){
                System.out.println("eachFile = " + eachFile);
                System.out.println(filepath + "-" +  eachFile.getName().split("-")[1]);
                ctx.channel().writeAndFlush(DfsMessages.MessagesWrapper.newBuilder().setClientWrapper(DfsMessages.ClientMessagesWrapper.newBuilder().setFileChunk(getChunks(eachFile, fileChunkMetadataMap.get(filepath + "-" +  eachFile.getName().split("-")[1])))));
                System.out.println("Written " + eachFile.getName());
            }
//                chunks.add(getChunks(eachFile));
        }
    }

    private void createChannels(List<DfsMessages.DataNodeMetadata> dataNodesList) {
        dataNodesList.forEach(node->{
            channelMap.put(node.getHostname(), connectToNode(node.getIp().split(":")[0], node.getPort()));
        });
    }

    public Channel connectToNode(String hostname, Integer port) {
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


    private void sendFileToReplicas(DfsMessages.FileChunk fileChunk) {
        String filePath = fileChunk.getFilepath().split("-")[0];
        System.out.println("filePath = " + filePath);
        //0-leader, 1,2- replicas

        DfsMessages.FileChunkHeader currHeader = DfsMessages.FileChunkHeader.newBuilder(fileChunk.getFilechunkHeader()).removeReplicas(2).removeReplicas(1).build();
        DfsMessages.FileChunk currFileChunk = DfsMessages.FileChunk.newBuilder(fileChunk).setType(DfsMessages.FileChunk.Type.REPLICA).setFilechunkHeader(currHeader).build();
        DfsMessages.MessagesWrapper message = DfsMessages.MessagesWrapper.newBuilder().setDataNodeWrapper(DfsMessages.DataNodeMessagesWrapper.newBuilder().setFileChunk(currFileChunk).build()).build();
//        System.out.println("message to replicas = " + message);
        fileChunk.getFilechunkHeader().getReplicasList().subList(1,3).forEach((nodeMetadata)->{
            Channel channel = channelMap.getOrDefault(nodeMetadata.getIp(), connectToNode(nodeMetadata.getIp().split(":")[0], nodeMetadata.getPort()));
            channelMap.putIfAbsent(nodeMetadata.getIp(), channel);
            channel.writeAndFlush(message);
        });
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
    }

    public class RunnableTask implements Runnable {

        public RunnableTask() {

        }

        public void run() {

        }
    }
}
