package edu.usfca.cs.chat;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

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
                    if(message.getFileChunk().getFilechunkHeader().getReplicasCount() == 1){ //replica
                        writeToFile(message.getFileChunk(),storagePath,"/replica/");
                    } else {//leader
                        writeToFile(message.getFileChunk(),storagePath,"/original/");
                        sendFileToReplicas(message.getFileChunk());
                        System.out.println("fileChunkMetadataMap = " + fileChunkMetadataMap.keySet());
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
                        FileUtils.clearDirectoryContents(storagePath + "/original/" + dirPath);
                        FileUtils.clearDirectoryContents(storagePath + "/replica/" + dirPath);
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
                List<DfsMessages.DataNodeMetadata> nodes = header.getReplicasList();
                System.out.println("Number of nodes available = " + nodes.size());
//                prepareForStorage(message.getFileChunkHeader( ));
                break;
            case 4: //replication status. not currently doing anything
                System.out.println("Replication Status of " + ctx.channel().remoteAddress().toString());
                System.out.println("Chunk num and success" + message.getReplicationStatus().getChunkNum() + " is " + message.getReplicationStatus().getSuccess());
                break;
            case 5: // ON NODE DOWN MSG
                DfsMessages.OnNodeDown nodeDown = message.getOnNodeDown();
                System.out.println("X X X X NODE DOWN DETECTED X X X X");
                System.out.println(nodeDown.getIp());
                System.out.println(" - - - - - - - - - - - - - - - - - ");
                initiateReplicationMaintenence(ctx, nodeDown.getIp());
                break;
            default:
                System.out.println("whaaaa");
                break;
        }

        //send back ok (?)
        /* Hmm... */

    }

    private DfsMessages.MessagesWrapper getControllerReqForStorage(int totalChunks) {
        return DfsMessages.MessagesWrapper.newBuilder().setControllerWrapper(
                DfsMessages.ControllerMessagesWrapper.newBuilder()
                .setGetFreeNodes(
                DfsMessages.GetFreeNodes.newBuilder()
                .setNumChunks(totalChunks)))
                .build();
    }

    private void initiateReplicationMaintenence(ChannelHandlerContext ctx, String nodeIp) {
        System.out.println("inside initiate replica maintenence");

        // map of all original and replica chunks that current node updates
        Map<String, DfsMessages.DataNodeMetadata> replicasUpdated = new HashMap<>();
        Map<String, DfsMessages.DataNodeMetadata> originalsUpdated = new HashMap<>();

        List<String> replicasList = new ArrayList<>();
        List<String> originalsList = new ArrayList<>();

        // check if there are any replicas on down node
        System.out.println(fileChunkMetadataMap);
        int totalChunksSpace = 0;

        // loop through  metadatamap and figure out total replicas and originals to replace
        for(String key : fileChunkMetadataMap.keySet()) {
            // key is filname-<chunk_number>
            DfsMessages.FileChunkHeader header = fileChunkMetadataMap.get(key);
            List<DfsMessages.DataNodeMetadata> metaDataList = header.getReplicasList();
            if(metaDataList.size() == 1) replicasList.add(key);
            else originalsList.add(key);
            totalChunksSpace++;
        }

        // make storage request to controller
        DfsMessages.MessagesWrapper wrapper = getControllerReqForStorage(totalChunksSpace);

        System.out.println("Sending Request for nodes to start replication on: ");
        System.out.println(wrapper);

        ctx.channel().writeAndFlush(wrapper);
        // if no

        // if yes, create a replica on new node

        // check if this node has replicas for any leaders that went down

        // if yes, create leader on some other node


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
        DfsMessages.FileChunk currFileChunk = DfsMessages.FileChunk.newBuilder(fileChunk).setFilechunkHeader(currHeader).build();
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
