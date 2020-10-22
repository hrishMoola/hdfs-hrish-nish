package edu.usfca.cs.chat;

import java.io.*;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import com.google.protobuf.ByteString;
import edu.usfca.cs.chat.Utils.FileUtils;
import edu.usfca.cs.chat.net.MessagePipeline;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

import javax.xml.bind.SchemaOutputResolver;

import static edu.usfca.cs.chat.DfsMessages.FileAck.Type.FILE_OVERWRITE;
import static edu.usfca.cs.chat.Utils.FileChunker.mergeFiles;
import static edu.usfca.cs.chat.Utils.FileUtils.*;

@ChannelHandler.Sharable
public class Client
    extends SimpleChannelInboundHandler<DfsMessages.MessagesWrapper> {

    private String username;    //client name
    private String controllerHostName;
    private int controllerPort;

    private Channel serverChannel;

    private Integer CHUNK_SIZE;
    private static String storagePath;
    private static volatile AtomicInteger chunksReceived;
    private static ConcurrentHashMap<String, Integer> totalChunks;
    private static ConcurrentHashMap<String, Channel> channelMap;
    private static ExecutorService executor ;


    public Client(String controllerHostName, int controllerPort, String username, Integer chunkSize, String localStorage) {
        this.controllerHostName = controllerHostName;
        this.controllerPort = controllerPort;
        this.username = username;
        this.CHUNK_SIZE = chunkSize * 1024;
        chunksReceived = new AtomicInteger(0);
        channelMap = new ConcurrentHashMap<>();
        totalChunks = new ConcurrentHashMap<>();
        executor = Executors.newFixedThreadPool(8);
        storagePath = localStorage;
        createLocalFolders(localStorage);
    }

    public static void main(String[] args) throws IOException {
        Client c = null;
        if (args.length >= 3) {
            c = new Client(args[0], Integer.parseInt(args[1]), args[2],Integer.parseInt(args[3]), args[4]);
            c.connect();
        }

        if (c == null) {
            System.out.println("Usage: Client <hostname> <port> <username>");
            System.exit(1);
        }

        InputReader reader = new InputReader(c);
        Thread inputThread = new Thread(reader);
        inputThread.run();
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

        System.out.println("Connecting to " + controllerHostName + ":" + controllerPort);
        ChannelFuture cf = bootstrap.connect(controllerHostName, controllerPort);
        cf.syncUninterruptibly();
        serverChannel = cf.channel();
    }

    private void chunkFileAndSendToNodes(DfsMessages.FileResponse fileResponse) throws IOException {
        File f = new File(fileResponse.getSystemFilePath());
        LRUCache lruCache = new LRUCache(CHUNK_SIZE);
        Map<String, DfsMessages.DataNodeMetadata> metadataMap = fileResponse.getDataNodesList().stream().collect(Collectors.toMap(DfsMessages.DataNodeMetadata::getIp, node-> node));
        createChannels(fileResponse.getDataNodesList());
        lruCache.addAll(fileResponse.getDataNodesList());

        int partCounter = 0;
        byte[] buffer = new byte[CHUNK_SIZE];

        System.out.println("lruCache = " + lruCache.nodeAge);
        String fileName = fileResponse.getDfsFilePath();
        int numChunks = new Double(Math.ceil(f.length() / Double.valueOf(CHUNK_SIZE))).intValue();
        System.out.println("filename in chunkfile is: " + fileName);

        //try-with-resources to ensure closing stream
        try (FileInputStream fis = new FileInputStream(f);
             BufferedInputStream bis = new BufferedInputStream(fis)) {

            int bytesAmount = 0;
            while ((bytesAmount = bis.read(buffer)) > 0) {
                // write each chunk of data into separate file with different number in name
                String filePartName = String.format("%s-%03d", fileName, partCounter++);
                    List<DfsMessages.DataNodeMetadata> replicas = lruCache.getWithReplicas().stream().map(metadataMap::get).collect(Collectors.toList());
                    DfsMessages.FileChunk.Type type = DfsMessages.FileChunk.Type.LEADER;
                    if(replicas.size() == 1) type = DfsMessages.FileChunk.Type.REPLICA;
                    if(partCounter == numChunks) {
                        byte[] newBuffer = Arrays.copyOfRange(buffer, 0, bytesAmount);
                        buffer = newBuffer;
                    }
                    sendChunks(filePartName, buffer, replicas, Integer.toString(numChunks), type);
                    buffer = new byte[CHUNK_SIZE];
                    System.out.println("Sent");
            }
        }
        catch(Error e) {
            System.out.println("Error while splitting file in chunkFile " + e);
        }
        System.out.println("Total chunks created: " + partCounter);
        System.out.println("Total chunks calculated: " + numChunks);
        System.out.println("lruCache remaining memory= " + lruCache.getRemainingMemory());
    }

    private void createChannels(List<DfsMessages.DataNodeMetadata> dataNodesList) {
        dataNodesList.forEach(node->{
                channelMap.putIfAbsent(node.getIp(), connectToNode(node.getIp().split(":")[0], node.getPort()));
        });

    }

    private DfsMessages.FileChunkHeader getChunkHeader(String numChunks, List<DfsMessages.DataNodeMetadata> replicas, String filePartName) {
        return DfsMessages.FileChunkHeader.newBuilder().setFilepath(filePartName.split("-")[0])
                        .setTotalChunks(Integer.parseInt(numChunks)).addAllReplicas(replicas).build();
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


    private void sendChunks(String filePartName, byte[] buffer, List<DfsMessages.DataNodeMetadata> replicas, String numChunks, DfsMessages.FileChunk.Type type) {

        DfsMessages.FileChunk fileChunkMessage = DfsMessages.FileChunk.newBuilder().setFilepath(filePartName).setChunks(ByteString.copyFrom(buffer))
                .setFilechunkHeader(getChunkHeader(numChunks, replicas, filePartName)).setType(type).build();
        DfsMessages.MessagesWrapper msgWrapper = DfsMessages.MessagesWrapper.newBuilder().setDataNodeWrapper(DfsMessages.DataNodeMessagesWrapper.newBuilder().setFileChunk(fileChunkMessage)).build();
        ChannelFuture write = channelMap.get(replicas.get(0).getIp()).writeAndFlush(msgWrapper);
        write.syncUninterruptibly();
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
        InetSocketAddress addr = (InetSocketAddress) ctx.channel().remoteAddress();
        System.out.println("Connection lost: " + addr);
    }

    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx)
    throws Exception {
        /* Writable status of the channel changed */
    }

    @Override
    public void channelRead0(
            ChannelHandlerContext ctx, DfsMessages.MessagesWrapper msg) {

        DfsMessages.ClientMessagesWrapper message = msg.getClientWrapper();
        int messageType = message.getMsgCase().getNumber();
        try{
            switch(messageType){
                case 1: // FileChunk
                    chunksReceived.incrementAndGet();
                    executor.submit(()->{
                        try {
                            storeFile(storagePath + "/cache/" + message.getFileChunk().getFilepath(), message.getFileChunk().getChunks().toByteArray());
                            System.out.println("chunksReceived = " + chunksReceived);
                            System.out.println("totalChunks.get() = " + totalChunks.get(message.getFileChunk().getFilechunkHeader().getFilepath()));
                            totalChunks.putIfAbsent(message.getFileChunk().getFilechunkHeader().getFilepath(), message.getFileChunk().getFilechunkHeader().getTotalChunks());
                            if(chunksReceived.get() == totalChunks.get(message.getFileChunk().getFilechunkHeader().getFilepath())){
                                System.out.println("Mergin file now");
                                mergeFiles(storagePath + "/" + "cache", storagePath + "/output/" + getFileName(message.getFileChunk().getFilechunkHeader().getFilepath()));
                                clearDirectoryContents(storagePath + "/cache");
                                chunksReceived = new AtomicInteger(0);
                            }
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    });
                    break;
                case 2: // FileResponse
                    System.out.println(message);
                    System.out.println("Received a file response for " + message.getFileResponse().getSystemFilePath());
                    System.out.println("Request type is  " + message.getFileResponse().getType().name());
                    executor.submit(()->{
                        if(message.getFileResponse().getType().equals(DfsMessages.FileResponse.Type.RETRIEVE)){
                            getChunksFromDataNodes(message.getFileResponse());
                        }
                        else{
                            startFileStorage(message.getFileResponse());
                        }
                    });
                    break;
                case 5:
                    System.out.println("Received replica patch");
                    System.out.println("message.getReplicaPatch() = " + message.getReplicaPatch());
                    System.out.println("message.getReplicaPatch().getNodeMetadata() = " + message.getReplicaPatch().getNodeMetadata());
                    DfsMessages.DataNodeMetadata nodeMetadata = message.getReplicaPatch().getNodeMetadata();
                    try {
                        Channel channel = channelMap.getOrDefault(nodeMetadata.getIp(), connectToNode(nodeMetadata.getIp().split(":")[0], nodeMetadata.getPort()));
                        channelMap.putIfAbsent(nodeMetadata.getIp(), channel);
                        channel.writeAndFlush(DfsMessages.MessagesWrapper.newBuilder().setDataNodeWrapper(DfsMessages.DataNodeMessagesWrapper.newBuilder().setReplicaPatch(message.getReplicaPatch()).build()).build());
                    } catch (Exception e) {
                        System.out.println("An error occurred.");
                        e.printStackTrace();
                    }
                    break;
                    case 6:
                        System.out.println("Received File System Response!");
                        System.out.println(message.getFsResponse().getInfo());
                        break;
                case 7:
                    System.out.println("Received status info");
                    System.out.println(message.getNodeStatus().getNodeMetadataList());
                    break;
                default:
                    System.out.println("Default channelRead switch case statement");
                    break;
            }
        } catch (Exception e){
            e.printStackTrace();
        }
    }

    private DfsMessages.FileAck createFileAckMsg(DfsMessages.FileResponse message) {
        return DfsMessages.FileAck.newBuilder().setFilepath(message.getDfsFilePath()).setSuccess(true).setType(FILE_OVERWRITE).build();
    }

    private void startFileStorage(DfsMessages.FileResponse message) {
        createChannels(message.getDataNodesList());

        if(message.getShouldOverwrite()) {
            DfsMessages.FileAck ackMsg = createFileAckMsg(message);
            DfsMessages.DataNodeMessagesWrapper wrapper = DfsMessages.DataNodeMessagesWrapper.newBuilder().setFileAck(ackMsg).build();
            DfsMessages.MessagesWrapper msgWrapper = DfsMessages.MessagesWrapper.newBuilder().setDataNodeWrapper(wrapper).build();
            try {
                message.getDataNodesList().forEach(nodeMetadata -> channelMap.get(nodeMetadata.getIp()).writeAndFlush(msgWrapper));
            }
            catch(Exception e) {
                System.out.println("error sending overwrite ack to storage nodes: " + e);
            }
        }
        try {
            chunkFileAndSendToNodes(message);
        }
        catch(Exception e) {
            System.out.println("error while starting file storage in client: " + e);
        }


    }

    private void getChunksFromDataNodes(DfsMessages.FileResponse fileResponse) {
        createChannels(fileResponse.getDataNodesList());
        fileResponse.getDataNodesList().forEach(nodeMetadata->{
            DfsMessages.MessagesWrapper wrapper = DfsMessages.MessagesWrapper.newBuilder().setDataNodeWrapper(DfsMessages.DataNodeMessagesWrapper.newBuilder()
                    .setFileAck(DfsMessages.FileAck.newBuilder()
                            .setFilepath(fileResponse.getDfsFilePath())
                            .setType(DfsMessages.FileAck.Type.FILE_RETRIEVAL))).build();
            channelMap.get(nodeMetadata.getIp()).writeAndFlush(wrapper);
            System.out.println("Request sent for file chunks");
        });
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
    }


    private static class InputReader implements Runnable {
        private Client client;

        public InputReader (Client client) {
            this.client = client;
        }

        public void run() {
            BufferedReader reader = 
                new BufferedReader(new InputStreamReader(System.in)); 

            while (true) {
                String line = "";
                try {
                    System.out.println("What do you want to do: ");
                    System.out.println("1. store <localFilePath> dfs/<dfsFilePath>");
                    System.out.println("2. retrieve dfs/<dfsFilePath>");
                    System.out.println("3. ls dfs(/<dfsFolder>)");
                    System.out.println("4. status");
                    line = reader.readLine();
                    if (line.startsWith("store"))
                        client.sendFileStoreToController(line);
                    else if (line.startsWith("retrieve"))
                        client.sendFileRetrieveToController(line);
                    else if (line.startsWith("ls"))
                        client.sendFsRequestToController(line);
                    else if (line.startsWith("status"))
                        client.getNodeStatusFromController();
                } catch (IOException e) {
                    e.printStackTrace();
                    break;
                }
            }
        }
    }

    private void getNodeStatusFromController() {
        DfsMessages.MessagesWrapper wrapper = DfsMessages.MessagesWrapper.newBuilder().setControllerWrapper(DfsMessages.ControllerMessagesWrapper.newBuilder().setFileRequest(
                DfsMessages.FileRequest.newBuilder().setFilepath("").setDirectory("").setType(DfsMessages.FileRequest.Type.STATUS))).build();
        serverChannel.writeAndFlush(wrapper);
    }

    private void sendFsRequestToController(String line) {

        DfsMessages.MessagesWrapper wrapper = DfsMessages.MessagesWrapper.newBuilder().setControllerWrapper(DfsMessages.ControllerMessagesWrapper.newBuilder().setFsRequest(DfsMessages.FileSystemRequest.newBuilder()
        .setFilepath(line.split(" ")[1]).setOpValue(0))).build();
        serverChannel.writeAndFlush(wrapper);
    }

    private void sendFileRetrieveToController(String line) {
        DfsMessages.MessagesWrapper wrapper = DfsMessages.MessagesWrapper.newBuilder().setControllerWrapper(DfsMessages.ControllerMessagesWrapper.newBuilder()
                .setFileRequest(DfsMessages.FileRequest.newBuilder()
                        .setFilepath(line.split("\\s")[1]).setType(DfsMessages.FileRequest.Type.RETRIEVE))).build();
        serverChannel.writeAndFlush(wrapper);
    }

    private void sendFileStoreToController(String line) {
        String[] args = line.split("\\s");

        // input error checking
        if(args.length < 3 || !FileUtils.doesFileExist(args[1])) {
            System.out.println("Sorry, invalid arguments or this file does not exist locally, try again :(");
            return;
        }

        String localFile = args[1];
        String dfsPath = args[2];
        System.out.println("LOCAL FILE: " + localFile);
        System.out.println("DFS PATH: " + dfsPath);

        DfsMessages.MessagesWrapper wrapper = DfsMessages.MessagesWrapper.newBuilder().setControllerWrapper(
                DfsMessages.ControllerMessagesWrapper.newBuilder().setFileRequest(
                        getFileRequest(localFile, dfsPath, Double.valueOf(this.CHUNK_SIZE)))).build();
        serverChannel.writeAndFlush(wrapper);
    }
}
