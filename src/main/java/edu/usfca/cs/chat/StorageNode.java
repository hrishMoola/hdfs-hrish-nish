package edu.usfca.cs.chat;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


import edu.usfca.cs.chat.net.MessagePipeline;
import edu.usfca.cs.chat.net.ServerMessageRouter;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;


import static edu.usfca.cs.chat.Utils.FileUtils.writeToFile;

@ChannelHandler.Sharable
public class StorageNode
    extends SimpleChannelInboundHandler<DfsMessages.DataNodeMessagesWrapper> {

    ServerMessageRouter messageRouter;
    private String storagePath;
    private String controllerHostname;    // controller to connect to and send heartbeats to
    private Integer controllerPort;
    private String hostName;        // host and part where storage node will be listening as a server
    private Integer hostPort;

    private Channel controllerChannel;
    private String localAddr;

    Map<String, List<Channel>> filePathToReplicaChannels;

    public StorageNode(String[] args) {
        this.storagePath = args[0];
        this.hostName = args[1];
        this.hostPort = Integer.parseInt(args[2]);
        this.controllerHostname = args[3]; // sto
        this.controllerPort = Integer.parseInt(args[4]);
        filePathToReplicaChannels = new HashMap<>();
    }

    public void start()
    throws IOException {
        messageRouter = new ServerMessageRouter(this,  DfsMessages.DataNodeMessagesWrapper.getDefaultInstance());
        messageRouter.listen(this.hostPort);
        System.out.println("Data node " + this.hostName + " on port " + this.hostPort + "...");
        // on start connect to controller and send alive notification
        this.connect();
        this.sendIntroMessage();
    }

    public static void main(String[] args)
    throws IOException {
        if (args.length >= 4) {
            StorageNode s = new StorageNode(args);
            s.start();

            // todo connect to controller as a client
//            c = new Client(args[0], Integer.parseInt(args[1]), args[2]);
        }
    }

    private void sendIntroMessage() {
        DfsMessages.ControllerMessagesWrapper wrapper = DfsMessages.ControllerMessagesWrapper.newBuilder()
                .setIntroMessage(DfsMessages.DataNodeMetadata.newBuilder()
                .setHostname(hostName)
                .setIp(localAddr)
                .setPort(hostPort)
                .setMemory("10GB")
                .build()).build();

        ChannelFuture write = controllerChannel.writeAndFlush(wrapper);
        write.syncUninterruptibly();
    }

    //connect to controller node upon startup
    public void connect() {
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        MessagePipeline pipeline = new MessagePipeline(this, DfsMessages.DataNodeMessagesWrapper.getDefaultInstance());

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
    }

    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx)
    throws Exception {
        /* Writable status of the channel changed */
    }

    @Override
    public void channelRead0(
            ChannelHandlerContext ctx, DfsMessages.DataNodeMessagesWrapper message) {
        int messageType = message.getMsgCase().getNumber();

        switch(messageType){
            case 1:
                //basically store the chunks being provided and send for replication to replicas
                try {
                    writeToFile(message.getFileChunk(),storagePath);
                    sendFileToReplicas(message.getFileChunk());
                    System.out.println("Successfully wrote to the file.");
                } catch (Exception e) {
                    System.out.println("An error occurred.");
                    e.printStackTrace();
                }
                break;
            case 3:// chunk header from client.
                System.out.println("Received chunk header and replica information");
                prepareForStorage(message.getFileChunkHeader());
            case 4:
                //replication status. not currently doing anything
                System.out.println("Replication Status of " + ctx.channel().remoteAddress().toString());
                System.out.println("Chunk num and success" + message.getReplicationStatus().getChunkNum() + " is " + message.getReplicationStatus().getSuccess());
                break;
            default:
                System.out.println("whaaaa");
                break;
        }
        //later on replicate

        //send back ok (?)

        /* Hmm... */

    }

    //create directories and tell replicas to create also
    private void prepareForStorage(DfsMessages.FileChunkHeader fileChunkHeader) {
        try {
            Path path = Paths.get(storagePath + "/" + fileChunkHeader.getFilepath());
            Files.createDirectories(path);
            path = Paths.get(storagePath + "/"  + fileChunkHeader.getFilepath() + "/metaData");
            Files.write(path, fileChunkHeader.toString().getBytes());
        } catch (IOException e) {
            e.printStackTrace();
        }
        //if list is not empty
        //create channels for the two and send them filechunk header without the list
        if(fileChunkHeader.getReplicasCount()> 0){
            filePathToReplicaChannels.put(fileChunkHeader.getFilepath(), new ArrayList<>());
            filePathToReplicaChannels.get(fileChunkHeader.getFilepath()).add(getChannel(fileChunkHeader.getReplicas(0).getHostname(), Integer.parseInt(fileChunkHeader.getReplicas(0).getIp())));
            filePathToReplicaChannels.get(fileChunkHeader.getFilepath()).add(getChannel(fileChunkHeader.getReplicas(1).getHostname(), Integer.parseInt(fileChunkHeader.getReplicas(1).getIp())));
            }
        System.out.println("filePathToReplicaChannels = " + filePathToReplicaChannels);
    }

    public Channel getChannel(String leadername, Integer port) {
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        MessagePipeline pipeline = new MessagePipeline(this, DfsMessages.DataNodeMessagesWrapper.getDefaultInstance());

        Bootstrap bootstrap = new Bootstrap()
                .group(workerGroup)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .handler(pipeline);

        System.out.println("Connecting to " + leadername + ":" + port);
        ChannelFuture cf = bootstrap.connect(leadername, port);
        cf.syncUninterruptibly();
        return cf.channel();
    }

    private void sendFileToReplicas(DfsMessages.FileChunk fileChunk) {
        String filePath = fileChunk.getFilepath().split("-")[0];
        System.out.println("filePath = " + filePath);
        filePathToReplicaChannels.get(filePath).forEach((channel)->{
            channel.writeAndFlush(DfsMessages.DataNodeMessagesWrapper.newBuilder().setFileChunk(fileChunk).build());
        });
        filePathToReplicaChannels.get(filePath).forEach(ChannelOutboundInvoker::close);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
    }
}
