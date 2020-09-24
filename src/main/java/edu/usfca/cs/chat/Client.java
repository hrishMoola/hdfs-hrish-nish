package edu.usfca.cs.chat;

import java.io.*;
import java.net.InetSocketAddress;

import com.google.protobuf.ByteString;
import edu.usfca.cs.chat.net.MessagePipeline;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

import static edu.usfca.cs.chat.Utils.FileUtils.getFileRequest;

@ChannelHandler.Sharable
public class Client
//    extends SimpleChannelInboundHandler<ChatMessages.ChatMessagesWrapper> {
    extends SimpleChannelInboundHandler<DfsMessages.ClientMessagesWrapper> {

    private String username;    //client name
    private String controllerHostName;
    private int controllerPort;

    private Channel serverChannel;
    private Channel leaderChannel;

    public Client(String controllerHostName, int controllerPort, String username) {
        this.controllerHostName = controllerHostName;
        this.controllerPort = controllerPort;
        this.username = username;
    }



    public static void main(String[] args) throws IOException {
        Client c = null;
        if (args.length >= 3) {
            c = new Client(args[0], Integer.parseInt(args[1]), args[2]);
            c.connect();
        }

        if (c == null) {
            System.out.println("Usage: Client <hostname> <port> <username>");
            System.exit(1);
        }

        c.sendGreeting();

        InputReader reader = new InputReader(c);
        Thread inputThread = new Thread(reader);
        inputThread.run();
    }

    //connect to controller node upon startup
    public void connect() {
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        MessagePipeline pipeline = new MessagePipeline(this, DfsMessages.ClientMessagesWrapper.getDefaultInstance());

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

    private void chunkFileAndSendToLeader(DfsMessages.FileResponse fileResponse) throws IOException {
        File f = new File(fileResponse.getFilepath());
        int partCounter = 0;
        int sizeOfFile = 50 * 1024; // 50kb chunks
//        int sizeOfFile = 128 * 1024 * 1024; // 128/chunk
        byte[] buffer = new byte[sizeOfFile];

        String fileName = f.getName();
        sendChunkHeaderToLeader((int)Math.ceil(f.length() / (float)sizeOfFile), fileResponse);

        //try-with-resources to ensure closing stream
        try (FileInputStream fis = new FileInputStream(f);
             BufferedInputStream bis = new BufferedInputStream(fis)) {

            int bytesAmount = 0;
            while ((bytesAmount = bis.read(buffer)) > 0) {
                //write each chunk of data into separate file with different number in name
                String filePartName = String.format("%s-%03d", fileName, partCounter++);
                    sendChunks(filePartName, buffer);
                    System.out.println("Sent");
            }
        }
        catch(Error e) {
            System.out.println("Error while splitting file in chunkFile " + e);
        }
        System.out.println("Total chunks: " + partCounter);
        leaderChannel.flush();
        leaderChannel.close();
    }

    private void sendChunkHeaderToLeader(int numChunks, DfsMessages.FileResponse fileResponse) {
        DfsMessages.DataNodeMessagesWrapper wrapper = DfsMessages.DataNodeMessagesWrapper.newBuilder()
                        .setFileChunkHeader(DfsMessages.FileChunkHeader.newBuilder()
                        .setFilepath(fileResponse.getFilepath()).setTotalChunks(numChunks)
                        .addReplicas(0,fileResponse.getDataNodes(1))
                        .addReplicas(1,fileResponse.getDataNodes(2))
                                .build()).build();
        connectToLeaderNode(fileResponse.getDataNodes(0).getHostname() , Integer.parseInt(fileResponse.getDataNodes(0).getIp()));
        ChannelFuture write = leaderChannel.writeAndFlush(wrapper);
        System.out.println("Sent chunk header to leader");
        write.syncUninterruptibly();
    }

    public void connectToLeaderNode(String leadername, Integer port) {
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        MessagePipeline pipeline = new MessagePipeline(this, DfsMessages.ClientMessagesWrapper.getDefaultInstance());

        Bootstrap bootstrap = new Bootstrap()
                .group(workerGroup)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .handler(pipeline);

        System.out.println("Connecting to " + leadername + ":" + port);
        ChannelFuture cf = bootstrap.connect(leadername, port);
        cf.syncUninterruptibly();
        leaderChannel = cf.channel();
    }
    private void sendChunks(String filePartName, byte[] buffer) {
        DfsMessages.FileChunk fileChunkMessage = DfsMessages.FileChunk.newBuilder().setFilepath(filePartName).setChunks(ByteString.copyFrom(buffer)).build();
        DfsMessages.DataNodeMessagesWrapper msgWrapper = DfsMessages.DataNodeMessagesWrapper.newBuilder().setFileChunk(fileChunkMessage).build();
        ChannelFuture write = leaderChannel.writeAndFlush(msgWrapper);
        write.syncUninterruptibly();
    }


    public void sendMessage(String message) {

        ChatMessages.ChatMessage msg
            = ChatMessages.ChatMessage.newBuilder()
            .setUsername(username)
            .setMessageBody(message)
        .build();

        ChatMessages.ChatMessagesWrapper msgWrapper =
            ChatMessages.ChatMessagesWrapper.newBuilder()
                .setChatMessage(msg)
                .build();

        /* Note: you could also do:
         * serverChannel.write(msgWrapper);
         * serverChannel.flush();
         * In this case there is no difference, but if you needed to do several
         * writes it would be more efficient to only do a single flush() after
         * the writes. */
        ChannelFuture write = serverChannel.writeAndFlush(msgWrapper);

        write.syncUninterruptibly();
//        serverChannel.close();
    }

    public void sendGreeting() {
        String message = "Hello all! I am " + username + ", pleased to meet you.";
//        sendMessage(message);
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
            ChannelHandlerContext ctx, DfsMessages.ClientMessagesWrapper message) {

        System.out.println(message);
        int messageType = message.getMsgCase().getNumber();
        try{
            switch(messageType){
                case 2:
                    System.out.println("Received a file response for " + message.getFileResponse().getFilepath());
                    chunkFileAndSendToLeader(message.getFileResponse());
                case 6:
                    try {
                        System.out.println("received file ack");

                    } catch (Exception e) {
                        System.out.println("An error occurred.");
                        e.printStackTrace();
                    }
                    break;
                default:
                    System.out.println("whaaaa");
                    break;
            }
        } catch (Exception e){
            e.printStackTrace();
        }
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
                    line = reader.readLine();
                    if (line.startsWith("store"))
                        client.sendFileRequestToController(line);
                } catch (IOException e) {
                    e.printStackTrace();
                    break;
                }
            }
        }
    }

    private void sendFileRequestToController(String line) {
        String localFile = line.split("\\s")[1];
        String dfsPath = line.split("\\s")[2];
        DfsMessages.ControllerMessagesWrapper wrapper = DfsMessages.ControllerMessagesWrapper.newBuilder().setFileRequest(getFileRequest(localFile,dfsPath)).build();
        serverChannel.write(wrapper);
        serverChannel.flush();
    }
}
