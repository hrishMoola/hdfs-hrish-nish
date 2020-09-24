
package edu.usfca.cs.chat;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import edu.usfca.cs.chat.net.ServerMessageRouter;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

@ChannelHandler.Sharable
public class Controller
        extends SimpleChannelInboundHandler<DfsMessages.ControllerMessagesWrapper> {


    // storage node map with key as hostname and DataNodeMetadata
    private ConcurrentMap<String, DfsMessages.DataNodeMetadata> activeStorageNodes;
    ServerMessageRouter messageRouter;

    public Controller() {
        activeStorageNodes = new ConcurrentHashMap<>();
    }

    public void start(int port) throws IOException {
        messageRouter = new ServerMessageRouter(this, DfsMessages.ControllerMessagesWrapper.getDefaultInstance());
        messageRouter.listen(port);
        System.out.println("Controller started on port " + port + "...");
    }

    public static void main(String[] args)
            throws IOException {
        Controller s = new Controller();
        s.start(Integer.parseInt(args[0]));
    }

    //todo register an active datanode and client connection over here.
    //todo figure out how to distinguish between the two of them to store accordingly. Mostly probably name them correctly
    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        /* A connection has been established */
        InetSocketAddress addr
                = (InetSocketAddress) ctx.channel().remoteAddress();
        System.out.println("Connection established: " + addr);
    }

    //todo remove reference to node upon disconnection
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
            ChannelHandlerContext ctx, DfsMessages.ControllerMessagesWrapper message) {

        int messageType = message.getMsgCase().getNumber();

        switch(messageType){
            case 1: // File Request
                try {
                    System.out.println("Received a file request for " + message.getFileRequest().getFilepath());
                    System.out.println("Request type is  " + message.getFileRequest().getType().name());
                    //todo blast file overwrite ack to all
                    //get list of nodes client can write to and reply to client with FileResponse
                    replyWithNodeInfo(ctx, message.getFileRequest().getFilepath());
                } catch (Exception e) {
                    System.out.println("An error in Controller while reading FileRequest " + e);
                    e.printStackTrace();
                }
                break;
            case 5: // Heart Beat
                try {
                    DfsMessages.DataNodeMetadata info = message.getHeartBeat().getNodeMetaData();
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
                    activeStorageNodes.put(IntroMsg.getHostname(), IntroMsg);
                } catch (Exception e) {
                    System.out.println("An error in Controller while reading DataNodeMetaData");
                }
                break;
            default:
                System.out.println("whaaaa");
                break;
        }

    }

    private void printMsg(DfsMessages.ControllerMessagesWrapper message) {
        System.out.println("**** New Storage Node connected ****");
        System.out.println("Hostname: " + message.getIntroMessage().getHostname());
        System.out.println("IP: " + message.getIntroMessage().getIp());
        System.out.println("Available Memory: " + message.getIntroMessage().getMemory());
        System.out.println("X - X - X - X - X - X - X - X - X - X");
    }

    //just populating with three known nodes right now. ideally here the bloomfilter stuff should come into play to create the response
    private void replyWithNodeInfo(ChannelHandlerContext ctx, String filepath) {
        DfsMessages.FileResponse fileResponse = DfsMessages.FileResponse.newBuilder()
                .addDataNodes(0, DfsMessages.DataNodeMetadata.newBuilder().setHostname("localhost").setIp("8000").build())
                .addDataNodes(1, DfsMessages.DataNodeMetadata.newBuilder().setHostname("localhost").setIp("8001").build())
                .addDataNodes(2, DfsMessages.DataNodeMetadata.newBuilder().setHostname("localhost").setIp("8002").build())
                .setFilepath(filepath)
                .build();
        DfsMessages.ClientMessagesWrapper wrapper = DfsMessages.ClientMessagesWrapper.newBuilder().setFileResponse(fileResponse).build();
        System.out.println("wrapper = " + wrapper);
        ctx.channel().writeAndFlush(wrapper);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
    }
}