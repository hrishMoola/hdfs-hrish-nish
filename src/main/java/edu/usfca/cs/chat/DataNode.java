package edu.usfca.cs.chat;

import java.io.IOException;
import java.net.InetSocketAddress;

import edu.usfca.cs.chat.net.ServerMessageRouter;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import static edu.usfca.cs.chat.Utils.FileUtils.writeToFile;

@ChannelHandler.Sharable
public class DataNode
    extends SimpleChannelInboundHandler<DfsMessages.DfsMessagesWrapper> {

    ServerMessageRouter messageRouter;
    private String storagePath;
    private String nameNodeHost;
    private Integer nameNodePort;
    private String hostName;
    private Integer hostPort;

    public DataNode(String[] args) {
        this.storagePath = args[0];
        this.hostName = args[1];
        this.hostPort = Integer.parseInt(args[2]);
        this.nameNodeHost = args[3];
        this.nameNodePort = Integer.parseInt(args[4]);
    }

    public void start()
    throws IOException {
        messageRouter = new ServerMessageRouter(this);
        messageRouter.listen(this.hostPort);
        System.out.println("Data node" + this.hostName + " on port " + this.hostPort + "...");
    }

    public static void main(String[] args)
    throws IOException {
        if (args.length >= 4) {
            DataNode s = new DataNode(args);
            s.start();

//            c = new Client(args[0], Integer.parseInt(args[1]), args[2]);
//            c.connect();
        }

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
            ChannelHandlerContext ctx, DfsMessages.DfsMessagesWrapper message) {
        //read from ctx
        int messageType = message.getMsgCase().getNumber();

        switch(messageType){
            case 1:
                try {
                    writeToFile(message.getFileChunk());
                    System.out.println("Successfully wrote to the file.");
                } catch (Exception e) {
                    System.out.println("An error occurred.");
                    e.printStackTrace();
                }
                break;
                default:
                    System.out.println("whaaaa");
                    break;
        }
        //later on replicate

        //send back ok (?)

        /* Hmm... */

    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
    }
}
