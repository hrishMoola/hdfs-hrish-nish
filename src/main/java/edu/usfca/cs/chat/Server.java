package edu.usfca.cs.chat;

import java.io.IOException;
import java.net.InetSocketAddress;

import edu.usfca.cs.chat.net.ServerMessageRouter;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

@ChannelHandler.Sharable
public class Server
    extends SimpleChannelInboundHandler<ChatMessages.ChatMessagesWrapper> {

    ServerMessageRouter messageRouter;

    public Server() {

    }

    public void start(int port)
    throws IOException {
        messageRouter = new ServerMessageRouter(this);
        messageRouter.listen(port);
        System.out.println("Listening on port " + port + "...");
    }

    public static void main(String[] args)
    throws IOException {
        Server s = new Server();
        s.start(Integer.parseInt(args[0]));
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
            ChannelHandlerContext ctx, ChatMessages.ChatMessagesWrapper msg) {

        /* Hmm... */

    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
    }
}
