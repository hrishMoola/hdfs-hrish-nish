package edu.usfca.cs.chat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;

import edu.usfca.cs.chat.ChatMessages.ChatMessage;
import edu.usfca.cs.chat.net.MessagePipeline;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

public class Client
    extends SimpleChannelInboundHandler<ChatMessages.ChatMessagesWrapper> {

    private String username;
    private String hostname;
    private int port;

    private Channel serverChannel;

    public Client(String hostname, int port, String username) {
        this.hostname = hostname;
        this.port = port;
        this.username = username;
    }

    public static void main(String[] args)
    throws IOException {
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

    public void connect() {
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
        serverChannel = cf.channel();
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
    }

    public void sendGreeting() {
        String message = "Hello all! I am " + username + ", pleased to meet you.";
        sendMessage(message);
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

        if (msg.hasChatMessage()) {
            ChatMessage message = msg.getChatMessage();
            String user = message.getUsername();
            String messageBody = message.getMessageBody();
            System.out.println("[" + user + "] " + messageBody);
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
                    line = reader.readLine();
                } catch (IOException e) {
                    e.printStackTrace();
                    break;
                }

                client.sendMessage(line);
            }
        }
    }
}
