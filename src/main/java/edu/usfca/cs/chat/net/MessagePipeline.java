package edu.usfca.cs.chat.net;

import com.google.protobuf.GeneratedMessageV3;
import edu.usfca.cs.chat.DfsMessages;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;

public class MessagePipeline extends ChannelInitializer<SocketChannel> {

    private ChannelInboundHandlerAdapter inboundHandler;

    public MessagePipeline(ChannelInboundHandlerAdapter inboundHandler) {
        this.inboundHandler = inboundHandler;
    }

    @Override
    public void initChannel(SocketChannel ch) throws Exception {
        ChannelPipeline pipeline = ch.pipeline();

        /* Here, we limit message sizes to 8192: */
        pipeline.addLast(new LengthFieldBasedFrameDecoder(150000000, 0, 4, 0, 4));
        pipeline.addLast(
                new ProtobufDecoder(
                    DfsMessages.MessagesWrapper.getDefaultInstance()));

        pipeline.addLast(new LengthFieldPrepender(4));
        pipeline.addLast(new ProtobufEncoder());
        pipeline.addLast(inboundHandler);
    }
}
