
package edu.usfca.cs.chat;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import edu.usfca.cs.chat.Utils.FileUtils;
import edu.usfca.cs.chat.net.ServerMessageRouter;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

@ChannelHandler.Sharable
public class Controller
        extends SimpleChannelInboundHandler<DfsMessages.MessagesWrapper> {


    // storage node map with key as hostname and DataNodeMetadata
    //todo update this with every heartbeat
    private ConcurrentMap<String, DfsMessages.DataNodeMetadata> activeStorageNodes;

    // routing table key is dir name
    // routing table value is map of Bloom Filter and associated DataNodeMetaData
    private ConcurrentMap<String, ConcurrentMap<BloomFilter, DfsMessages.DataNodeMetadata>> routingTable;

    // todo decide the m and k for master bloom filter
    BloomFilter masterBloomFilter;
    ServerMessageRouter messageRouter;

//    public static int CHUNK_SIZE = 128; // MB
    public static int CHUNK_SIZE = 10 * 1024; // 10kb

    int m = 1000;
    int k = 20;

    public Controller() {
        activeStorageNodes = new ConcurrentHashMap<>();
        routingTable = new ConcurrentHashMap<>();
        masterBloomFilter = new BloomFilter(m, k);
    }

    public void start(int port) throws IOException {
        messageRouter = new ServerMessageRouter(this);
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
            ChannelHandlerContext ctx, DfsMessages.MessagesWrapper msg) {
        DfsMessages.ControllerMessagesWrapper message = msg.getControllerWrapper();

        int messageType = message.getMsgCase().getNumber();

        switch(messageType){
            case 1: // File Request - STORE/RETRIEVE
                try {
                    System.out.println("Received a file request for " + message.getFileRequest().getFilepath());
                    System.out.println("Request type is  " + message.getFileRequest().getType().name());
                    //get list of nodes client can write to and reply to client with FileResponse
                    if(message.getFileRequest().getType().equals(DfsMessages.FileRequest.Type.STORE))
                        storeFile(ctx, message.getFileRequest());
                    else
                        retrieveFile(ctx, message.getFileRequest());
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

    private List<DfsMessages.DataNodeMetadata> getNodesToStoreFile(long size, int chunks) {
        List<DfsMessages.DataNodeMetadata> nodes = new ArrayList<>();

        Set<String> keyset = activeStorageNodes.keySet();
        System.out.println(activeStorageNodes);
        for(String hostname : keyset) {
            DfsMessages.DataNodeMetadata node = activeStorageNodes.get(hostname);
            if(node.getMemory() > CHUNK_SIZE) {
                nodes.add(node);
            }
        }

        // this returns AT-MAX as many nodes as the number of chunks of file

        if(nodes.size() <= chunks) return nodes;
        return nodes.subList(0, chunks);
    }

    private DfsMessages.MessagesWrapper createClientFileResponseMsg(String systemFilePath, String dfsFilePath, List<DfsMessages.DataNodeMetadata> availableNodes, DfsMessages.FileRequest.Type type, boolean shouldOverwrite) {
        return DfsMessages.MessagesWrapper.newBuilder().setClientWrapper((DfsMessages.ClientMessagesWrapper.newBuilder()
                .setFileResponse(DfsMessages.FileResponse.newBuilder()
                .setSystemFilePath(systemFilePath)
                .setDfsFilePath(dfsFilePath)
                        .setType(type.equals(DfsMessages.FileRequest.Type.STORE) ? DfsMessages.FileResponse.Type.STORE : DfsMessages.FileResponse.Type.RETRIEVE )
                .addAllDataNodes(availableNodes)
                .setShouldOverwrite(shouldOverwrite))
                )).build();
    }

    /**
     * checks to see if directory exists on file request for store and retreive
     */
    private ConcurrentMap<BloomFilter, DfsMessages.DataNodeMetadata> getDirBloomFilters(DfsMessages.FileRequest message) {
        String dfsFilePath = message.getFilepath();

        // get absolute parent directory path without filename
        String dirPath = FileUtils.getDirPath(dfsFilePath);

        // check if directory exists in bloom filter
        return routingTable.getOrDefault(dirPath, null);
    }


    private void retrieveFile(ChannelHandlerContext ctx, DfsMessages.FileRequest message) {
        DfsMessages.MessagesWrapper wrapper;
        String dfsFilePath = message.getFilepath();
        ConcurrentMap<BloomFilter, DfsMessages.DataNodeMetadata> bfMap = getDirBloomFilters(message);

        try {
            if(bfMap == null) {
                // todo file does not exist
                wrapper = createClientFileResponseMsg("", dfsFilePath, null, message.getType(), false);
            }
            else {
                    // get all storage nodes corresponding to the bloom filters
                    List<DfsMessages.DataNodeMetadata> availableNodes = new ArrayList<>(bfMap.values());
                    // send FileResponse with storage nodes that potentially have file chunks
                    wrapper = createClientFileResponseMsg("", dfsFilePath, availableNodes, message.getType(), false);
                }
            ctx.channel().writeAndFlush(wrapper);
        }
        catch(Exception e) {
            System.out.println("error in controller during retrieveFile: " + e);
        }
    }

    private void storeFile(ChannelHandlerContext ctx, DfsMessages.FileRequest message) {
        List<DfsMessages.DataNodeMetadata> availableNodes;
        // 1. check if it exists in routing table
        String dfsFilePath = message.getFilepath();
        String systemFilePath = message.getDirectory();


        System.out.println("dfs file path is: " + dfsFilePath);
        System.out.println("system file path is: " + message.getDirectory());

        long size = message.getSize();
        int chunks = message.getNumChunks();
        boolean shouldOverwrite = false;

        // get absolute parent directory path without filename
        String dirPath = FileUtils.getDirPath(dfsFilePath);

        System.out.println("parent dir path is: " + dirPath);

        // check if directory exists in bloom filter
        ConcurrentMap<BloomFilter, DfsMessages.DataNodeMetadata> bfMap = routingTable.getOrDefault(dirPath, null);

        // 2. if no, add to routing table, FS and master bloom filter
        if(bfMap == null) {
            System.out.println("DIR DOES NOT EXIST: " + dirPath);

            ConcurrentMap<BloomFilter, DfsMessages.DataNodeMetadata> map = new ConcurrentHashMap<>();
            // before adding to routing table, figure out active storage node associated with new file

            availableNodes = getNodesToStoreFile(size, chunks);
            int numNodesAvailable = availableNodes.size();

            if(numNodesAvailable == 0) {
                // todo create and return a new error message to client if all storage nodes are full
            }


            // todo add to FS

            // now that we have space to add file, add directory path to master BF
            byte[] data = dirPath.getBytes();
            masterBloomFilter.put(data);

            int i;
            for(i = 0; i < numNodesAvailable; i++) {
                map.put(new BloomFilter(m,k), availableNodes.get(i));
            }

            // insert entry into routingTable
            routingTable.put(dirPath, map);

        }
        else {
            System.out.println("DIR PATH DOES EXIST: " + dirPath);
            // 3. if yes, get all bloom filters that might have file and return to client with overwrite flag turned on
            availableNodes = new ArrayList<>(bfMap.values());
            // set overWrite flag to true;
            shouldOverwrite = true;
        }

        // send message to clients with all available nodes
        DfsMessages.MessagesWrapper wrapper = createClientFileResponseMsg(systemFilePath, dfsFilePath, availableNodes, message.getType(), shouldOverwrite);
        ctx.channel().writeAndFlush(wrapper);
    }


//    //just populating with three known nodes right now. ideally here the bloomfilter stuff should come into play to create the response
//    private void replyWithNodeInfo(ChannelHandlerContext ctx, String filepath) {
//        DfsMessages.FileResponse fileResponse = DfsMessages.FileResponse.newBuilder()
//                .addDataNodes(0, DfsMessages.DataNodeMetadata.newBuilder().setHostname("alpha").setIp("8000").build())
//                .addDataNodes(1, DfsMessages.DataNodeMetadata.newBuilder().setHostname("beta").setIp("8001").build())
//                .addDataNodes(2, DfsMessages.DataNodeMetadata.newBuilder().setHostname("gamma").setIp("8002").build())
//                .setFilepath(filepath)
//                .build();
//        DfsMessages.ClientMessagesWrapper wrapper = DfsMessages.ClientMessagesWrapper.newBuilder().setFileResponse(fileResponse).build();
//        System.out.println("wrapper = " + wrapper);
//        ctx.channel().writeAndFlush(wrapper);
//    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
    }
}