package edu.usfca.cs.chat;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.protobuf.ByteString;
import edu.usfca.cs.chat.Utils.FileUtils;
import edu.usfca.cs.chat.net.MessagePipeline;
import edu.usfca.cs.chat.net.ServerMessageRouter;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

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

    private static ConcurrentHashMap<String, Channel> channelMap;
    private static ConcurrentHashMap<String, DfsMessages.FileChunkHeader> fileChunkMetadataMap;
    private static ExecutorService executor;
    ScheduledExecutorService executorService;


    private static String REPLICA_DIR = "replica";
    private static String ORIGINAL_DIR = "original";

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
        channelMap = new ConcurrentHashMap<>();
        fileChunkMetadataMap = new ConcurrentHashMap<>();
        executorService = Executors.newSingleThreadScheduledExecutor();
        executor = Executors.newFixedThreadPool(8);
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
                executor.submit(()-> {
                try {
                    String chunkName = message.getFileChunk().getFilepath();
                    DfsMessages.FileChunkHeader header = message.getFileChunk().getFilechunkHeader();
                    fileChunkMetadataMap.put(chunkName, header);

                    // IF SAME CHUNK REPLICA OR ORIGINAL exists here update it's value as well
                    String tempFileName = chunkName.contains(".replica") ? chunkName.substring(0, chunkName.lastIndexOf('.')) : chunkName + ".replica";
                    System.out.println("TEMP FILE NAME: " + tempFileName);
                    if(fileChunkMetadataMap.keySet().contains(tempFileName)) {
                        System.out.println("NODE ALSO CONTAINS TEMPFILENAME");
                        fileChunkMetadataMap.put(tempFileName, header);
                    }

                    System.out.println(tempMemory.addAndGet(-this.chunkSize));
                    totalStorageReqs.incrementAndGet();

                    System.out.println("FILE PATH TO WRITE: " + message.getFileChunk().getFilepath());
                    System.out.println("TYPE: " + message.getFileChunk().getType());

                    if(message.getFileChunk().getType().equals(DfsMessages.FileChunk.Type.REPLICA)){ //replica
                        System.out.println("inside replica");
                        //  remove word replica from filepath
                        String filePath = message.getFileChunk().getFilepath();
                        String newFilePath = filePath.contains(".replica") ? filePath.substring(0, filePath.lastIndexOf('.')) : filePath;
                        DfsMessages.FileChunk newFC = DfsMessages.FileChunk.newBuilder(message.getFileChunk()).setFilepath(newFilePath).build();
                        writeToFile(newFC,storagePath,"/" + REPLICA_DIR + "/");
                    } else {//leader
                        System.out.println("WRITING TO FILE: ");
                        writeToFile(message.getFileChunk(),storagePath,"/" + ORIGINAL_DIR + "/");
                    }
                    if(message.getFileChunk().getType().equals(DfsMessages.FileChunk.Type.LEADER)) {
                        // replicas only for leader not for maintenance leader which is when a node with original file is down and is replicated
                        sendFileToReplicas(message.getFileChunk());
                    }
                    System.out.println(tempMemory.addAndGet(-this.chunkSize));
                    totalStorageReqs.incrementAndGet();
                    System.out.println("Successfully wrote to the file.");
                } catch (Exception e) {
                    System.out.println("An error occurred while writing to file in storage node.");
                    e.printStackTrace();
                }
                });
                break;
            case 2: // File Ack
                executor.submit(()-> {
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
                        FileUtils.clearDirectoryContents(storagePath + "/" + ORIGINAL_DIR + "/" + dirPath);
                        FileUtils.clearDirectoryContents(storagePath + "/" + REPLICA_DIR + "/" + dirPath);
                        // todo do we need to overwrite .replica folders here as well?
                    }
                }catch (Exception e){
                    System.out.println("Error in fileAck in storage node: " + hostName + " at port: " + hostPort);
                    e.printStackTrace();
                }
                });
        break;
            case 3: // chunk header from client.
                executor.submit(()-> {
                System.out.println("Received chunk header and replica information");
                DfsMessages.FileChunkHeader header = message.getFileChunkHeader();
                // these are the nodes that replication needs to happen on
                List<DfsMessages.DataNodeMetadata> nodesToReplicateOn = header.getReplicasList();
                // this is the list of nodes whose replication status needs to be maintained sequentially
                List<DfsMessages.DataNodeMetadata> nodesToMaintain = header.getMaintenanceNodesList();

                DfsMessages.AlreadyMaintainedChunks alreadyMaintainedChunks = header.getAlreadyMaintainedChunks();

                System.out.println("O_O_O STARTING FILE MAINTENANCE ON NODE: O_O_O" + hostName + ":" + hostPort);
                System.out.println("ALREADY MAINTAINED CHUNKS ARE: " + alreadyMaintainedChunks);
                System.out.println("NODES TO REPLICATE ON: " + nodesToReplicateOn);
                System.out.println("NODES TO MAINTAIN: " + nodesToMaintain);

                initiateReplicationMaintenence(ctx, header, alreadyMaintainedChunks);
                });
                break;
            case 4: //replication Patch.
                executorService.submit(()->{
                    try {
                        String filepath = message.getReplicaPatch().getFilepath();
                        String chunkNum = message.getReplicaPatch().getChunkNum();
                        System.out.println("Replication Patch for " + filepath + "-" + chunkNum);
                        sendReplicationPath(ctx, filepath, chunkNum);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });
//                49162
                break;
            case 5: // ON NODE DOWN MSG
                executor.submit(()-> {
                    DfsMessages.OnNodeDown nodeDown = message.getOnNodeDown();
                    List<DfsMessages.DataNodeMetadata> replicas = message.getOnNodeDown().getAffectedNodesList();

                    System.out.println("X X X X NODE DOWN DETECTED X X X X");
                    System.out.println(nodeDown.getIp());
                    System.out.println(" - - - - - - - - - - - - - - - - - ");
                    sendTotalNodeChunksToReplicateMsg(ctx, nodeDown.getIp(), replicas);
                });
                break;
            default:
                System.out.println("whaaaa");
                break;
        }
    }

    private void sendReplicationPath(ChannelHandlerContext ctx, String filepath, String chunkNum) throws IOException {
        String storageDirectory = this.storagePath + "/replica/";
        File specificChunk = new File(storageDirectory + filepath + "/chunk-" + chunkNum );
        System.out.println("specificChunk.getPath() = " + specificChunk.getPath());
        DfsMessages.FileChunk chunk = getChunks(specificChunk, fileChunkMetadataMap.get(filepath + "-" + chunkNum));
        System.out.println("chunk = " + chunk);
        ctx.channel().writeAndFlush(DfsMessages.MessagesWrapper.newBuilder().setClientWrapper(DfsMessages.ClientMessagesWrapper.newBuilder().setFileChunk(chunk)).build());
        DfsMessages.DataNodeMetadata leaderToFix = fileChunkMetadataMap.get(filepath + "-" + chunkNum).getReplicas(0);
        System.out.println("leaderToFix = " + leaderToFix);
        channelMap.get(leaderToFix.getIp()).writeAndFlush(DfsMessages.MessagesWrapper.newBuilder()
                .setDataNodeWrapper(DfsMessages.DataNodeMessagesWrapper.newBuilder()
                        .setFileChunk(DfsMessages.FileChunk.newBuilder(chunk)).setFileChunkHeader(DfsMessages.FileChunkHeader.getDefaultInstance())).build());
    }

    // need to send total chunks space needed and ip of node that went down so that when the  controller
    // responds it can respond back with the node down ip so we know which node's replication maintenece to initiate
    private DfsMessages.MessagesWrapper getControllerReqForStorage(int totalChunks, String nodeIp, List<DfsMessages.DataNodeMetadata> replicas) {
        return DfsMessages.MessagesWrapper.newBuilder().setControllerWrapper(
                DfsMessages.ControllerMessagesWrapper.newBuilder()
                .setFileChunkHeader(
                DfsMessages.FileChunkHeader.newBuilder()
                .setNodeIp(nodeIp)
                .addAllReplicas(replicas)
                .setTotalChunks(totalChunks)))
                .build();
    }

    private void sendTotalNodeChunksToReplicateMsg(ChannelHandlerContext ctx, String nodeIp, List<DfsMessages.DataNodeMetadata> replicas) {

        int totalChunksSpace = 0;
        // loop through  metadatamap and figure out total replicas and originals to replace
        System.out.println("FILE CHUNK METADATA: ");
        System.out.println(fileChunkMetadataMap);

        for(String key : fileChunkMetadataMap.keySet()) {
            // key is filname-<chunk_number>
            DfsMessages.FileChunkHeader header = fileChunkMetadataMap.get(key);
            List<DfsMessages.DataNodeMetadata> metaDataList = header.getReplicasList();
            for(DfsMessages.DataNodeMetadata node : metaDataList) {
                // if list contains node that went down, add to total chunks to replicate
                if(node.getIp().equals(nodeIp)) totalChunksSpace++;
            }
        }

        // make storage request to controller
        DfsMessages.MessagesWrapper wrapper = getControllerReqForStorage(totalChunksSpace, nodeIp, replicas);

        System.out.println("Sending Request for nodes to start replication on: ");
        System.out.println(wrapper);

        ctx.channel().writeAndFlush(wrapper);
    }

    private Map<String, List<String>> getFilesToBeReplicated(String nodeDownIp, DfsMessages.AlreadyMaintainedChunks alreadyMaintainedChunks) {
        Set<String> originals = new HashSet<>();
        Set<String> replicas = new HashSet<>();

        List<String> completedOrg = new ArrayList<>();
        List<String> completedRep = new ArrayList<>();

        if(alreadyMaintainedChunks != null) {
            completedOrg = alreadyMaintainedChunks.getOriginalChunksList();
            completedRep = alreadyMaintainedChunks.getRepeatedChunksList();
        }

        System.out.println("completedOrg: " + completedOrg);
        System.out.println("completedRep: " + completedRep);

        for(String dir : fileChunkMetadataMap.keySet()) {
            DfsMessages.FileChunkHeader valueHeader = fileChunkMetadataMap.get(dir);
            List<DfsMessages.DataNodeMetadata> replicatedNodes = valueHeader.getReplicasList();

            int i;
            for(i = 0; i < replicatedNodes.size(); i++) {
                DfsMessages.DataNodeMetadata node = replicatedNodes.get(i);
                String orgFileName = dir.contains(".replica") ? dir.substring(0, dir.lastIndexOf('.')) : dir;

                if(nodeDownIp.equals(node.getIp())) {
                    if(i == 0) {
                        // node that went down was the original since it's in the first position
                        // make an original
                        // check to make sure that the original's replication has not already been done
                        if(!completedOrg.contains(orgFileName)) originals.add(orgFileName);
                    }
                    else {
                        // check to make sure that the replica's replication has not already been done
                        if(!completedRep.contains(orgFileName)) replicas.add(orgFileName);
                    }
                }
            }
        }

        Map<String, List<String>> map = new HashMap<>();
        map.put(ORIGINAL_DIR, new ArrayList<>(originals));
        map.put(REPLICA_DIR, new ArrayList<>(replicas));

        System.out.println("FILES THAT ARE ALREADY MAINTAINED FROM PREVIOUS NODES: " + alreadyMaintainedChunks);
        System.out.println("FILES TO BE REPLICATED ON NODE: " + hostName + ":" + hostPort);
        System.out.println(map);

        return map;
    }

    private String createPathFromChunkName(String chunkName, String filePresentInDir) {
        int idx = chunkName.lastIndexOf('-');
        String fileName = chunkName.substring(0,idx);
        String chunk = "/chunk" + chunkName.substring(idx);

        return storagePath + "/" + filePresentInDir + "/" + fileName + chunk;
    }

    private List<DfsMessages.DataNodeMetadata> replicateChunks(String nodeDownIp, List<String> chunkList, List<DfsMessages.DataNodeMetadata> nodesToReplicateOn, DfsMessages.FileChunk.Type type) {
        int i = 0;
        int chunkIdx = 0;

        List<DfsMessages.DataNodeMetadata> alreadyMaintainedNodes = new ArrayList<>();

        while(chunkIdx < chunkList.size()) {
            String chunkName = chunkList.get(chunkIdx++);
            System.out.println("Replicating chunk name: " + chunkName);
            System.out.println("TYPE: " +  type);

            // find file which is located in replica and create it's original on a new node
            String dirToFindFileIn = type.equals(DfsMessages.FileChunk.Type.MAINTENANCE_LDR) ? REPLICA_DIR : ORIGINAL_DIR;
            String oppDir = dirToFindFileIn.equalsIgnoreCase(REPLICA_DIR) ? ORIGINAL_DIR : REPLICA_DIR;

            String absFilePath = createPathFromChunkName(chunkName, dirToFindFileIn);
            String oppPath = createPathFromChunkName(chunkName, oppDir);

            File fileChunk = new File(absFilePath);

            if(!fileChunk.isFile()) {
                fileChunk = new File(oppPath);
            }

            if (i == nodesToReplicateOn.size()) i = 0;
            DfsMessages.DataNodeMetadata replicateTo = nodesToReplicateOn.get(i++);

            System.out.println("connecting to: " + replicateTo.getIp().split(":")[0] + ":" + replicateTo.getPort());
            Channel ch = connectToNode(replicateTo.getIp().split(":")[0], replicateTo.getPort());
            alreadyMaintainedNodes.add(replicateTo);

            // if current node already has a replica don't create it here
            if(type.equals(DfsMessages.FileChunk.Type.REPLICA)) {
                String chunkIdentifier = chunkName + ".replica";
                if(fileChunkMetadataMap.keySet().contains(chunkIdentifier)) {
                    if(replicateTo.getHostname().equals(hostName) && replicateTo.getPort() == hostPort) {
                        // node to replicate on already contains the file
                        if (i == nodesToReplicateOn.size()) i = 0;
                        replicateTo = nodesToReplicateOn.get(i++);
                    }
                }
            }

            byte[] bytes = new byte[(int) fileChunk.length()];
            // funny, if can use Java 7, please uses Files.readAllBytes(path)
            try(FileInputStream fis = new FileInputStream(fileChunk)){
                fis.read(bytes);
            }
            catch(Exception e) {
                System.out.println("error converting file chunk to byte array: ");
                e.printStackTrace();
            }

            // before replicating update the filechunkmetadatamap with the new node
            DfsMessages.FileChunkHeader header = fileChunkMetadataMap.getOrDefault(chunkName, null);
            if(header == null) header = fileChunkMetadataMap.get(chunkName + ".replica");
            List<DfsMessages.DataNodeMetadata> replicas = new ArrayList<>(header.getReplicasList());


            System.out.println("UPDATING REPLICA LIST");
            if(type.equals(DfsMessages.FileChunk.Type.MAINTENANCE_LDR)) {
                // the node the request is being sent to is going to be the new original of this file
                // update idx 0
                // update the replicas list with new node if currently present on this node
                replicas.set(0, replicateTo);
                // ALSO UPDATE IT FOR THE REPLICA'S METADATA if same chunk replica exists on current node
            }
            else {
                boolean replicate = false;
                int k;
                for(k = 1; k < replicas.size(); k++) {
                    DfsMessages.DataNodeMetadata node = replicas.get(k);
                    System.out.println("REPLICA SET NODE IP: " + node.getIp());
                    if(node.getIp().equals(nodeDownIp)) {
                        replicate = true;
                        break;
                    }
                }
                if(replicate) {
                    System.out.println("FOUND REPLICA!!!");
                    replicas.set(k, replicateTo);
                }
            }

            DfsMessages.FileChunkHeader newHeader = DfsMessages.FileChunkHeader.newBuilder()
            .addAllReplicas(replicas)
            .build();

            //  get replicas for creating filechunkheader
            System.out.println("FIND CHUNK NAME: " + chunkName);
            System.out.println(fileChunkMetadataMap.keySet());

            if(type.equals(DfsMessages.FileChunk.Type.REPLICA)) {
                chunkName = chunkName + ".replica";
            }

            // also update current nodes filechunkmetadata map right?
            System.out.println("UPDATING CURRENT NODE FILECHUNKMETADATA");
            fileChunkMetadataMap.put(chunkName, newHeader);

            // ALSO update filechunkmetadata of the replica chunk is it exists here or the original if replica exists here
            String tempFileName = chunkName.contains(".replica") ? chunkName.substring(0, chunkName.lastIndexOf('.')) : chunkName + ".replica";
            System.out.println("TEMP FILE NAME: " + tempFileName);
            if(fileChunkMetadataMap.keySet().contains(tempFileName)) {
                System.out.println("NODE ALSO CONTAINS TEMPFILENAME");
                fileChunkMetadataMap.put(tempFileName, newHeader);
            }


            System.out.println("FOR FILE: " + chunkName);
            System.out.println("NEW HEADER REPLICAS ARE: " + newHeader);


            // create filechunk msg
            DfsMessages.MessagesWrapper message = DfsMessages.MessagesWrapper.newBuilder()
                    .setDataNodeWrapper(DfsMessages.DataNodeMessagesWrapper.newBuilder()
                    .setFileChunk(DfsMessages.FileChunk.newBuilder()
                    .setFilechunkHeader(DfsMessages.FileChunkHeader.newBuilder(newHeader).build())
                    .setChunks(ByteString.copyFrom(bytes))
                    .setFilepath(chunkName)
                    .setType(type))
                    .build())
                    .build();

            ch.writeAndFlush(message);

        }
        return alreadyMaintainedNodes;
    }

    private DfsMessages.DataNodeMetadata getNextNode(List<DfsMessages.DataNodeMetadata> nodesToMaintain) {
        int i;
        int len = nodesToMaintain.size();
        for(i = 0; i < len; i++) {
            DfsMessages.DataNodeMetadata node = nodesToMaintain.get(i);
            if(node.getHostname().equals(this.hostName) && node.getPort() == this.hostPort) {
                if(i + 1 < len) return nodesToMaintain.get(i+1);
            }
        }
        return null;
    }

    private List<String> createListOfFilepathsReplicated(Map<String, List<String>> map) {
        List<String> originalChunks = map.get(ORIGINAL_DIR);
        List<String> replicaChunks  = map.get(REPLICA_DIR);

        Set<String> allFilePaths = new HashSet<>();

        originalChunks.addAll(replicaChunks);

        for(String filechunk : originalChunks) {
            // return the parent dir path since we're sending this to controller to update routing table
            if(filechunk.contains(".replica")) filechunk = filechunk.substring(0, filechunk.lastIndexOf('.'));
            String filepath = FileUtils.getDirPath(filechunk);
            allFilePaths.add(filepath);
        }

        System.out.println("UNIQUE FILEPATHS UPDATED: " + allFilePaths);
        System.out.println("set length: " + allFilePaths.size());
        return new ArrayList<>(allFilePaths);
    }

    private DfsMessages.MessagesWrapper createUpdateRoutingTableMsg(List<String> updatedDirPaths, String nodeIp) {
        return DfsMessages.MessagesWrapper.newBuilder()
                .setControllerWrapper(DfsMessages.ControllerMessagesWrapper.newBuilder()
                .setUpdateRoutingTable(DfsMessages.UpdateRoutingTable.newBuilder()
                .setNodeIp(nodeIp)
                .addAllDirpath(updatedDirPaths)
                .build())
                .build())
                .build();
    }

    private void updateFileChunkMetaDataFromMaintainedNodes(String nodeDownIp, DfsMessages.AlreadyMaintainedChunks alreadyMaintainedChunks) {

        List<String> maintainedRepChunks = new ArrayList<>(alreadyMaintainedChunks.getRepeatedChunksList());

        List<DfsMessages.DataNodeMetadata> allMaintainedNodes = new ArrayList<>(alreadyMaintainedChunks.getMaintainedOriginalsList());
        allMaintainedNodes.addAll(alreadyMaintainedChunks.getMaintainedReplicasList());

        List<String> allMaintainedChunks = new ArrayList<>(alreadyMaintainedChunks.getOriginalChunksList());
        maintainedRepChunks.forEach(chunk -> allMaintainedChunks.add(chunk + ".replica"));

        System.out.println("ALL MAINTAINED CHUNKS: " + allMaintainedChunks);
        System.out.println("ALL MAINTAINED NODES: " + allMaintainedNodes);

        int j;
        for(j = 0; j < allMaintainedChunks.size(); j++) {
            String chunkName = allMaintainedChunks.get(j);
            System.out.println("CHUNK NAME: " + chunkName);

            DfsMessages.FileChunkHeader header = fileChunkMetadataMap.getOrDefault(chunkName, null);


            if(header == null) {
                // check to see if we have the replica file or original if current is replica
                String tempFileName = chunkName.contains(".replica") ? chunkName.substring(0, chunkName.lastIndexOf('.')) : chunkName + ".replica";
                header = fileChunkMetadataMap.getOrDefault(tempFileName, null);
            };
            if(header == null) continue;

            // find nodeDownIp node, remove it and replace it with the new node that now holds the maintained replica
            List<DfsMessages.DataNodeMetadata> replicas = new ArrayList<>(header.getReplicasList());
            boolean isPresent = false;
            int i;
            for(i = 0; i < replicas.size(); i++) {
                DfsMessages.DataNodeMetadata node = replicas.get(i);
                if(node.getIp().equals(nodeDownIp)) {
                    System.out.println("NODE GETTING REPLACED: " + node);
                    isPresent = true;
                    break;
                }
            }
            // replace node
            if(isPresent) {
                System.out.println("NODE BEING REPLACED WITH: " + allMaintainedNodes.get(j));
                System.out.println("ALL REPLICAS: " + replicas);
                System.out.println("idx being replaced: " + i);
                replicas.set(i, allMaintainedNodes.get(j));
                System.out.println("AFTE NODES REPLACED: " + replicas);
                DfsMessages.FileChunkHeader newHeader = DfsMessages.FileChunkHeader.newBuilder(header).clearReplicas().addAllReplicas(replicas).build();
                fileChunkMetadataMap.put(chunkName, newHeader);
            }



        }

    }

    private void initiateReplicationMaintenence(ChannelHandlerContext ctx, DfsMessages.FileChunkHeader header, DfsMessages.AlreadyMaintainedChunks alreadyMaintainedChunks) {
        String nodeIp = header.getNodeIp();
        List<DfsMessages.DataNodeMetadata> nodesToReplicateOn = header.getReplicasList();

        // update filechunkmetadata map with the already maintained nodes
        updateFileChunkMetaDataFromMaintainedNodes(nodeIp, alreadyMaintainedChunks);

        // go through filechunkmetadatamap and find all files that need to be replicated
        Map<String, List<String>> map = getFilesToBeReplicated(nodeIp, alreadyMaintainedChunks);

        // divide them between original and replicas that need to be replicated
        List<String> originalChunks = map.get(ORIGINAL_DIR);
        List<String> replicaChunks  = map.get(REPLICA_DIR);

        System.out.println("storagepath: " + storagePath);
        String originalDir = storagePath + "/" + ORIGINAL_DIR + "/";
        System.out.println("original dir: " + originalDir);

        // start replicating originals
        List<DfsMessages.DataNodeMetadata> maintainedOrgNodes = replicateChunks(nodeIp, originalChunks, nodesToReplicateOn, DfsMessages.FileChunk.Type.MAINTENANCE_LDR);

        // start replicating replicas
        List<DfsMessages.DataNodeMetadata> maintainedRepNodes = replicateChunks(nodeIp, replicaChunks, nodesToReplicateOn, DfsMessages.FileChunk.Type.REPLICA);

        // create a map of file chunks and whether an original or replica was created for them
        List<DfsMessages.DataNodeMetadata> nodesToMaintain = header.getMaintenanceNodesList();

        // send this map to the next storage node on the list
        DfsMessages.DataNodeMetadata node = getNextNode(nodesToMaintain);

        // if next node is not null keep cascading
        if(node != null) {
            if(alreadyMaintainedChunks != null) {
                //  grow the list with existing replications done by previous nodes
                originalChunks.addAll(alreadyMaintainedChunks.getOriginalChunksList());
                replicaChunks.addAll(alreadyMaintainedChunks.getRepeatedChunksList());
                // add the datanodemetadata as well in the right order as the chunks
                maintainedOrgNodes.addAll(alreadyMaintainedChunks.getMaintainedOriginalsList());
                maintainedRepNodes.addAll(alreadyMaintainedChunks.getMaintainedReplicasList());
            }

            // Send next node a FileChunkHeader msg with all info for cascading maintenance
            DfsMessages.MessagesWrapper wrapper = DfsMessages.MessagesWrapper.newBuilder()
            .setDataNodeWrapper(
            DfsMessages.DataNodeMessagesWrapper.newBuilder()
            .setFileChunkHeader(
            DfsMessages.FileChunkHeader.newBuilder()
            .addAllReplicas(nodesToReplicateOn)
            .addAllMaintenanceNodes(nodesToMaintain)
            .setNodeIp(nodeIp)
            .setAlreadyMaintainedChunks(
            DfsMessages.AlreadyMaintainedChunks.newBuilder()
            .addAllRepeatedChunks(replicaChunks)
            .addAllOriginalChunks(originalChunks)
            .addAllMaintainedOriginals(maintainedOrgNodes)
            .addAllMaintainedReplicas(maintainedRepNodes)
            .build())
            .build())
            .build())
            .build();

            System.out.println(wrapper);

            String ip = node.getIp().split(":")[0];
            int port = node.getPort();
            System.out.println("NEXT NODE TO REPLICATE ON: " + ip + ":" + port);
            Channel ch = connectToNode(ip, port);

            ch.writeAndFlush(wrapper);
        }


        // send msg to controller to update routing table with files replicated on this node
        System.out.println("SEND MSG TO CONTROLLER WITH UPDATED NODES");
        List<String> filesUpdatedDirPath = createListOfFilepathsReplicated(map);

        DfsMessages.MessagesWrapper message = createUpdateRoutingTableMsg(filesUpdatedDirPath, nodeIp);
        System.out.println("update routing table msg: " + message);

        Channel ctrlCh = channelMap.getOrDefault(controllerHostname,  connectToNode(controllerHostname, controllerPort));
        ctrlCh.writeAndFlush(message);
    }

    private void getAndSendChunks(ChannelHandlerContext ctx, String filepath) throws IOException {
        String storageDirectory = this.storagePath + "/original/";

        File dir = new File(storageDirectory + filepath);
        List<DfsMessages.MessagesWrapper> chunks = new ArrayList<>();
        String currChunkName = "";
        File[] allFiles = dir.listFiles();

        if(allFiles == null) return;

        for (File eachFile: allFiles) {
            if (!eachFile.getName().contains("checksum") &&  !eachFile.getName().contains("metadata")){
                System.out.println("eachFile = " + eachFile);
                String currChunkNum = eachFile.getName().split("-")[1];
                currChunkName = filepath + "-" +  currChunkNum;
                System.out.println("currChunkName = " + currChunkName);
                DfsMessages.FileChunk chunk = getChunks(eachFile, fileChunkMetadataMap.get(currChunkName));
                if(chunk == null){
                    DfsMessages.DataNodeMetadata nodeMetadata = fileChunkMetadataMap.get(currChunkName).getReplicas(1);
                    DfsMessages.ReplicaPatch replicaPatch = DfsMessages.ReplicaPatch.newBuilder().setNodeMetadata(nodeMetadata).setFilepath(filepath).setChunkNum(currChunkNum).build();
                    System.out.println("replicaPatch = " + replicaPatch);
                    channelMap.get(nodeMetadata.getIp()).writeAndFlush(DfsMessages.MessagesWrapper.newBuilder().setDataNodeWrapper(DfsMessages.DataNodeMessagesWrapper.newBuilder().setReplicaPatch(replicaPatch)));
                    ctx.channel().writeAndFlush(DfsMessages.MessagesWrapper.newBuilder().setClientWrapper(DfsMessages.ClientMessagesWrapper.newBuilder().setReplicaPatch(replicaPatch)));
                    System.out.println("Waiting");
                    System.out.println("Asked for replica patch for " + eachFile.getName());
                } else {
                    ctx.channel().writeAndFlush(DfsMessages.MessagesWrapper.newBuilder().setClientWrapper(DfsMessages.ClientMessagesWrapper.newBuilder().setFileChunk(chunk)));
                    System.out.println("Sent to client " + eachFile.getName());
                }
            }
        }
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
        String filePath = fileChunk.getFilepath() + ".replica";
        System.out.println("filePath = " + filePath);
        //0-leader, 1,2- replicas

        DfsMessages.FileChunkHeader currHeader = DfsMessages.FileChunkHeader.newBuilder(fileChunk.getFilechunkHeader()).build();
        DfsMessages.FileChunk currFileChunk = DfsMessages.FileChunk.newBuilder(fileChunk).setFilepath(filePath).setType(DfsMessages.FileChunk.Type.REPLICA).setFilechunkHeader(currHeader).setType(DfsMessages.FileChunk.Type.REPLICA).build();
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
