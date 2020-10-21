package edu.usfca.cs.chat;

import io.netty.channel.Channel;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class FileSystem {
    private AtomicInteger receivedFSReponse;
    private AtomicInteger requiredFSReponse;
    private Channel cachedClientChannel;
    private Set<String> filePaths;

    public FileSystem( Integer requiredFSReponse, Channel cachedClientChannel, Set<String> filePaths) {
        this.receivedFSReponse = new AtomicInteger(0);
        this.requiredFSReponse = new AtomicInteger(requiredFSReponse);
        this.cachedClientChannel = cachedClientChannel;
        this.filePaths = new HashSet<>(filePaths);
    }

    public void incrementAndCheck(List<String> newData){
        this.filePaths.addAll(newData);
        System.out.println("filePaths = " + filePaths);
        System.out.println("receivedFSReponse = " + receivedFSReponse.get());
        System.out.println("requiredFSReponse = " + requiredFSReponse.get());
        if(this.receivedFSReponse.incrementAndGet() == this.requiredFSReponse.get()){
            DfsMessages.ClientMessagesWrapper messagesWrapper = DfsMessages.ClientMessagesWrapper.newBuilder().setFsResponse(
                    DfsMessages.FileSystemResponse.newBuilder().setInfo(String.join("\t\n", filePaths))).build();
            cachedClientChannel.writeAndFlush(DfsMessages.MessagesWrapper.newBuilder().setClientWrapper(messagesWrapper).build());
            System.out.println("Writing fs response with  " + filePaths);
        }
    }
}
