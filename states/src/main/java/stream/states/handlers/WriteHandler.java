package stream.states.handlers;

import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import stream.models.proto.requests.WriteRequestOuterClass;

import java.util.concurrent.CompletableFuture;

public interface WriteHandler {
    CompletableFuture<?> handleWrite(WriteRequestOuterClass.WriteRequest request, LogEntryProto entry);
}
