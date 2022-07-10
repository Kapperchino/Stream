package stream.states.handlers;

import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import stream.models.proto.requests.ReadRequestOuterClass;

import java.util.concurrent.CompletableFuture;

public interface ReadHandler {
    CompletableFuture<ByteString> handleRead(ReadRequestOuterClass.ReadRequest request);
}
