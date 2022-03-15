package models.lombok.dto;

import lombok.Builder;
import lombok.Value;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;

@Value
@Builder
public class WriteFileMeta {
    int index;
    String path;
    boolean close;
    boolean sync;
    long offset;
    ByteString data;

    public FileWrittenMeta getFileWritten() {
        return FileWrittenMeta.builder()
                .index(index)
                .path(path)
                .close(close)
                .sync(sync)
                .offset(offset)
                .build();
    }
}
