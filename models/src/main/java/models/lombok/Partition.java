package models.lombok;

import lombok.Builder;
import lombok.Data;
import lombok.NonNull;
import models.proto.record.RecordOuterClass;

import java.nio.file.Paths;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;

@Data
@Builder
public class Partition {
    @NonNull
    Map<Integer, Segment> segmentMap;
    @NonNull
    Map<Integer, RecordInfo> recordInfoMap;
    int partitionId;
    AtomicInteger offset;
    @NonNull
    String topic;

    public Segment getLastSegment() {
        return segmentMap.get(segmentMap.size() - 1);
    }

    public RecordInfo getLastRecord() {
        return recordInfoMap.get(recordInfoMap.size() - 1);
    }

    public Segment getSegment(int offset) {
        if (!recordInfoMap.containsKey(offset)) {
            throw new NoSuchElementException();
        }
        var segmentId = recordInfoMap.get(offset).getSegmentId();
        if (!segmentMap.containsKey(segmentId)) {
            throw new NoSuchElementException();
        }
        return segmentMap.get(segmentId);
    }

    public void putRecordInfo(RecordOuterClass.Record record, int fileOffSet, int segmentId) {
        var id = recordInfoMap.size();
        recordInfoMap.put(id,
                RecordInfo.builder()
                        .size(record.getSerializedSize())
                        .offset(id)
                        .fileOffset(fileOffSet)
                        .segmentId(segmentId).build());
    }

    public RecordInfo getRecord(int offset) {
        if (!recordInfoMap.containsKey(offset)) {
            throw new NoSuchElementException();
        }
        return recordInfoMap.get(offset);
    }

    public Segment addSegment() {
        int i = segmentMap.size() - 1;
        var segment = Segment.builder()
                .relativePath(Paths.get(getFileName(topic, partitionId, i + 1)))
                .segmentId(i + 1)
                .build();
        segmentMap.put(i + 1, segment);
        return segment;
    }

    public static String getFileName(String topicName, int partitionId, int segment) {
        return String.format("%s/%s/%s", topicName, partitionId, segment);
    }
}
