package models.lombok;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.protobuf.CodedOutputStream;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.jackson.Jacksonized;
import models.proto.record.RecordOuterClass.Record;

import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;

@Data
@Builder
@Jacksonized
public class Partition {
    @NonNull
    Map<Integer, Segment> segmentMap;
    @NonNull
    Map<Integer, RecordInfo> recordInfoMap;
    @Getter
    long partitionId;
    AtomicInteger offset;
    @NonNull
    String topic;

    @JsonIgnore
    public Segment getLastSegment() {
        return segmentMap.get(segmentMap.size() - 1);
    }

    @JsonIgnore
    public RecordInfo getLastRecord() {
        return recordInfoMap.get(recordInfoMap.size() - 1);
    }

    public Segment getSegment(int offset) {
        if (!recordInfoMap.containsKey(offset)) {
            return null;
        }
        var segmentId = recordInfoMap.get(offset).getSegmentId();
        if (!segmentMap.containsKey(segmentId)) {
            return null;
        }
        return segmentMap.get(segmentId);
    }

    public void putRecordInfo(Record record, int fileOffSet, int segmentId) {
        var id = recordInfoMap.size();
        var serialized = record.getSerializedSize();
        recordInfoMap.put(id,
                RecordInfo.builder()
                        .size(serialized + CodedOutputStream.computeUInt32SizeNoTag(serialized))
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

    @JsonIgnore
    public Segment addSegment() {
        int i = segmentMap.size() - 1;
        var segment = Segment.builder()
                .relativePath(getFileName(topic, partitionId, i + 1))
                .segmentId(i + 1)
                .build();
        segmentMap.put(i + 1, segment);
        return segment;
    }

    public static String getFileName(String topicName, long partitionId, int segment) {
        return String.format("%s/%s/%s", topicName, partitionId, segment);
    }
}
