package models.lombok;

import lombok.Builder;
import lombok.Data;
import java.util.Map;

@Data
@Builder
public class Partition {
    Map<Integer,Segment> segmentMap;
    Map<Integer,RecordInfo> recordInfoMap;
    int partitionId;
    int offset;
    String topic;
}
