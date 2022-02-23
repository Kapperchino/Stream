package models.lombok;

import lombok.Builder;
import lombok.Data;
import java.util.Map;

@Data
@Builder
public class Topic {
    int partitions;
    Map<Long,Partition> partitionMap;
}
