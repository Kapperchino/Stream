package models.lombok;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class TopicPartition {
    Partition partition;
    Topic topic;
}
