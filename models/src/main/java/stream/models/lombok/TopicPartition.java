package stream.models.lombok;

import lombok.Builder;
import lombok.Data;
import lombok.extern.jackson.Jacksonized;

@Data
@Builder
@Jacksonized
public class TopicPartition {
    Partition partition;
    Topic topic;
}
