package stream.states.metaData;

import lombok.Builder;
import lombok.Data;
import lombok.Singular;

import java.util.Map;

@Builder
@Data
public class ClusterMeta {
    @Singular
    Map<String,RaftGroupInfo> raftGroups;
}
