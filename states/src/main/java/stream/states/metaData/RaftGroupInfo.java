package stream.states.metaData;

import lombok.Builder;
import lombok.Data;
import org.apache.ratis.protocol.RaftPeer;

import java.util.List;
import java.util.Map;

@Builder
@Data
public class RaftGroupInfo {
    String raftGroupId;
    Map<String,RaftPeer> raftPeers;
}
