package stream.states.metaData;

import io.scalecube.cluster.Member;
import lombok.Builder;
import lombok.Data;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;

import java.util.List;
import java.util.Map;
import java.util.UUID;

@Builder
@Data
public class ClusterMeta {
    Map<String, ShardGroupInfo> raftGroups;

    public void addRaftGroup(List<RaftPeer> peers, String groupId, Member member) {
        var raftGroup = RaftGroup.valueOf(RaftGroupId.valueOf(UUID.fromString(groupId)), peers);
        raftGroups.put(groupId, ShardGroupInfo.builder()
                .group(raftGroup)
                .member(member)
                .build());
    }

    public void removeRaftGroup(String id) {
        raftGroups.remove(id);
    }

}
