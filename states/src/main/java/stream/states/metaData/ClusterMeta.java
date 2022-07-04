package stream.states.metaData;

import io.scalecube.cluster.Member;
import lombok.Builder;
import lombok.Data;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;

import java.util.List;
import java.util.SortedMap;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

@Builder
@Data
public class ClusterMeta {
    SortedMap<String, ShardGroupInfo> raftGroups;
    @Builder.Default
    AtomicLong timeStamp = new AtomicLong(0);

    public void addRaftGroup(List<RaftPeer> peers, String groupId, Member member, long timeStamp) {
        var raftGroup = RaftGroup.valueOf(RaftGroupId.valueOf(UUID.fromString(groupId)), peers);
        raftGroups.put(groupId, ShardGroupInfo.builder()
                .group(raftGroup)
                .member(member)
                .build());
        this.timeStamp.set(timeStamp);
    }

    public void removeRaftGroup(String id) {
        raftGroups.remove(id);
    }

}
