package stream.states.metaData;

import io.scalecube.cluster.Member;
import lombok.Builder;
import lombok.Data;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;

import java.util.List;

@Builder
@Data
public class ShardGroupInfo {
    RaftGroup group;
    Member member;

    public static ShardGroupInfo of(RaftGroupId id, List<RaftPeer> peers, Member member) {
        var raftGroup = RaftGroup.valueOf(id, peers);
        return ShardGroupInfo.builder()
                .group(raftGroup)
                .member(member)
                .build();
    }
}
