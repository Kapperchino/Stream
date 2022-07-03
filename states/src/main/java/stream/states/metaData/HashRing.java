package stream.states.metaData;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.digest.MurmurHash3;

import java.nio.charset.StandardCharsets;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentSkipListMap;

@Slf4j
public class HashRing {
    ConcurrentSkipListMap<Integer, Integer> set;
    String topic;
    final int size = 256;

    public HashRing(String topic) {
        this.topic = topic;
        set = new ConcurrentSkipListMap<>();
    }


    public void addPartition() {
        if (set.isEmpty()) {
            set.put(Math.abs(MurmurHash3.hash32(0) % size), 0);
            return;
        }
        var curSize = set.size();
        set.put(Math.abs(MurmurHash3.hash32(curSize) % size), curSize);
        //splitting and other stuff
    }

    public int findPlacement(String key) {
        if(set.isEmpty()){
            throw new RuntimeException("Empty map");
        }
        var position = Math.abs(MurmurHash3.hash32x86(key.getBytes(StandardCharsets.UTF_8)) % size);
        //gets the partition of the key
        if (set.size() == 1) {
            return set.firstEntry().getValue();
        }
        log.info("hash for this key {} is {}", key, position);
        var floor = set.floorEntry(position);
        //smallest value means the last partition will take it
        if (floor == null) {
            return set.lastEntry().getValue();
        }
        return floor.getValue();
    }

    public void removePartition(int partition) {
        if (set.isEmpty() || set.size() <= partition) {
            log.warn("[HashRing][remove] partition to remove does not exist");
            return;
        }
        set.remove(partition);
        //merge partitions
    }
}
