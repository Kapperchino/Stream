package stream.states.metaData;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@Slf4j
class HashRingTest {
    HashRing ring;
    static final String TEST_TOPIC = "test";
    @BeforeEach
    void init() {
        ring = new HashRing(TEST_TOPIC);
    }

    @Test
    void addPartition() {
        for(int x=0;x<3;x++){
            ring.addPartition();
        }
        log.info("Hash ring {}",ring.set);
        log.info("key goes to partition {}",ring.findPlacement("asdasd"));
    }

    @Test
    void removePartition() {
    }
}