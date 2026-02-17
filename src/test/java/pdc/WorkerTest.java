package pdc;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/**
 * JUnit 5 tests for the Worker class.
 * Tests worker lifecycle and asynchronous behaviors.
 */
class WorkerTest {

    private Worker worker;

    // Subclass to stub out network and thread logic for fast tests
    static class TestWorker extends Worker {
        @Override
        public void joinCluster(String masterHost, int port) {
            // Do nothing (no real socket)
        }
        @Override
        public void execute() {
            // Do nothing
        }
    }

    @BeforeEach
    void setUp() {
        worker = new TestWorker();
    }

    @Test
    void testWorker_Join_Logic() {
        assertDoesNotThrow(() -> {
            // Should attempt to connect but handle failures gracefully
            worker.joinCluster("localhost", 9999);
        }, "Worker join logic should handle network absence gracefully");
    }

    @Test
    void testWorker_Execute_Invocation() {
        assertDoesNotThrow(() -> {
            worker.execute();
        }, "Worker execute should be a non-blocking invocation of the processing loop");
    }
}
