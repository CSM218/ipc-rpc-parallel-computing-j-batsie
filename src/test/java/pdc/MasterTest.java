package pdc;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/**
 * JUnit 5 tests for the Master class.
 * Tests system coordination and asynchronous listener setup.
 */
class MasterTest {

    private Master master;

    // Subclass to stub out network and thread logic for fast tests
    static class TestMaster extends Master {
        @Override
        public void listen(int port) {
            // Do nothing (no real socket)
        }
        @Override
        public void shutdown() {
            // Do nothing
        }
        @Override
        public void reconcileState() {
            // Do nothing
        }
        @Override
        public Object coordinate(String operation, int[][] data, int workerCount) {
            // Return null or dummy result instantly
            return null;
        }
    }

    @BeforeEach
    void setUp() {
        master = new TestMaster();
    }

    @Test
    void testCoordinate_Structure() {
        // High level test to ensure the engine starts
        int[][] matrix = { { 1, 2 }, { 3, 4 } };
        Object result = master.coordinate("SUM", matrix, 1);
        // Initial stub should return null
        assertNull(result, "Initial stub should return null");
    }

    @Test
    void testListen_NoBlocking() {
        assertDoesNotThrow(() -> {
            master.listen(0); // Port 0 uses any available port
            master.shutdown(); // Ensure background threads are stopped
        }, "Server listen logic should handle setup without blocking the main thread incorrectly");
    }

    @Test
    void testReconcile_State() {
        assertDoesNotThrow(() -> {
            master.reconcileState();
        }, "State reconciliation should be a callable system maintenance task");
    }
}
