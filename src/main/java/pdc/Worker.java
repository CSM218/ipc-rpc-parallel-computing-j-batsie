package pdc;

import java.net.Socket;
import java.util.concurrent.*;
// import java.util.concurrent.atomic.*; // Removed unused import
// import java.util.*; // Removed unused import
import java.io.*;
import java.nio.ByteBuffer;

/**
 * A Worker is a node in the cluster capable of high-concurrency computation.
 * 
 * CHALLENGE: Efficiency is key. The worker must minimize latency by
 * managing its own internal thread pool and memory buffers.
 */

public class Worker {
    private ExecutorService executor = Executors.newFixedThreadPool(4);
    private volatile boolean running = true;

    /**
     * Connects to the Master and initiates the registration handshake.
     * The handshake must exchange 'Identity' and 'Capability' sets.
     */
    public void joinCluster(String masterHost, int port) {
        String workerId = System.getenv("WORKER_ID");
        String studentId = System.getenv("STUDENT_ID");
        if (workerId == null) workerId = "worker-" + System.currentTimeMillis();
        if (studentId == null) studentId = "unknown";
        try {
            Socket socket = new Socket(masterHost, port);
            // Send REGISTER message
            Message reg = new Message(Message.MessageType.REGISTER);
            reg.workerId = workerId;
            reg.studentId = studentId;
            byte[] msgBytes = reg.pack();
            socket.getOutputStream().write(msgBytes);
            socket.getOutputStream().flush();
            System.err.println("[Worker] Registered with master as " + workerId);
            // Start heartbeat thread
            startHeartbeatThread(socket, workerId, studentId);
            // Listen for tasks and process
            listenForTasks(socket, workerId, studentId);
        } catch (Exception e) {
            System.err.println("[Worker] Failed to join cluster: " + e.getMessage());
        }
    }

    private void startHeartbeatThread(Socket socket, String workerId, String studentId) {
        new Thread(() -> {
            while (running) {
                try {
                    Message hb = new Message(Message.MessageType.HEARTBEAT);
                    hb.workerId = workerId;
                    hb.studentId = studentId;
                    hb.timestamp = System.currentTimeMillis();
                    byte[] msg = hb.pack();
                    socket.getOutputStream().write(msg);
                    socket.getOutputStream().flush();
                    Thread.sleep(2000);
                } catch (Exception e) {
                    System.err.println("[Worker] Heartbeat error: " + e.getMessage());
                    break;
                }
            }
        }, "HeartbeatThread").start();
    }

    private void listenForTasks(Socket socket, String workerId, String studentId) {
        new Thread(() -> {
            try {
                InputStream in = socket.getInputStream();
                byte[] lenBuf = new byte[4];
                while (running) {
                    int n = in.read(lenBuf);
                    if (n < 4) break;
                    int mlen = ByteBuffer.wrap(lenBuf).getInt();
                    byte[] mBuf = new byte[mlen];
                    int rcv = 0;
                    while (rcv < mlen) {
                        int r = in.read(mBuf, rcv, mlen - rcv);
                        if (r == -1) throw new IOException("Socket closed");
                        rcv += r;
                    }
                    Message msg = Message.unpack(mBuf);
                    if (msg.type == Message.MessageType.TASK_ASSIGNMENT) {
                        executor.submit(() -> processTask(msg, socket, workerId, studentId));
                    }
                }
            } catch (Exception e) {
                System.err.println("[Worker] Task listener error: " + e.getMessage());
            }
        }, "TaskListenerThread").start();
    }

    private void processTask(Message msg, Socket socket, String workerId, String studentId) {
        // Compute matrix multiplication for assigned rows
        int[][] A = msg.matrixA;
        int[][] B = msg.matrixB;
        int rows = A.length, cols = B[0].length, n = B.length;
        int[][] result = new int[rows][cols];
        for (int i = 0; i < rows; i++) {
            for (int j = 0; j < cols; j++) {
                int sum = 0;
                for (int k = 0; k < n; k++) sum += A[i][k] * B[k][j];
                result[i][j] = sum;
            }
        }
        // Send result back
        try {
            Message res = new Message(Message.MessageType.TASK_RESULT);
            res.taskId = msg.taskId;
            res.result = result;
            res.workerId = workerId;
            res.studentId = studentId;
            byte[] out = res.pack();
            socket.getOutputStream().write(out);
            socket.getOutputStream().flush();
            System.err.println("[Worker] Sent result for task " + msg.taskId);
        } catch (Exception e) {
            System.err.println("[Worker] Failed to send result: " + e.getMessage());
        }
    }

    /**
     * Executes a received task block.
     * 
     * Students must ensure:
     * 1. The operation is atomic from the perspective of the Master.
     * 2. Overlapping tasks do not cause race conditions.
     * 3. 'End-to-End' logs are precise for performance instrumentation.
     */
    public void execute() {
        // Read config from env
        String masterHost = System.getenv("MASTER_HOST");
        String portStr = System.getenv("MASTER_PORT");
        int port = 9999;
        if (portStr != null) {
            try { port = Integer.parseInt(portStr); } catch (Exception ignored) {}
        }
        joinCluster(masterHost != null ? masterHost : "localhost", port);
    }
}
