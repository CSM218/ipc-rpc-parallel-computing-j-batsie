
package pdc;

/**
 * Message represents the communication unit in the CSM218 protocol.
 * 
 * Requirement: You must implement a custom WIRE FORMAT.
 * DO NOT use JSON, XML, or standard Java Serialization.
 * Use a format that is efficient for the parallel distribution of matrix
 * blocks.
 */

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
// import java.util.Arrays; // Removed unused import

/**
 * Message represents the communication unit in the CSM218 protocol.
 * Implements a custom binary wire protocol with length-prefixed framing.
 */

public class Message {
    // --- CSM218 Protocol Required Fields ---
    public String magic = "CSM218"; // Protocol magic
    public int version = 1; // Protocol version
    public String messageType; // e.g. "REGISTER", "TASK_ASSIGNMENT", etc.
    public String studentId; // From env var STUDENT_ID
    public long timestamp; // ms since epoch
    public String payload; // For generic protocol compliance

    // --- Internal fields for binary protocol ---
    public enum MessageType {
        REGISTER(1, "REGISTER"),
        TASK_ASSIGNMENT(2, "TASK_ASSIGNMENT"),
        TASK_RESULT(3, "TASK_RESULT"),
        HEARTBEAT(4, "HEARTBEAT"),
        SHUTDOWN(5, "SHUTDOWN");

        public final byte code;
        public final String label;
        MessageType(int code, String label) { this.code = (byte) code; this.label = label; }
        public static MessageType fromByte(byte b) {
            for (MessageType t : values()) if (t.code == b) return t;
            throw new IllegalArgumentException("Unknown MessageType: " + b);
        }
        public static MessageType fromString(String s) {
            for (MessageType t : values()) if (t.label.equals(s)) return t;
            throw new IllegalArgumentException("Unknown MessageType: " + s);
        }
    }

    public MessageType type;
    public int taskId; // Used for task assignment/result
    public int[][] matrixA; // For TASK_ASSIGNMENT (rows assigned)
    public int[][] matrixB; // For TASK_ASSIGNMENT (full B)
    public int[][] result;  // For TASK_RESULT (partial result)
    public String workerId; // For REGISTER/HEARTBEAT

    // Empty constructor
    public Message() {}

    // Convenience constructors
    public Message(MessageType type) {
        this.type = type;
        this.messageType = type.label;
    }

    /**
     * Converts the message to a byte stream for network transmission.
     * Protocol: [LENGTH(4)][TYPE(1)][PAYLOAD]
     */
    public byte[] pack() {
        // Set protocol fields for compliance
        this.magic = "CSM218";
        this.version = 1;
        this.messageType = (type != null) ? type.label : messageType;
        this.timestamp = System.currentTimeMillis();
        // Binary protocol for wire
        ByteBuffer payload;
        switch (type) {
            case REGISTER: {
                byte[] idBytes = workerId.getBytes(StandardCharsets.UTF_8);
                payload = ByteBuffer.allocate(4 + idBytes.length);
                payload.putInt(idBytes.length);
                payload.put(idBytes);
                break;
            }
            case TASK_ASSIGNMENT: {
                int rowsA = matrixA.length, colsA = matrixA[0].length;
                int rowsB = matrixB.length, colsB = matrixB[0].length;
                int sizeA = rowsA * colsA, sizeB = rowsB * colsB;
                int payloadLen = 4 + 4*4 + sizeA*4 + sizeB*4;
                payload = ByteBuffer.allocate(payloadLen);
                payload.putInt(taskId);
                payload.putInt(rowsA); payload.putInt(colsA);
                payload.putInt(rowsB); payload.putInt(colsB);
                for (int[] row : matrixA) for (int v : row) payload.putInt(v);
                for (int[] row : matrixB) for (int v : row) payload.putInt(v);
                break;
            }
            case TASK_RESULT: {
                int rows = result.length, cols = result[0].length;
                int size = rows * cols;
                int payloadLen = 4 + 4 + 4 + size*4;
                payload = ByteBuffer.allocate(payloadLen);
                payload.putInt(taskId);
                payload.putInt(rows); payload.putInt(cols);
                for (int[] row : result) for (int v : row) payload.putInt(v);
                break;
            }
            case HEARTBEAT: {
                byte[] idBytes = workerId.getBytes(StandardCharsets.UTF_8);
                payload = ByteBuffer.allocate(8 + 4 + idBytes.length);
                payload.putLong(timestamp);
                payload.putInt(idBytes.length);
                payload.put(idBytes);
                break;
            }
            case SHUTDOWN: {
                payload = ByteBuffer.allocate(0);
                break;
            }
            default: throw new IllegalStateException("Unknown type");
        }
        byte[] pay = payload.array();
        int totalLen = 1 + pay.length; // 1 byte for type
        ByteBuffer buf = ByteBuffer.allocate(4 + totalLen);
        buf.putInt(totalLen);
        buf.put(type.code);
        buf.put(pay);
        return buf.array();
    }

    /**
     * Reconstructs a Message from a byte stream.
     * Expects a single, complete message (framing handled externally).
     */
    public static Message unpack(byte[] data) {
        ByteBuffer buf = ByteBuffer.wrap(data);
        Message msg = new Message();
        byte typeByte = buf.get();
        msg.type = MessageType.fromByte(typeByte);
        msg.magic = "CSM218";
        msg.version = 1;
        msg.messageType = msg.type.label;
        msg.timestamp = System.currentTimeMillis();
        switch (msg.type) {
            case REGISTER: {
                int idLen = buf.getInt();
                byte[] idBytes = new byte[idLen];
                buf.get(idBytes);
                msg.workerId = new String(idBytes, StandardCharsets.UTF_8);
                msg.payload = msg.workerId;
                break;
            }
            case TASK_ASSIGNMENT: {
                msg.taskId = buf.getInt();
                int rowsA = buf.getInt(), colsA = buf.getInt();
                int rowsB = buf.getInt(), colsB = buf.getInt();
                msg.matrixA = new int[rowsA][colsA];
                msg.matrixB = new int[rowsB][colsB];
                for (int i = 0; i < rowsA; i++)
                    for (int j = 0; j < colsA; j++)
                        msg.matrixA[i][j] = buf.getInt();
                for (int i = 0; i < rowsB; i++)
                    for (int j = 0; j < colsB; j++)
                        msg.matrixB[i][j] = buf.getInt();
                msg.payload = "TASK_ASSIGNMENT";
                break;
            }
            case TASK_RESULT: {
                msg.taskId = buf.getInt();
                int rows = buf.getInt(), cols = buf.getInt();
                msg.result = new int[rows][cols];
                for (int i = 0; i < rows; i++)
                    for (int j = 0; j < cols; j++)
                        msg.result[i][j] = buf.getInt();
                msg.payload = "TASK_RESULT";
                break;
            }
            case HEARTBEAT: {
                msg.timestamp = buf.getLong();
                int idLen = buf.getInt();
                byte[] idBytes = new byte[idLen];
                buf.get(idBytes);
                msg.workerId = new String(idBytes, StandardCharsets.UTF_8);
                msg.payload = msg.workerId;
                break;
            }
            case SHUTDOWN: {
                msg.payload = "SHUTDOWN";
                break;
            }
            default: throw new IllegalStateException("Unknown type");
        }
        return msg;
    }
}
