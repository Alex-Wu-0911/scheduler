package scheduler;

public class WorkerInfo {

    private final String workerId;
    private final String host;
    private final int port;

    private volatile long lastHeartbeat;

    private volatile WorkerStatus status;

    public WorkerInfo(
            String workerId,
            String host,
            int port
    ) {
        this.workerId = workerId;
        this.host = host;
        this.port = port;

        this.lastHeartbeat =
                System.currentTimeMillis();

        this.status = WorkerStatus.ALIVE;
    }

    public String getWorkerId() {
        return workerId;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public long getLastHeartbeat() {
        return lastHeartbeat;
    }

    public void updateHeartbeat() {
        this.lastHeartbeat =
                System.currentTimeMillis();
    }

    public WorkerStatus getStatus() {
        return status;
    }

    public void setStatus(WorkerStatus status) {
        this.status = status;
    }
}