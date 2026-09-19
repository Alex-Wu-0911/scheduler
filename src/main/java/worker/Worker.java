package worker;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.ServerSocket;
import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class Worker {

    private final String workerId;
    private final String host;
    private final int port;
    private final ScheduledExecutorService heartbeatExecutor;
    // Network handlers must not wait for task execution to finish.
    private final ThreadPoolExecutor connectionPool = new ThreadPoolExecutor(
            2, 2, 0, TimeUnit.SECONDS, new ArrayBlockingQueue<>(20),
            new ThreadPoolExecutor.AbortPolicy());
    private final ThreadPoolExecutor taskPool = new ThreadPoolExecutor(
            3, 3, 0, TimeUnit.SECONDS, new ArrayBlockingQueue<>(10),
            new ThreadPoolExecutor.AbortPolicy());

    public Worker(
            String workerId,
            String host,
            int port
    ) {
        this.workerId = workerId;
        this.host = host;
        this.port = port;
        this.heartbeatExecutor =
                Executors.newSingleThreadScheduledExecutor();
    }

    public static void main(String[] args)
            throws Exception {

        Worker worker = new Worker(
                "worker-1",
                "localhost",
                9001
        );

        worker.start("localhost", 8080);
    }

    public void start(String schedulerHost, int schedulerPort) throws Exception {
        // Bind before registration so the advertised port is ready to accept connections.
        try (ServerSocket server = new ServerSocket(port)) {
            register(schedulerHost, schedulerPort);
            startHeartbeat(schedulerHost, schedulerPort);
            System.out.println("Worker " + workerId + " listening on " + port);
            while (true) {
                Socket socket = server.accept();
                try {
                    socket.setSoTimeout(3000);
                    connectionPool.execute(() -> handleTask(socket));
                } catch (RejectedExecutionException | IOException e) {
                    socket.close();
                }
            }
        } finally {
            heartbeatExecutor.shutdownNow();
            connectionPool.shutdownNow();
            taskPool.shutdownNow();
        }
    }

    private void handleTask(Socket socket) {
        try (socket;
             BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
             PrintWriter writer = new PrintWriter(socket.getOutputStream(), true)) {
            String request = reader.readLine();
            if (request == null) return;
            String[] parts = request.split("\\|", -1);
            if (parts.length != 3 || !"EXECUTE".equals(parts[0]) || parts[1].isBlank()) {
                writer.println("ERROR|INVALID_EXECUTE");
                return;
            }
            int durationMs;
            try {
                durationMs = Integer.parseInt(parts[2]);
                if (durationMs < 0) throw new NumberFormatException();
            } catch (NumberFormatException e) {
                writer.println("ERROR|INVALID_DURATION");
                return;
            }
            String taskId = parts[1];
            try {
                taskPool.execute(() -> executeTask(taskId, durationMs));
                writer.println("ACCEPTED|" + taskId);
            } catch (RejectedExecutionException e) {
                writer.println("SERVER_BUSY|" + taskId);
            }
        } catch (IOException e) {
            System.out.println("Worker connection error: " + e.getMessage());
        }
    }

    private void executeTask(String taskId, int durationMs) {
        String executor = workerId + "/" + Thread.currentThread().getName();
        System.out.println(executor + " starts " + taskId);
        try {
            Thread.sleep(durationMs);
            System.out.println(executor + " completed " + taskId);
        } catch (InterruptedException e) {
            // sleep clears the interrupt flag when throwing; restore it for callers.
            Thread.currentThread().interrupt();
            System.out.println(executor + " interrupted " + taskId);
        }
    }

    public void register(
            String schedulerHost,
            int schedulerPort
    ) throws Exception {

        try (
                Socket socket =
                        new Socket(
                                schedulerHost,
                                schedulerPort
                        );

                BufferedReader reader =
                        new BufferedReader(
                                new InputStreamReader(
                                        socket.getInputStream()
                                )
                        );

                PrintWriter writer =
                        new PrintWriter(
                                socket.getOutputStream(),
                                true
                        )
        ) {

            writer.println(
                    "REGISTER|"
                            + workerId
                            + "|"
                            + host
                            + "|"
                            + port
            );

            String response =
                    reader.readLine();

            if (!("REGISTERED|" + workerId).equals(response)) {
                throw new IOException("Worker registration rejected: " + response);
            }

            System.out.println(
                    "Scheduler: " + response
            );
        }
    }

    public void startHeartbeat(
            String schedulerHost,
            int schedulerPort
    ) {

        heartbeatExecutor.scheduleAtFixedRate(
                () -> sendHeartbeat(
                        schedulerHost,
                        schedulerPort
                ),
                0,
                2,
                TimeUnit.SECONDS
        );
    }

    private void sendHeartbeat(
            String schedulerHost,
            int schedulerPort
    ) {

        try (
                Socket socket =
                        new Socket(
                                schedulerHost,
                                schedulerPort
                        );

                BufferedReader reader =
                        new BufferedReader(
                                new InputStreamReader(
                                        socket.getInputStream()
                                )
                        );

                PrintWriter writer =
                        new PrintWriter(
                                socket.getOutputStream(),
                                true
                        )
        ) {

            writer.println(
                    "HEARTBEAT|" + workerId
            );

            String response =
                    reader.readLine();

            System.out.println(
                    "Heartbeat response: "
                            + response
            );

        } catch (Exception e) {

            System.out.println(
                    "Heartbeat failed: "
                            + e.getMessage()
            );
        }
    }
}
