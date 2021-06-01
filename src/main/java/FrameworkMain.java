import lombok.EqualsAndHashCode;
import lombok.Value;
import org.zeromq.*;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class FrameworkMain {
    public static final String IPC_SERVICE_LOCATOR = "ipc://./service-locator";
    static ZContext ctx = new ZContext();
    public static final String FRUITS_PROCESSOR_NAME = "fruits";

    public static int readerCount = 0;

    static class ReaderClient implements Runnable {

        private final ZMQ.Socket serviceSocket;
        private final ZMQ.Socket dataSocket;
        private final Thread thread;
        private final String location;

        private final int readerCount = FrameworkMain.readerCount++;

        public ReaderClient(String name) {
            dataSocket = ctx.createSocket(SocketType.PULL);
            this.location = "ipc://./location-only-available-from-reader" + "-" + readerCount;
            dataSocket.bind(location);
            System.out.println("Reader bound to " + this.location);

            serviceSocket = ctx.createSocket(SocketType.DEALER);
            serviceSocket.connect(IPC_SERVICE_LOCATOR);
            thread = new Thread(() -> {
                serviceSocket.setIdentity("Reader".getBytes(StandardCharsets.UTF_8));
                while (true) {
                    serviceSocket.sendMore("register");
                    serviceSocket.sendMore(name);
                    serviceSocket.send(location);
                    try {
                        Thread.sleep(10000);
                    } catch (InterruptedException e) {
                        // ignored
                    }
                }
            });
            thread.start();
        }

        @Override
        public void run() {
            while (true) {
                System.out.println("[Reader-" + readerCount + "]" + dataSocket.recvStr());
            }
        }
    }

    static class WriterClient implements Runnable {
        private final String app;
        private final ZMQ.Socket serviceSocket;
        private boolean connected = false;
        private ZMQ.Socket dataSocket;
        private Set<String> locations = new HashSet<>();

        public WriterClient(String app) {
            this.app = app;
            serviceSocket = ctx.createSocket(SocketType.DEALER);
            serviceSocket.setReceiveTimeOut(5000);
            serviceSocket.connect(IPC_SERVICE_LOCATOR);
            System.out.printf("writer connected service locator at %s%n", IPC_SERVICE_LOCATOR);
        }

        @Override
        public void run() {
            while (true) {
                while (!connected) {
                    connect();
                }
                this.dataSocket.send("Apple");

                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    // ignored
                }

            }
        }

        private boolean connect() {
            if (dataSocket != null) {
                dataSocket.close();
            }
            serviceSocket.sendMore("query");
            serviceSocket.send(this.app);

            ZMsg msg = ZMsg.recvMsg(serviceSocket);
            ZFrame frame = msg.poll();
            List<String> locations = new ArrayList<>();
            while (frame != null) {
                String location = frame.getString(ZMQ.CHARSET);
                locations.add(location);
                frame = msg.poll();
            }

            if (!locations.isEmpty() && !locations.contains("Not Found")) {
                this.dataSocket = ctx.createSocket(SocketType.PUSH);
                for (String location : locations) {
                    this.dataSocket.connect(location);
                    System.out.println("Writer connecting to " + location);
                }
                this.connected = true;
            }
            return this.connected;
        }

    }

    static class ServiceLocator implements Runnable {

        @Value
        @EqualsAndHashCode(exclude = "lastUpdateTime")
        class Location {
            String location;
            Instant lastUpdateTime;
        }

        private final ZMQ.Socket socket;
        private final Map<String, Set<Location>> map = new ConcurrentHashMap<>();

        public ServiceLocator() {
            socket = ctx.createSocket(SocketType.ROUTER);
            socket.bind(IPC_SERVICE_LOCATOR);
        }

        public void run() {
            while (true) {
                ZMsg msg = ZMsg.recvMsg(socket);
                assert (msg.size() >= 3);
                ZFrame address = msg.poll();

                ZFrame req = msg.poll();
                assert (req != null);

                String requestStr = req.getString(ZMQ.CHARSET);
                switch (requestStr) {
                    case "query" -> {
                        ZFrame data = msg.poll();
                        assert (data != null);
                        Set<Location> locations = map.get(data.getString(ZMQ.CHARSET));
                        ZMsg result = new ZMsg();
                        result.add(address);
                        if (locations != null) {
                            for (Location location : locations) {
                                result.add(location.getLocation());
                            }
                        } else {
                            result.add("Not Found");
                        }
                        result.send(socket);
                    }
                    case "register" -> {
                        ZFrame nameFrame = msg.poll();
                        assert (nameFrame != null);
                        String name = nameFrame.getString(ZMQ.CHARSET);

                        ZFrame locationFrame = msg.poll();
                        assert (locationFrame != null);
                        String location = locationFrame.getString(ZMQ.CHARSET);
                        this.map.putIfAbsent(name, new HashSet<>());
                        Set<Location> locations = this.map.get(name);
                        if (locations != null) {
                            locations.add(new Location(location, Instant.now()));
                        }
                    }
                }
            }
        }
    }

    public static void main(String[] args) throws InterruptedException {
        ServiceLocator serviceLocator = new ServiceLocator();
        ExecutorService executorService = Executors.newCachedThreadPool();
        executorService.submit(serviceLocator);

        executorService.submit(new ReaderClient(FRUITS_PROCESSOR_NAME));
        executorService.submit(new ReaderClient(FRUITS_PROCESSOR_NAME));
        executorService.submit(new WriterClient(FRUITS_PROCESSOR_NAME));

        while (true) {
            Thread.sleep(500);
        }
    }
}
