package au.ooi.streams;

import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.nio.charset.StandardCharsets;


public class DataServiceWriter implements Runnable {
    private final String serviceName;
    private ZMQ.Socket serviceSocket;
    private ZMQ.Socket dataSocket;
    private final ZContext ctx;
    private final String dataServiceUrl;
    private final String dataServiceLocatorUrl;

    public DataServiceWriter(String serviceName, String dataServiceUrl, ZContext ctx, String dataServiceLocatorUrl) {
        this.serviceName = serviceName;
        this.dataServiceUrl = dataServiceUrl;
        this.ctx = ctx;
        this.dataServiceLocatorUrl = dataServiceLocatorUrl;
    }

    public void connect() {
        this.dataSocket = ctx.createSocket(SocketType.PULL);
        this.dataSocket.bind(this.dataServiceUrl);
        System.out.printf("[Writer] bound to %s%n", this.dataServiceUrl);
        this.serviceSocket = ctx.createSocket(SocketType.DEALER);
        this.serviceSocket.connect(dataServiceLocatorUrl);
    }

    public void put(byte[] type) {
        this.dataSocket.send(type);
    }

    @Override
    public void run() {
        serviceSocket.setIdentity(String.format("Writer").getBytes(StandardCharsets.UTF_8));
        while (true) {
            serviceSocket.sendMore("register");
            serviceSocket.sendMore(this.serviceName);
            serviceSocket.send(this.dataServiceUrl);
            System.out.printf("[Writer] Registered %s to %s%n", this.dataServiceUrl, serviceName);
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                // ignored
            }
        }
    }


//    @Override
//    public void run() {
//        while (true) {
//            this.dataSocket.send("Apple");
//
//            if (this.lastCheckTime.isBefore(Instant.now().minusSeconds(10))) {
//                serviceSocket.sendMore("query");
//                serviceSocket.send(this.app);
//
//                ZMsg msg = ZMsg.recvMsg(serviceSocket);
//                ZFrame frame = msg.poll();
//                Set<String> locations = new HashSet<>();
//                while (frame != null) {
//                    String location = frame.getString(ZMQ.CHARSET);
//                    locations.add(location);
//                    frame = msg.poll();
//                }
//                for (String location : locations) {
//                    if (this.locations.contains(location)) {
//                        this.dataSocket.connect(location);
//                        System.out.printf("[Writer] Connecting to new provider %s%n", location);
//                    }
//                }
//                for (String location : this.locations) {
//                    if (!locations.contains(location)) {
//                        this.dataSocket.disconnect(location);
//                        System.out.printf("[Writer] Disconnecting to new provider %s%n", location);
//                    }
//                }
//                this.locations = locations;
//            }
//        }
//    }
}
