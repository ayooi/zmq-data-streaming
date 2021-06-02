package au.ooi.streams;

import org.zeromq.*;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class DataServiceLocator implements Runnable {

    private final ZMQ.Socket socket;
    private final int timeoutSeconds;

    private final ServiceStore serviceStore = new ServiceStore();

    public DataServiceLocator(ZContext ctx, String serviceBindUrl, int timeoutSeconds) {
        socket = ctx.createSocket(SocketType.ROUTER);
        this.timeoutSeconds = timeoutSeconds;
        socket.bind(serviceBindUrl);
    }

    void process() {
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
                String serviceName = data.getString(ZMQ.CHARSET);
                List<ServiceLocation> serviceLocations = this.serviceStore.query(serviceName);
                ZMsg result = new ZMsg();
                result.add(address);
                if (serviceLocations != null) {
                    for (ServiceLocation serviceLocation : serviceLocations) {
                        result.add(serviceLocation.getLocation());
                    }
                } else {
                    result.add("Not Found");
                }
                result.send(socket);
            }
            case "register" -> {
                ZFrame nameFrame = msg.poll();
                assert (nameFrame != null);
                String serviceName = nameFrame.getString(ZMQ.CHARSET);

                ZFrame locationFrame = msg.poll();
                assert (locationFrame != null);
                String location = locationFrame.getString(ZMQ.CHARSET);
                this.serviceStore.register(serviceName, location);
                System.out.printf("[ServiceLocator] registered %s to %s%n", location, serviceName);
            }
        }
    }

    public List<ServiceLocation> query(String serviceName) {
        List<ServiceLocation> serviceLocations = this.serviceStore.query(serviceName);
        if (serviceLocations.isEmpty()) {
            return Collections.emptyList();
        } else {
            return new ArrayList<>(serviceLocations);
        }
    }

    public void run() {
        while (true) {
            process();
        }
    }
}
