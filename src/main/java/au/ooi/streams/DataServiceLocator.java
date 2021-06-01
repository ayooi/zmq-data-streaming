package au.ooi.streams;

import org.zeromq.*;

import java.time.Instant;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class DataServiceLocator {

    private static final String IPC_SERVICE_LOCATOR = "ipc://./service-locator-ipc";

    private final ZMQ.Socket socket;

    private final Map<String, Set<ServiceLocation>> map = new ConcurrentHashMap<>();

    public DataServiceLocator(ZContext ctx) {
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
                    Set<ServiceLocation> serviceLocations = map.get(data.getString(ZMQ.CHARSET));
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
                    String name = nameFrame.getString(ZMQ.CHARSET);

                    ZFrame locationFrame = msg.poll();
                    assert (locationFrame != null);
                    String location = locationFrame.getString(ZMQ.CHARSET);
                    this.map.putIfAbsent(name, new HashSet<>());
                    Set<ServiceLocation> serviceLocations = this.map.get(name);
                    if (serviceLocations != null) {
                        serviceLocations.add(new ServiceLocation(location, Instant.now()));
                        System.out.printf("[ServiceLocator] registered %s to %s%n", location, name);
                    }
                }
            }
        }
    }
}
