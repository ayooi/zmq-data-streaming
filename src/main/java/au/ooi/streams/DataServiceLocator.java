package au.ooi.streams;

import lombok.Value;
import org.zeromq.*;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class DataServiceLocator implements Runnable {

    @Value
    class PendingRequest {
        String serviceRequested;
        String address;
        Instant received;
    }

    private final ZMQ.Socket socket;
    private final TimeProvider timeProvider;
    private final ServiceStore serviceStore;
    private final Map<String, List<PendingRequest>> knownRequesters = new ConcurrentHashMap<>();

    public DataServiceLocator(ZContext ctx, String serviceBindUrl, int timeoutSeconds, TimeProvider timeProvider) {
        socket = ctx.createSocket(SocketType.ROUTER);
        this.timeProvider = timeProvider;
        socket.bind(serviceBindUrl);
        serviceStore = new ServiceStore(timeoutSeconds, timeProvider);
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

                ServiceLocations serviceLocations = null;
                String addressString = address.getString(StandardCharsets.UTF_8);
                List<PendingRequest> put = knownRequesters.putIfAbsent(serviceName, new ArrayList<>());
                boolean shouldSend = false;
                if (put == null) {
                    // first time seeing this requester so we should immediately send back known service endpoints
                    shouldSend = true;
                    knownRequesters.get(serviceName).add(new PendingRequest(serviceName, addressString, timeProvider.now()));
                } else {
                    serviceLocations = serviceStore.query(serviceName);
                    Optional<PendingRequest> pendingRequest = put.stream().filter(x -> x.getAddress().equals(addressString)).findFirst();
                    if (pendingRequest.isPresent()) {
                        if (serviceLocations.getLastUpdated().isAfter(pendingRequest.get().getReceived())) {
                            // if the locations of services has been updated between the last time we received a request
                            // and now, then we should resend the list of locations
                            shouldSend = true;
                        }
                        put.remove(pendingRequest.get());
                        put.add(new PendingRequest(serviceName, addressString, timeProvider.now()));
                    } else {
                        shouldSend = true;
                        knownRequesters.get(serviceName).add(new PendingRequest(serviceName, addressString, timeProvider.now()));
                    }
                }

                if (shouldSend) {
                    if (serviceLocations == null) {
                        serviceLocations = this.serviceStore.query(serviceName);
                    }
                    ZMsg result = new ZMsg();
                    result.add(address);
                    if (serviceLocations != null) {
                        for (String serviceLocation : serviceLocations.getLocations()) {
                            result.add(serviceLocation);
                        }
                    }
                    result.send(socket);
                }
            }
            case "register" -> {
                ZFrame nameFrame = msg.poll();
                assert (nameFrame != null);
                String serviceName = nameFrame.getString(ZMQ.CHARSET);
                ZFrame locationFrame = msg.poll();
                assert (locationFrame != null);
                String location = locationFrame.getString(ZMQ.CHARSET);
                boolean updated = this.serviceStore.register(serviceName, location);
                if (updated) {
                    System.out.printf("[ServiceLocator] registered %s to %s%n", location, serviceName);
                    // we need to send back all pending queries for this locations that satisfy this service name
                    List<PendingRequest> pendingRequests = this.knownRequesters.get(serviceName);
                    if (pendingRequests == null) {
                        return;
                    }

                    ServiceLocations serviceLocations = this.serviceStore.query(serviceName);
                    for (PendingRequest pendingRequest : pendingRequests) {
                        ZMsg result = new ZMsg();
                        result.add(pendingRequest.getAddress());
                        for (String loc : serviceLocations.getLocations()) {
                            result.add(loc);
                        }
                        result.send(this.socket);
                    }
                }
            }
        }
    }

    public List<String> query(String serviceName) {
        ServiceLocations serviceLocations = this.serviceStore.query(serviceName);
        if (serviceLocations.getLocations().isEmpty()) {
            return Collections.emptyList();
        } else {
            return new ArrayList<>(serviceLocations.getLocations());
        }
    }

    public void run() {
        while (true) {
            process();
        }
    }
}
