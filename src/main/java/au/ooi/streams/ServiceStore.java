package au.ooi.streams;

import lombok.Value;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class ServiceStore implements Runnable {

    @Value
    class ExpiryTracker {
        String serviceName;
        String location;
        Instant expiryTime;
    }

    private final Map<String, Map<String, Instant>> map = new ConcurrentHashMap<>();

    private final ArrayBlockingQueue<ExpiryTracker> queue = new ArrayBlockingQueue<>(10000);

    public void register(String serviceName, String location) {
        Map<String, Instant> incoming = new ConcurrentHashMap<>();
        Map<String, Instant> existing = map.putIfAbsent(serviceName, incoming);

        Map<String, Instant> serviceLocations;
        serviceLocations = Objects.requireNonNullElse(existing, incoming);

        serviceLocations.put(location, Instant.now());
        try {
            queue.put(new ExpiryTracker(serviceName, location, Instant.now().plusSeconds(30)));
        } catch (InterruptedException e) {
            // ignored for now
        }
    }

    public List<ServiceLocation> query(String serviceName) {
        Map<String, Instant> serviceLocations = this.map.get(serviceName);
        if (serviceLocations == null) {
            return Collections.emptyList();
        } else {
            return serviceLocations.entrySet().stream()
                    .map(x -> new ServiceLocation(x.getKey(), x.getValue()))
                    .collect(Collectors.toList());
        }
    }

    @Override
    public void run() {
        while (true) {
            try {
                ExpiryTracker take = queue.take();
                Instant now = Instant.now();
                Duration between = Duration.between(now, take.expiryTime);
                if (between.isNegative()) {
                    Map<String, Instant> serviceLocations = this.map.get(take.getServiceName());
                }
            } catch (InterruptedException e) {
            }
        }
    }
}
