package au.ooi.streams;

import au.ooi.data.ExpiredServiceDetails;
import au.ooi.data.ServiceLocations;
import au.ooi.externals.TimeProvider;
import lombok.Value;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

public class ServiceStore implements Runnable {

    private int timeoutSeconds;
    private TimeProvider timeProvider;

    @Value
    class ExpiryTracker {
        String serviceName;
        String location;
        Instant expiryTime;
    }

    private final Map<String, Map<String, Instant>> map = new ConcurrentHashMap<>();

    private final ArrayBlockingQueue<ExpiryTracker> queue = new ArrayBlockingQueue<>(10000);

    private final ArrayBlockingQueue<ExpiredServiceDetails> expiredEvents = new ArrayBlockingQueue<>(1000);

    public ServiceStore(int timeoutSeconds, TimeProvider timeProvider) {
        this.timeoutSeconds = timeoutSeconds;
        this.timeProvider = timeProvider;
    }

    public boolean register(String serviceName, String location) {
        Map<String, Instant> incoming = new ConcurrentHashMap<>();
        Map<String, Instant> existing = map.putIfAbsent(serviceName, incoming);

        Map<String, Instant> serviceLocations;
        serviceLocations = Objects.requireNonNullElse(existing, incoming);

        Instant existingEntry = serviceLocations.put(location, this.timeProvider.now());
        try {
            queue.put(new ExpiryTracker(serviceName, location, this.timeProvider.now().plusSeconds(this.timeoutSeconds)));
        } catch (InterruptedException e) {
            // ignored for now
        }

        // Allows callers to know if this should result in an update being pushed.
        return existing == null || existingEntry == null;
    }

    public ServiceLocations query(String serviceName) {
        Map<String, Instant> serviceLocations = this.map.get(serviceName);
        if (serviceLocations == null) {
            return new ServiceLocations(Collections.emptyList(), this.timeProvider.now());
        } else {
            Optional<Instant> first = serviceLocations.values()
                    .stream().max(Instant::compareTo)
                    .stream().findFirst();
            if (first.isEmpty()) {
                throw new IllegalStateException("Something exploded");
            }
            return new ServiceLocations(new ArrayList<>(serviceLocations.keySet()), first.get());
        }
    }

    void processTimeout() throws InterruptedException {
        ExpiryTracker take = queue.take();
        Instant now = this.timeProvider.now();
        Duration between = Duration.between(now, take.expiryTime);
        if (!between.isNegative()) {
            this.timeProvider.sleep(between.toMillis());
        }

        Map<String, Instant> serviceLocations = this.map.get(take.getServiceName());
        Instant instant = serviceLocations.get(take.getLocation());
        if (instant != null && instant.isBefore(now)) {
            serviceLocations.remove(take.getLocation());
            this.expiredEvents.put(new ExpiredServiceDetails(take.getServiceName(), take.getLocation()));
        }
    }

    @Override
    public void run() {
        while (true) {
            try {
                processTimeout();
            } catch (InterruptedException e) {
                // ignored
            }
        }
    }

    public boolean hasEvents() {
        return !this.expiredEvents.isEmpty();
    }

    public ExpiredServiceDetails take() throws InterruptedException {
        return this.expiredEvents.take();
    }
}
