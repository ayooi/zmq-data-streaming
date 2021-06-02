package au.ooi.streams;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class ServiceStore {
    private final Map<String, Set<ServiceLocation>> map = new ConcurrentHashMap<>();

    public void register(String serviceName, String location) {
        Set<ServiceLocation> incoming = new HashSet<>();
        Set<ServiceLocation> existing = map.putIfAbsent(serviceName, incoming);

        Set<ServiceLocation> serviceLocations;
        serviceLocations = Objects.requireNonNullElse(existing, incoming);

        ServiceLocation updated = new ServiceLocation(location, Instant.now());
        if (Objects.requireNonNull(serviceLocations).contains(updated)) {
            serviceLocations.remove(updated);
        }
        serviceLocations.add(updated);
    }

    public List<ServiceLocation> query(String serviceName) {
        Set<ServiceLocation> serviceLocations = this.map.get(serviceName);
        if (serviceLocations == null) {
            return Collections.emptyList();
        } else {
            return new ArrayList<>(serviceLocations);
        }
    }
}
