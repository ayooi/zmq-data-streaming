package au.ooi.streams;

import lombok.Value;

import java.time.Instant;
import java.util.List;

@Value
public class ServiceLocations {
    List<String> locations;
    Instant lastUpdated;
}
