package au.ooi.data;

import lombok.Value;

import java.time.Instant;
import java.util.List;

@Value
public class ServiceLocations {
    List<String> locations;
    Instant lastUpdated;
}
