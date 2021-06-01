package au.ooi.streams;

import lombok.EqualsAndHashCode;
import lombok.Value;

import java.time.Instant;

@Value
@EqualsAndHashCode(exclude = "lastUpdateTime")
class ServiceLocation {
    String location;
    Instant lastUpdateTime;
}
