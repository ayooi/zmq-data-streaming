package au.ooi.streams;

import lombok.Value;

@Value
class ExpiredServiceDetails {
    String serviceName;
    String location;
}
