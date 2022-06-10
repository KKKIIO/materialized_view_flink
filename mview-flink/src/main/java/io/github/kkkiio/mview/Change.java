package io.github.kkkiio.mview;

import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class Change {
    Customer customer;
    Order order;
    CustomerPreference customerPreference;
    boolean create;
}
