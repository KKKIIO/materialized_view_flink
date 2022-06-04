package io.github.kkkiio.mview;

import lombok.Value;

@Value
public class ReorderInfo {
    long customerId;
    int orderCount;
    long lastOrderTime;
    long expectedNextOrderTime;
}
