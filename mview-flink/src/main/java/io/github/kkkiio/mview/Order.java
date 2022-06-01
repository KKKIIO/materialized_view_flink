package io.github.kkkiio.mview;

import lombok.Value;

@Value
public class Order {
    long id;
    long customerId;
    long orderTime;
    long createTime;
}
