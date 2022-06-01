package io.github.kkkiio.mview;

import lombok.Builder;
import lombok.Data;

@Data
@Builder(toBuilder = true)
public class ReorderCalcValue {
    private long customerId;
    private int orderCount;
    private long lastOrderTime;
}
