package io.github.kkkiio.mview;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;

import lombok.Value;
import lombok.val;

public class ReorderCalc extends RichMapFunction<Change, ReorderInfo> {
    private static final long serialVersionUID = 1L;
    private transient AggregatingState<Change, ReorderState> reorderState;
    private static final long DAY_IN_MILLIS = 1000L * 60L * 60L * 24L;

    @Value
    private static class ReorderState {
        int orderCount;
        long lastOrderTime;
        int frequency;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        AggregatingStateDescriptor<Change, ReorderState, ReorderState> descriptor = new AggregatingStateDescriptor<>(
                "reorder",
                new AggregateFunction<Change, ReorderState, ReorderState>() {
                    @Override
                    public ReorderState createAccumulator() {
                        return new ReorderState(0, 0, 0);
                    }

                    @Override
                    public ReorderState add(Change value, ReorderState accumulator) {
                        if (value.getOrder() != null) {
                            return new ReorderState(
                                    accumulator.getOrderCount() + (value.isCreate() ? 1 : 0),
                                    Math.max(accumulator.getLastOrderTime(), value.getOrder().getOrderTime()),
                                    accumulator.getFrequency());
                        } else {
                            return new ReorderState(
                                    accumulator.getOrderCount(),
                                    accumulator.getLastOrderTime(),
                                    value.getCustomerPreference().getFrequency());
                        }
                    }

                    @Override
                    public ReorderState getResult(ReorderState accumulator) {
                        return accumulator;
                    }

                    @Override
                    public ReorderState merge(ReorderState a, ReorderState b) {
                        // merge will not be called, see
                        // https://stackoverflow.com/questions/57943469/flink-valuestate-vs-reducingstate-aggregatingstate
                        throw new UnsupportedOperationException();
                    }
                }, TypeInformation.of(ReorderState.class));
        this.reorderState = this.getRuntimeContext()
                .getAggregatingState(descriptor);
    }

    @Override
    public ReorderInfo map(Change value) throws Exception {
        this.reorderState.add(value);
        val state = this.reorderState.get();
        long customerId;
        if (value.getOrder() != null) {
            customerId = value.getOrder().getCustomerId();
        } else {
            customerId = value.getCustomerPreference().getCustomerId();
        }
        long expectedNextOrderTime = 0;
        if (state.getFrequency() > 0 && state.getLastOrderTime() > 0) {
            expectedNextOrderTime = state.getLastOrderTime() + state.getFrequency() * DAY_IN_MILLIS;
        }
        return new ReorderInfo(customerId, state.getOrderCount(), state.getLastOrderTime(), expectedNextOrderTime);
    }
}
