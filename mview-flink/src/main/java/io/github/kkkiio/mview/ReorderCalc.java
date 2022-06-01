package io.github.kkkiio.mview;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;

import lombok.val;

public class ReorderCalc extends RichMapFunction<Change, ReorderCalcValue> {
    private static final long serialVersionUID = 1L;
    private transient AggregatingState<Change, ReorderCalcValue> reorderState;

    @Override
    public void open(Configuration parameters) throws Exception {
        val descriptor = new AggregatingStateDescriptor<Change, ReorderCalcValue, ReorderCalcValue>("reorder",
                new AggregateFunction<Change, ReorderCalcValue, ReorderCalcValue>() {

                    @Override
                    public ReorderCalcValue createAccumulator() {
                        return ReorderCalcValue.builder().build();
                    }

                    @Override
                    public ReorderCalcValue add(Change value, ReorderCalcValue accumulator) {
                        accumulator.setCustomerId(value.getOrder().getCustomerId());
                        accumulator.setLastOrderTime(
                                Math.max(accumulator.getLastOrderTime(),
                                        value.getOrder().getOrderTime()));
                        accumulator.setOrderCount(accumulator.getOrderCount() + 1);
                        return accumulator;
                    }

                    @Override
                    public ReorderCalcValue getResult(ReorderCalcValue accumulator) {
                        return accumulator.toBuilder().build();
                    }

                    @Override
                    public ReorderCalcValue merge(ReorderCalcValue a, ReorderCalcValue b) {
                        // merge will not be called, see
                        // https://stackoverflow.com/questions/57943469/flink-valuestate-vs-reducingstate-aggregatingstate
                        return a.toBuilder().orderCount(a.getOrderCount() + b.getOrderCount())
                                .lastOrderTime(Math.max(a.getLastOrderTime(), b.getLastOrderTime()))
                                .build();
                    }
                }, TypeInformation.of(ReorderCalcValue.class));
        this.reorderState = this.getRuntimeContext()
                .getAggregatingState(descriptor);
    }

    @Override
    public ReorderCalcValue map(Change value) throws Exception {
        this.reorderState.add(value);
        return this.reorderState.get();
    }
}
