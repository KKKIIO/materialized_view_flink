package io.github.kkkiio.mview;

import com.ververica.cdc.debezium.DebeziumDeserializationSchema;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;

import lombok.val;

public class ChangeDeserializer implements DebeziumDeserializationSchema<Change> {
    private static final long serialVersionUID = 1L;

    @Override
    public TypeInformation<Change> getProducedType() {
        return TypeInformation.of(Change.class);
    }

    @Override
    public void deserialize(SourceRecord record, Collector<Change> out) throws Exception {
        val payload = ((Struct) record.value());
        val table = payload.getStruct("source").getString("table");
        val newValue = payload.getStruct("after");
        if (newValue == null) {
            return;
        }
        Change change;
        try {
            switch (table) {
                case "customer_tab":
                    change = new Change(new Customer(newValue.getInt64("id"), newValue.getString("first_name"),
                            newValue.getString("last_name")), null, null);
                    break;
                case "order_tab":
                    change = new Change(null, new Order(newValue.getInt64("id"), newValue.getInt64("customer_id"),
                            newValue.getInt64("order_time"), newValue.getInt64("create_time")), null);
                    break;
                case "customer_preference_tab":
                    change = new Change(null, null, new CustomerPreference(newValue.getInt64("customer_id"),
                            newValue.getInt32("frequency")));
                    break;
                default:
                    throw new IllegalArgumentException(String.format("Unknown table %s, payload: %s", table, payload));

            }
        } catch (DataException e) {
            throw new DataException(String.format("Failed to deserialize payload: %s", payload), e);
        }
        out.collect(change);
    }
}
