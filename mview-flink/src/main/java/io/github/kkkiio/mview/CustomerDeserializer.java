package io.github.kkkiio.mview;

import com.ververica.cdc.debezium.DebeziumDeserializationSchema;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import lombok.val;

public class CustomerDeserializer implements DebeziumDeserializationSchema<CustomerChange> {
    public static final long serialVersionUID = 1L;

    @Override
    public TypeInformation<CustomerChange> getProducedType() {
        return TypeInformation.of(CustomerChange.class);
    }

    @Override
    public void deserialize(SourceRecord record, Collector<CustomerChange> out) throws Exception {
        val newValue = ((Struct) record.value()).getStruct("after");
        if (newValue != null) {
            out.collect(new CustomerChange(
                    newValue.getInt64("id"),
                    newValue.getString("first_name"),
                    newValue.getString("last_name")));
        }
    }
}
