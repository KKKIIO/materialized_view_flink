package io.github.kkkiio.mview;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import lombok.val;

public class FlinkJob {

	public static void main(String[] args) throws Exception {
		val mysqlHost = "localhost";
		val mysqlPort = 3306;
		val mysqlDb = "demo";
		val mysqlUser = "root";
		val mysqlPassword = "123456";

		val env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.enableCheckpointing(3000);

		val mySqlSource = MySqlSource.<Change>builder()
				.hostname(mysqlHost)
				.port(mysqlPort)
				.databaseList(mysqlDb)
				.tableList("demo.customer_tab", "demo.order_tab")
				.username(mysqlUser)
				.password(mysqlPassword)
				.deserializer(new ChangeDeserializer())
				.build();

		val src = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Customer Source");
		val customerTag = new OutputTag<Customer>("customer") {
		};
		val mainStream = src.process(new ProcessFunction<Change, Change>() {
			private static final long serialVersionUID = 1L;

			@Override
			public void processElement(Change value, ProcessFunction<Change, Change>.Context ctx,
					Collector<Change> out)
					throws Exception {
				if (value.getCustomer() != null) {
					ctx.output(customerTag, value.getCustomer());
				} else {
					out.collect(value);
				}
			}
		});
		val jdbcConnOpts = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
				.withUrl(String.format("jdbc:mysql://%s:%d/%s", mysqlHost, mysqlPort, mysqlDb))
				.withDriverName("com.mysql.cj.jdbc.Driver")
				.withUsername(mysqlUser)
				.withPassword(mysqlPassword)
				.build();
		mainStream.getSideOutput(customerTag).addSink(JdbcSink.sink(
						"INSERT INTO customer_reorder_tab (customer_id, first_name, last_name) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE first_name = ?, last_name = ?",
						(statement, customer) -> {
							statement.setLong(1, customer.getId());
							statement.setString(2, customer.getFirstName());
							statement.setString(3, customer.getLastName());
							statement.setString(4, customer.getFirstName());
							statement.setString(5, customer.getLastName());
						},
				JdbcExecutionOptions.builder().withBatchIntervalMs(200).build(), jdbcConnOpts))
				.name("MySQL Customer Sink")
				.setParallelism(1);
		mainStream.keyBy(c -> {
			if (c.getOrder() != null) {
				return c.getOrder().getCustomerId();
			} else {
				throw new IllegalArgumentException(String.format("Unknown change type: %s", c));
			}
		}).map(new ReorderCalc()).name("Calculate reorder info").addSink(
				JdbcSink.sink(
						"INSERT INTO customer_reorder_tab (customer_id, order_count, last_order_time) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE order_count = ?, last_order_time = ?",
						(statement, reorder) -> {
							statement.setLong(1, reorder.getCustomerId());
							statement.setInt(2, reorder.getOrderCount());
							statement.setLong(3, reorder.getLastOrderTime());
							statement.setInt(4, reorder.getOrderCount());
							statement.setLong(5, reorder.getLastOrderTime());
						},
						JdbcExecutionOptions.builder().withBatchIntervalMs(200).build(), jdbcConnOpts))
				.name("MySQL Customer Reorder Sink");
		env.execute("Sync Reorder");
	}
}
