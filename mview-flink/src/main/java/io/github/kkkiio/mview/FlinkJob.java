/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.github.kkkiio.mview;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import lombok.val;

/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>
 * For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the
 * <a href="https://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>
 * To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>
 * If you change the name of the main class (with the public static void
 * main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for
 * 'mainClass').
 */
public class FlinkJob {

	public static void main(String[] args) throws Exception {
		/*
		 * Here, you can start creating your execution plan for Flink.
		 *
		 * Start with getting some data from the environment, like
		 * env.readTextFile(textPath);
		 *
		 * then, transform the resulting DataStream<String> using operations
		 * like
		 * .filter()
		 * .flatMap()
		 * .join()
		 * .coGroup()
		 *
		 * and many more.
		 * Have a look at the programming guide for the Java API:
		 *
		 * https://flink.apache.org/docs/latest/apis/streaming/index.html
		 *
		 */

		val mysqlHost = "localhost";
		val mysqlPort = 3306;
		val mysqlDb = "demo";
		val mysqlUser = "root";
		val mysqlPassword = "123456";

		val env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.enableCheckpointing(3000);

		val mySqlSource = MySqlSource.<CustomerChange>builder()
				.hostname(mysqlHost)
				.port(mysqlPort)
				.databaseList(mysqlDb)
				.tableList("demo.customer_tab")
				.username(mysqlUser)
				.password(mysqlPassword)
				.deserializer(new CustomerDeserializer())
				.build();

		val src = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Customer Source")
				.setParallelism(1);
		src.addSink(JdbcSink.sink(
						"INSERT INTO customer_reorder_tab (customer_id, first_name, last_name) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE first_name = ?, last_name = ?",
						(statement, customer) -> {
							statement.setLong(1, customer.getId());
							statement.setString(2, customer.getFirstName());
							statement.setString(3, customer.getLastName());
							statement.setString(4, customer.getFirstName());
							statement.setString(5, customer.getLastName());
						},
						JdbcExecutionOptions.builder()
								.withBatchSize(1000)
								.withBatchIntervalMs(200)
								.withMaxRetries(5)
								.build(),
						new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
						.withUrl(String.format("jdbc:mysql://%s:%d/%s", mysqlHost, mysqlPort, mysqlDb))
						.withDriverName("com.mysql.cj.jdbc.Driver")
						.withUsername(mysqlUser)
						.withPassword(mysqlPassword)
								.build()))
				.name("MySQL Customer Sink")
				.setParallelism(1);

		env.execute("Print MySQL Snapshot + Binlog");
	}
}
