package io.github.kkkiio.mview;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

import lombok.val;

public class FlinkSqlJob {

	public static void main(String[] args) throws Exception {
		val mysqlHost = "localhost";
		val mysqlPort = 3306;
		val mysqlDb = "demo";
		val mysqlUser = "root";
		val mysqlPassword = "123456";
		TableEnvironment env = TableEnvironment.create(EnvironmentSettings.newInstance().inStreamingMode().build());
		env.getConfig().getConfiguration().setString("execution.checkpointing.interval", "3s");

		val cdcOptSQL = String.format(
				"WITH ('connector'='mysql-cdc', 'hostname'='%s', 'port'='%d','username'='%s', 'password'='%s', 'database-name'='%s', 'table-name'='%%s')",
				mysqlHost, mysqlPort, mysqlUser, mysqlPassword, mysqlDb);
		env.executeSql(
				"CREATE TEMPORARY TABLE customer_tab (id BIGINT, first_name STRING, last_name STRING, PRIMARY KEY(id) NOT ENFORCED) "
						+ String.format(cdcOptSQL, "customer_tab"));
		env.executeSql(
				"CREATE TEMPORARY TABLE order_tab (id BIGINT, customer_id BIGINT, order_time BIGINT, create_time BIGINT, PRIMARY KEY(id) NOT ENFORCED) "
						+ String.format(cdcOptSQL, "order_tab"));
		env.executeSql(
				"CREATE TEMPORARY TABLE customer_preference_tab (customer_id BIGINT, frequency INT, PRIMARY KEY(customer_id) NOT ENFORCED) "
						+ String.format(cdcOptSQL, "customer_preference_tab"));
		env.executeSql(
				"CREATE TEMPORARY TABLE customer_reorder_tab (customer_id BIGINT, first_name STRING, last_name STRING, order_count INT, last_order_time BIGINT, expected_next_order_time BIGINT, PRIMARY KEY(customer_id) NOT ENFORCED) "
						+ String.format(
								"WITH ('connector'='jdbc', 'url'='jdbc:mysql://%s:%d/%s', 'table-name' = 'customer_reorder_tab', 'username' = '%s', 'password' = '%s')",
								mysqlHost, mysqlPort, mysqlDb, mysqlUser, mysqlPassword));

		env.executeSql(
				"INSERT INTO customer_reorder_tab \n" +
						"SELECT c.id, FIRST_VALUE(c.first_name), FIRST_VALUE(c.last_name), CAST(COUNT(o.id) AS INT)\n" +
						", IFNULL(MAX(o.order_time),0)\n" +
						", IFNULL(MAX(o.order_time) + NULLIF(FIRST_VALUE(cp.frequency),0) * 24 * 3600000, 0) \n" +
						"FROM customer_tab AS c \n" +
						"LEFT OUTER JOIN order_tab AS o ON c.id = o.customer_id \n" +
						"LEFT OUTER JOIN customer_preference_tab AS cp ON c.id = cp.customer_id \n" +
						"GROUP BY c.id");
	}
}
