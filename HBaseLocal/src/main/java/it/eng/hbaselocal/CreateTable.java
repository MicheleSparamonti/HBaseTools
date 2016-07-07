/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
/*
 * Copyright 2016 Michele Sparamonti, Spiros Koulouzis
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package it.eng.hbaselocal;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

/**
 *
 * @author Michele Sparamonti (Michele.Sparamonti@eng.it)
 */
public class CreateTable {
	private static Connection conn;
	public static final TableName tblName = TableName.valueOf("terms");
	public static String[] data_analytics = { "Predictive analytics",
			"Statistical techniques", "Analytics for decision making",
			"Data blending", "Big data analytics platform" };
	public static String[] data_management_curation = { "Data Management Plan",
			"Develop data models", "Data collection & Integration",
			"Data visualization", "Repository of analysis history" };
	public static String[] data_science_engineering = {
			"Engineering principles", "Big data coputational solutions",
			"Analysis tools for decision making",
			"Relational & non-relational databases",
			"Security service management", "Agile development" };
	public static String[] scientific_research_methods = { "Scientific method",
			"Systematic study", "Devise new applications",
			"Develop innovative ideas", "Strategies into action plans",
			"Contribute research objectives" };
	public static String[] domain_knowledge = { "Business process",
			"Improve existing services", "Participate financial decisions",
			"Analytic support to other organisation",
			"Analyse data for marketing", "Analyse customer data" };

	public static void main(String[] args) throws IOException {
		Set<String> values = new HashSet<String>();
		 Configuration hBaseConfig =  HBaseConfiguration.create();
		    conn = ConnectionFactory.createConnection(hBaseConfig);
		List<String> families = new ArrayList<>();
		families.add("data analytics");
		families.add("data management/curation");
		families.add("data science engineering");
		families.add("scientific/research methods");
		families.add("domain knowledge");
		createTable(tblName, families);
		try (Admin admin = getConn().getAdmin()) {
			try (Table tbl = getConn().getTable(tblName)) {
				for (String t : values) {
					Put put = new Put(Bytes.toBytes(t));
					int count = 0;
					Double distances = Double.valueOf(t);
					for (int i = 0; i < data_analytics.length; i++) {
						put.addColumn(Bytes.toBytes("data analytics"),
								Bytes.toBytes(data_analytics[i]),
								Bytes.toBytes(distances));
						count++;
					}
					/*for (int i = 0; i < data_management_curation.length; i++) {
						put.addColumn(
								Bytes.toBytes("data management/curation"),
								Bytes.toBytes(data_management_curation[i]),
								Bytes.toBytes(distances.remove(count)));
						count++;
					}
					for (int i = 0; i < data_science_engineering.length; i++) {
						put.addColumn(
								Bytes.toBytes("data science engineering"),
								Bytes.toBytes(data_science_engineering[i]),
								Bytes.toBytes(distances.remove(count)));
						count++;
					}
					for (int i = 0; i < scientific_research_methods.length; i++) {
						put.addColumn(
								Bytes.toBytes("scientific/research methods"),
								Bytes.toBytes(scientific_research_methods[i]),
								Bytes.toBytes(distances.remove(count)));
						count++;
					}
					for (int i = 0; i < domain_knowledge.length; i++) {
						put.addColumn(Bytes.toBytes("domain knowledge"),
								Bytes.toBytes(domain_knowledge[i]),
								Bytes.toBytes(distances.remove(count)));
						count++;
					}
*/
					tbl.put(put);
				}
			}
			admin.flush(tblName);
		}

	}

	public static Connection getConn() {
		return conn;
	}

	protected static void createTable(TableName tblName, List<String> families)
			throws IOException {
		try (Admin admin = getConn().getAdmin()) {
			if (!admin.tableExists(tblName)) {
				HTableDescriptor tableDescriptor = new HTableDescriptor(tblName);
				for (String f : families) {
					HColumnDescriptor desc = new HColumnDescriptor(f);
					tableDescriptor.addFamily(desc);
				}
				admin.createTable(tableDescriptor);
			}
		}

	}
}
