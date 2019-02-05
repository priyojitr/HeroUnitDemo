package com.stackroute.datamunger;

import java.util.HashMap;
import java.util.Scanner;

import com.stackroute.datamunger.query.Query;
import com.stackroute.datamunger.writer.JsonWriter;

public class DataMunger {

	public static void main(String[] args) {
		// Read the query from the user

		/*
		 * Instantiate Query class. This class is responsible for: 1. Parsing the query
		 * 2. Select the appropriate type of query processor 3. Get the resultSet which
		 * is populated by the Query Processor
		 */

		/*
		 * Instantiate JsonWriter class. This class is responsible for writing the
		 * ResultSet into a JSON file
		 */

		/*
		 * call executeQuery() method of Query class to get the resultSet. Pass this
		 * resultSet as parameter to writeToJson() method of JsonWriter class to write
		 * the resultSet into a JSON file
		 */
		System.out.println("Enter your Query: ");
		Scanner scanner = new Scanner(System.in);
		String queryString = scanner.nextLine();
			
		Query query = new Query();
		HashMap resultMap = query.executeQuery(queryString);
		JsonWriter jsonWriter = new JsonWriter();
		boolean writeStatus = jsonWriter.writeToJson(resultMap);
		if(writeStatus) {
			System.out.println("Resultset has been written into data/result.json successfully");
		} else {
			System.out.println("Error occurred at the time of writting");
		}
	}
}
