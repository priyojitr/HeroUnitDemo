package com.stackroute.datamunger.reader;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.stackroute.datamunger.query.DataSet;
import com.stackroute.datamunger.query.DataTypeDefinitions;
import com.stackroute.datamunger.query.Filter;
import com.stackroute.datamunger.query.GenericComparator;
import com.stackroute.datamunger.query.Row;
import com.stackroute.datamunger.query.RowDataTypeDefinitions;
import com.stackroute.datamunger.query.parser.QueryParameter;
import com.stackroute.datamunger.query.parser.Restriction;

//This class will read from CSV file and process and return the resultSet
public class CsvQueryProcessor implements QueryProcessingEngine {
	/*
	 * This method will take QueryParameter object as a parameter which contains
	 * the parsed query and will process and populate the ResultSet
	 */
	public DataSet getResultSet(QueryParameter queryParameter) {

		/*
		 * initialize BufferedReader to read from the file which is mentioned in
		 * QueryParameter. Consider Handling Exception related to file reading.
		 */

		/*
		 * read the first line which contains the header. Please note that the
		 * headers can contain spaces in between them. For eg: city, winner
		 */
		BufferedReader br = null;
		String line = null;
		String csvSplitBy = ",";
		String[] headers = null;
		DataSet dataSet = new DataSet();
		DataSet finalDataSet = new DataSet();
		try {
			try {

				br = new BufferedReader(new FileReader(
						queryParameter.getFileName()));
				while ((line = br.readLine()) != null) {

					headers = line.split(csvSplitBy);
					break;
				}

			} catch (FileNotFoundException e) {
				System.out.println("File Not Found Exception occurred");
			} catch (IOException e) {
				System.out.println("IO Exception occurred");
			}

			/*
			 * read the next line which contains the first row of data. We are
			 * reading this line so that we can determine the data types of all
			 * the fields. Please note that ipl.csv file contains null value in
			 * the last column. If you do not consider this while splitting,
			 * this might cause exceptions later
			 */
			int count = 0;

			DataTypeDefinitions dataTypeDef = null;
			String[] dataValues = null;
			// String[] dataTypes = null;
			// List<String> dataTypeList = null;
			try {
				br = new BufferedReader(new FileReader(
						queryParameter.getFileName()));
				while ((line = br.readLine()) != null) {
					if (count == 1) {
						dataValues = line.split(csvSplitBy, -1);
						break;
					}
					count++;
				}
			} catch (FileNotFoundException e) {
				System.out.println("File Not Found Exception occurred");
			} catch (IOException e) {
				System.out.println("IO Exception occurred");
			}
			/*
			 * populate the header Map object from the header array. header map
			 * is having data type <String,Integer> to contain the header and
			 * it's index.
			 */
			HashMap<String, Integer> headerMap = new HashMap<String, Integer>();
			int rowIndex = 0;
			for (String headerName : headers) {
				headerMap.put(headerName, rowIndex);
				rowIndex++;
			}

			/*
			 * We have read the first line of text already and kept it in an
			 * array. Now, we can populate the dataTypeDefinition Map object.
			 * dataTypeDefinition map is having data type <Integer,String> to
			 * contain the index of the field and it's data type. To find the
			 * dataType by the field value, we will use getDataType() method of
			 * DataTypeDefinitions class
			 */

			RowDataTypeDefinitions rowDataTypeDef = new RowDataTypeDefinitions();
			int dataIndex = 0;
			for (String dataValue : dataValues) {
				String dataType = (String) DataTypeDefinitions
						.getDataType(dataValue);
				rowDataTypeDef.put(dataIndex, dataType);
				dataIndex++;
			}

			/*
			 * once we have the header and dataTypeDefinitions maps populated,
			 * we can start reading from the first line. We will read one line
			 * at a time, then check whether the field values satisfy the
			 * conditions mentioned in the query,if yes, then we will add it to
			 * the resultSet. Otherwise, we will continue to read the next line.
			 * We will continue this till we have read till the last line of the
			 * CSV file.
			 */

			/*
			 * reset the buffered reader so that it can start reading from the
			 * first line
			 */

			/*
			 * skip the first line as it is already read earlier which contained
			 * the header
			 */

			/*
			 * read one line at a time from the CSV file till we have any lines
			 * left
			 */

			/*
			 * once we have read one line, we will split it into a String Array.
			 * This array will continue all the fields of the row. Please note
			 * that fields might contain spaces in between. Also, few fields
			 * might be empty.
			 */

			/*
			 * if there are where condition(s) in the query, test the row fields
			 * against those conditions to check whether the selected row
			 * satisfies the conditions
			 */

			/*
			 * from QueryParameter object, read one condition at a time and
			 * evaluate the same. For evaluating the conditions, we will use
			 * evaluateExpressions() method of Filter class. Please note that
			 * evaluation of expression will be done differently based on the
			 * data type of the field. In case the query is having multiple
			 * conditions, you need to evaluate the overall expression i.e. if
			 * we have OR operator between two conditions, then the row will be
			 * selected if any of the condition is satisfied. However, in case
			 * of AND operator, the row will be selected only if both of them
			 * are satisfied.
			 */

			/*
			 * check for multiple conditions in where clause for eg: where
			 * salary>20000 and city=Bangalore for eg: where salary>20000 or
			 * city=Bangalore and dept!=Sales
			 */

			/*
			 * if the overall condition expression evaluates to true, then we
			 * need to check if all columns are to be selected(select *) or few
			 * columns are to be selected(select col1,col2). In either of the
			 * cases, we will have to populate the row map object. Row Map
			 * object is having type <String,String> to contain field name and
			 * field value for the selected fields. Once the row object is
			 * populated, add it to DataSet Map Object. DataSet Map object is
			 * having type <Long,Row> to hold the rowId (to be manually
			 * generated by incrementing a Long variable) and it's corresponding
			 * Row Object.
			 */

			/*
			 * check for the existence of Order By clause in the Query Parameter
			 * Object. if it contains an Order By clause, implement sorting of
			 * the dataSet
			 */

			/* return dataset object */

			int rowCount = 1;
			String[] rowData = null;
			Long rowDataCount = new Long(1);
			boolean filterStatus = false;
			Row row = null;
			try {
				br = new BufferedReader(new FileReader(
						queryParameter.getFileName()));
				while ((line = br.readLine()) != null) {
					if (rowCount == 1) {
						rowCount++;
						continue;
					} else {
						rowData = line.split(csvSplitBy, -1);
						row = new Row();

						if (queryParameter.getRestrictions() != null
								&& !queryParameter.getRestrictions().isEmpty()) {

							if (queryParameter.getRestrictions().size() == 1) {
								Restriction restriction = queryParameter
										.getRestrictions().get(0);
								filterStatus = Filter.evaluateExpression(
										restriction, rowData[headerMap
												.get(restriction
														.getPropertyName())],
										rowDataTypeDef.get(headerMap
												.get(restriction
														.getPropertyName())));
								if (filterStatus) {
									row = getRowWithSelectedFields(
											queryParameter, headerMap, rowData);
									dataSet.put(rowDataCount, row);
									rowDataCount++;
								}

							} else {

								List<Restriction> restrictionList = queryParameter
										.getRestrictions();
								List<String> logicalOperatorList = queryParameter
										.getLogicalOperators();
								List<Boolean> evaluationList = new ArrayList<Boolean>();
								for (Restriction restriction : restrictionList) {
									boolean status = Filter
											.evaluateExpression(
													restriction,
													rowData[headerMap.get(restriction
															.getPropertyName())],
													rowDataTypeDef.get(headerMap.get(restriction
															.getPropertyName())));
									evaluationList.add(status);
								}

								if (logicalOperatorList != null
										&& !logicalOperatorList.isEmpty()) {
									int counter = evaluationList.size() - 1;
									boolean overAllCondition = validateAllWhereConditions(
											logicalOperatorList,
											evaluationList, counter);
									;

									if (overAllCondition) {
										row = getRowWithSelectedFields(
												queryParameter, headerMap,
												rowData);
										dataSet.put(rowDataCount, row);
										rowDataCount++;

									}
								}
							}
						} else {
							row = getRowWithSelectedFields(queryParameter,
									headerMap, rowData);
							dataSet.put(rowDataCount, row);
							rowDataCount++;
						}

						rowCount++;
					}

				}
				finalDataSet = getFinalDataSet(queryParameter, dataSet);

			} catch (FileNotFoundException e) {
				System.out.println("File Not Found Exception occurred");
			} catch (IOException e) {
				System.out.println("IO Exception occurred");
			}
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
				}
			}
		}
		return finalDataSet;
	}

	private DataSet getFinalDataSet(QueryParameter queryParameter,
			DataSet dataSet) {
		DataSet finalDataSet = new DataSet();
		Long recordCounter = new Long(1);
		if (dataSet != null && !dataSet.isEmpty()) {
			List<Row> rowList = new ArrayList<Row>();
			for (Long key : dataSet.keySet()) {
				Row row = dataSet.get(key);
				rowList.add(row);
			}

			if (queryParameter.getOrderByFields() != null
					&& !queryParameter.getOrderByFields().isEmpty()) {
				for (String orderByField : queryParameter.getOrderByFields()) {
					Collections.sort(rowList, new GenericComparator(
							orderByField));
				}
				for (Row row : rowList) {
					finalDataSet.put(recordCounter, row);
					recordCounter++;
				}
			} else {
				for (Row row : rowList) {
					finalDataSet.put(recordCounter, row);
					recordCounter++;
				}
			}
		}
		return finalDataSet;
	}

	private Row getRowWithSelectedFields(QueryParameter queryParameter,
			HashMap<String, Integer> headerMap, String[] rowData) {
		Row row = new Row();
		if ("*".equals(queryParameter.getFields().get(0))) {
			for (String fieldName : headerMap.keySet()) {
				row.put(fieldName, rowData[headerMap.get(fieldName)]);
			}
		} else if (queryParameter.getFields() != null
				&& !queryParameter.getFields().isEmpty()) {
			for (String fieldName : queryParameter.getFields()) {
				String rowDataVal = rowData[headerMap.get(fieldName)];
				row.put(fieldName, rowDataVal);

			}
		}
		return row;
	}

	private boolean validateAllWhereConditions(
			List<String> logicalOperatorList, List<Boolean> evaluationList,
			int counter) {
		boolean temp = false;
		for (int i = logicalOperatorList.size() - 1; i >= 0; i--) {
			String logicalOperator = logicalOperatorList.get(i);
			if (counter == evaluationList.size() - 1) {
				if (logicalOperator.equalsIgnoreCase("AND")) {
					if (evaluationList.get(counter)
							&& evaluationList.get(counter - 1)) {
						temp = true;
					} else {
						temp = false;
					}
				} else if (logicalOperator.equalsIgnoreCase("OR")) {
					if (evaluationList.get(counter)
							|| evaluationList.get(counter - 1)) {
						temp = true;
					} else {
						temp = false;
					}
				}
			} else {
				if (logicalOperator.equalsIgnoreCase("AND")) {
					if (temp && evaluationList.get(counter - 1)) {
						temp = true;
					} else {
						temp = false;
					}
				} else if (logicalOperator.equalsIgnoreCase("OR")) {
					if (temp || evaluationList.get(counter - 1)) {
						temp = true;
					} else {
						temp = false;
					}
				}
			}
			counter--;
		}
		return temp;
	}

}
