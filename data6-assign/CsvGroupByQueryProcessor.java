package com.stackroute.datamunger.reader;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

import com.stackroute.datamunger.query.DataSet;
import com.stackroute.datamunger.query.DataTypeDefinitions;
import com.stackroute.datamunger.query.Filter;
import com.stackroute.datamunger.query.GroupedDataSet;
import com.stackroute.datamunger.query.Row;
import com.stackroute.datamunger.query.RowDataTypeDefinitions;
import com.stackroute.datamunger.query.parser.QueryParameter;
import com.stackroute.datamunger.query.parser.Restriction;

/* This is the CsvGroupByQueryProcessor class used for evaluating queries without 
 * aggregate functions but with group by clause*/
@SuppressWarnings("rawtypes")
public class CsvGroupByQueryProcessor implements QueryProcessingEngine {
	/*
	 * This method will take QueryParameter object as a parameter which contains the
	 * parsed query and will process and populate the ResultSet
	 */
	public HashMap getResultSet(QueryParameter queryParameter) {

		/*
		 * initialize BufferedReader to read from the file which is mentioned in
		 * QueryParameter. Consider Handling Exception related to file reading.
		 */

		/*
		 * read the first line which contains the header. Please note that the headers
		 * can contain spaces in between them. For eg: city, winner
		 */

		BufferedReader br = null;
		String line = null;
		String csvSplitBy = ",";
		String[] headers = null;
		DataSet dataSet = new DataSet();
		GroupedDataSet groupedDataSet = new GroupedDataSet();
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
		 * read the next line which contains the first row of data. We are reading this
		 * line so that we can determine the data types of all the fields. Please note
		 * that ipl.csv file contains null value in the last column. If you do not
		 * consider this while splitting, this might cause exceptions later
		 */
			int count = 0;

			//DataTypeDefinitions dataTypeDef = null;
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
		 * populate the header Map object from the header array. header map is having
		 * data type <String,Integer> to contain the header and it's index.
		 */
			HashMap<String, Integer> headerMap = new HashMap<String, Integer>();
			int rowIndex = 0;
			for (String headerName : headers) {
				headerMap.put(headerName, rowIndex);
				rowIndex++;
			}

		/*
		 * We have read the first line of text already and kept it in an array. Now, we
		 * can populate the dataTypeDefinition Map object. dataTypeDefinition map is
		 * having data type <Integer,String> to contain the index of the field and it's
		 * data type. To find the dataType by the field value, we will use getDataType()
		 * method of DataTypeDefinitions class
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
		 * once we have the header and dataTypeDefinitions maps populated, we can start
		 * reading from the first line. We will read one line at a time, then check
		 * whether the field values satisfy the conditions mentioned in the query,if
		 * yes, then we will add it to the resultSet. Otherwise, we will continue to
		 * read the next line. We will continue this till we have read till the last
		 * line of the CSV file.
		 */

		/* reset the buffered reader so that it can start reading from the first line */

		/*
		 * skip the first line as it is already read earlier which contained the header
		 */

		/* read one line at a time from the CSV file till we have any lines left */

		/*
		 * once we have read one line, we will split it into a String Array. This array
		 * will continue all the fields of the row. Please note that fields might
		 * contain spaces in between. Also, few fields might be empty.
		 */

		/*
		 * if there are where condition(s) in the query, test the row fields against
		 * those conditions to check whether the selected row satifies the conditions
		 */

		/*
		 * from QueryParameter object, read one condition at a time and evaluate the
		 * same. For evaluating the conditions, we will use evaluateExpressions() method
		 * of Filter class. Please note that evaluation of expression will be done
		 * differently based on the data type of the field. In case the query is having
		 * multiple conditions, you need to evaluate the overall expression i.e. if we
		 * have OR operator between two conditions, then the row will be selected if any
		 * of the condition is satisfied. However, in case of AND operator, the row will
		 * be selected only if both of them are satisfied.
		 */

		/*
		 * check for multiple conditions in where clause for eg: where salary>20000 and
		 * city=Bangalore for eg: where salary>20000 or city=Bangalore and dept!=Sales
		 */

		/*
		 * if the overall condition expression evaluates to true, then we need to check
		 * for the existence for group by clause in the Query Parameter. The dataSet
		 * generated after processing a group by clause is completely different from a
		 * dataSet structure(which contains multiple rows of data). In case of queries
		 * containing group by clause, the resultSet will contain multiple dataSets,
		 * each of which will be assigned to the group by column value i.e. for all
		 * unique values of the group by column, there can be multiple rows associated
		 * with it. Hence, we will use GroupedDataSet<String,Object> to store the same
		 * and not DataSet<Long,Row>. Please note we will process queries containing one
		 * group by column only for this example.
		 */

		// return groupedDataSet object
		

			//int rowCount = 1;
			String[] rowData = null;
			Long rowCount = new Long(0);
			boolean filterStatus = false;
			Row row = null;
			try {
				br = new BufferedReader(new FileReader(
						queryParameter.getFileName()));
				while ((line = br.readLine()) != null) {
					if (rowCount == 0) {
						rowCount++;
						continue;
					} else {
						rowData = line.split(csvSplitBy, -1);

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
											queryParameter, headerMap,
											rowData);
									dataSet.put(rowCount, row);
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
									int counter = 0;
									boolean temp = false;
									boolean overAllCondition = validateAllWhereConditions(
											logicalOperatorList,
											evaluationList, counter, temp);
									if (overAllCondition) {
										row = getRowWithSelectedFields(
												queryParameter, headerMap,
												rowData);
										dataSet.put(rowCount, row);									
									}

								}

							}

						} else {
							
							row = getRowWithSelectedFields(
									queryParameter, headerMap,
									rowData);
							dataSet.put(rowCount, row);
						}
						rowCount++;
					}
				}
				
				groupedDataSet = getGroupedDataSetWithGroupBy(queryParameter, dataSet);
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
		return groupedDataSet;
			}
	private GroupedDataSet getGroupedDataSetWithGroupBy(QueryParameter queryParameter,
			DataSet dataSet) {
		 GroupedDataSet groupedDataSet = new GroupedDataSet();
		if(dataSet != null && !dataSet.isEmpty()) {
			List<String> groupByFields = queryParameter.getGroupByFields();
			if(groupByFields != null && !groupByFields.isEmpty()) {
				for (String groupByField : groupByFields) {
					for (Entry<Long, Row> entry : dataSet.entrySet()) {
						Long key = entry.getKey();
						Row value = entry.getValue();
						DataSet groupedDataSetForRows = null;
						if (groupedDataSet == null
								|| groupedDataSet.isEmpty()) {
							groupedDataSetForRows = new DataSet();
							groupedDataSetForRows.put(key, value);
						} else if (value.get(groupByField) != null
								&& groupedDataSet.containsKey(value
										.get(groupByField))) {
							groupedDataSetForRows = (DataSet) groupedDataSet
									.get(value.get(groupByField));
							groupedDataSetForRows.put(key, value);
						} else if (value.get(groupByField) != null
								&& !groupedDataSet.containsKey(value
										.get(groupByField))) {
							groupedDataSetForRows = new DataSet();
							groupedDataSetForRows.put(key, value);
						}
						groupedDataSet.put(value.get(groupByField),
								groupedDataSetForRows);
					}

				}

				}
			}
		return groupedDataSet;
	}
	private boolean validateAllWhereConditions(
			List<String> logicalOperatorList, List<Boolean> evaluationList,
			int counter, boolean temp) {
		for (int i = 0; i < logicalOperatorList.size(); i++) {
			String logicalOperator = logicalOperatorList
					.get(i);
			if (counter == 0) {
				if (logicalOperator
						.equalsIgnoreCase("AND")) {
					if (evaluationList.get(counter)
							&& evaluationList
									.get(counter + 1)) {
						temp = true;
					} else {
						temp = false;
					}
				} else if (logicalOperator
						.equalsIgnoreCase("OR")) {
					if (evaluationList.get(counter)
							|| evaluationList
									.get(counter + 1)) {
						temp = true;
					} else {
						temp = false;
					}
				}
			} else {
				if (logicalOperator
						.equalsIgnoreCase("AND")) {
					if (temp
							&& evaluationList
									.get(counter + 1)) {
						temp = true;
					} else {
						temp = false;
					}
				} else if (logicalOperator
						.equalsIgnoreCase("OR")) {
					if (temp
							|| evaluationList
									.get(counter + 1)) {
						temp = true;
					} else {
						temp = false;
					}
				}
			}
			counter++;
			
		}
		return temp;
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
	}


