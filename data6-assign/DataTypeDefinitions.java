package com.stackroute.datamunger.query;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

/*
 * Implementation of DataTypeDefinitions class. This class contains a method getDataTypes() 
 * which will contain the logic for getting the datatype for a given field value. This
 * method will be called from QueryProcessors.   
 * In this assignment, we are going to use Regular Expression to find the 
 * appropriate data type of a field. 
 * Integers: should contain only digits without decimal point 
 * Double: should contain digits as well as decimal point 
 * Date: Dates can be written in many formats in the CSV file. 
 * However, in this assignment,we will test for the following date formats('dd/mm/yyyy',
 * 'mm/dd/yyyy','dd-mon-yy','dd-mon-yyyy','dd-month-yy','dd-month-yyyy','yyyy-mm-dd')
 */
public class DataTypeDefinitions {

	public static Object getDataType(String input) {

		// check for empty object

		// checking for Integer

		// checking for floating point numbers

		// checking for date format dd/mm/yyyy

		// checking for date format mm/dd/yyyy

		// checking for date format dd-mon-yy

		// checking for date format dd-mon-yyyy

		// checking for date format dd-month-yy

		// checking for date format dd-month-yyyy

		String result = null;
		String integerRegex = "^\\d+$";
		String doubleRegex = "^-?\\d*\\.\\d+$";
		if (input == null || input.isEmpty()) {
			result = "java.lang.Object";
		} else if (input.matches(integerRegex)) {
			result = "java.lang.Integer";
		} else if (input.matches(doubleRegex)) {
			result = "java.lang.Double";
		} else if (isValidDate(input)) {
			result = "java.util.Date";
		} else {
			result = "java.lang.String";
		}

		return result;
	}
	private static boolean isValidDate(String dateValue) {

		boolean returnVal = false;

		List<String> dateFormatList = new ArrayList<String>();
		dateFormatList.add("dd/mm/yyyy");
		dateFormatList.add("mm/dd/yyyy");
		dateFormatList.add("dd-MMM-yy");
		dateFormatList.add("dd-MMM-yyyy");
		dateFormatList.add("dd-MMMM-yy");
		dateFormatList.add("dd-MMMM-yyyy");
		dateFormatList.add("yyyy-mm-dd");

		for (String dateFormat : dateFormatList) {
			try {
				SimpleDateFormat sdfObj = new SimpleDateFormat(dateFormat);
				sdfObj.setLenient(false);
				sdfObj.parse(dateValue);
				returnVal = true;
				break;
			} catch (ParseException e) {
				returnVal = false;
				continue;
			}
		}
		return returnVal;
	}
	

}
