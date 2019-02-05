package com.stackroute.datamunger.writer;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

@SuppressWarnings("rawtypes")
public class JsonWriter {
	/*
	 * this method will write the resultSet object into a JSON file. On successful
	 * writing, the method will return true, else will return false
	 */
	public boolean writeToJson(Map resultSet) {
		/*
		 * Gson is a third party library to convert Java object to JSON. We will use
		 * Gson to convert resultSet object to JSON
		 */
		@SuppressWarnings("unused")
		Gson gson = new GsonBuilder().setPrettyPrinting().create();
		/*
		 * write JSON string to data/result.json file. As we are performing File IO,
		 * consider handling exception
		 */
		/* return true if file writing is successful */
		/* return false if file writing is failed */
		/* close BufferedWriter object */
		@SuppressWarnings("unused")
		String result = gson.toJson(resultSet);
		boolean status = false;

		BufferedWriter bw = null;
		FileWriter fw = null;

		try {

			fw = new FileWriter("data/result.json");
			bw = new BufferedWriter(fw);
			bw.write(result);
			status = true;

		} catch (IOException e) {

			e.printStackTrace();
			status = false;

		} finally {

			try {

				if (bw != null)
					bw.close();

				if (fw != null)
					fw.close();

			} catch (IOException ex) {

				ex.printStackTrace();
				status = false;

			}

			

		}
		return status;
	}
}
