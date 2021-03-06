package sagar.database.mydatabase;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Scanner;
import java.util.TreeMap;

/**
 * Program to understand database indexing
 * 
 * @author Sagar
 * 
 */
public class MyDatabase {
	private RandomAccessFile file;
	private PrintWriter idFile;
	private PrintWriter lnameFile;
	private PrintWriter stateFile;
	private PrintWriter emailFile;

	private ArrayList<String> attributes;
	private TreeMap<String, ArrayList<Long>> idIndex;
	private TreeMap<String, ArrayList<Long>> lnameIndex;
	private TreeMap<String, ArrayList<Long>> stateIndex;
	private TreeMap<String, ArrayList<Long>> emailIndex;

	private static final String INPUTCSV = "us-500.csv";
	private static final String DATABASE = "data.db";
	private static final String ID = "id";
	private static final String LNAME = "last_name";
	private static final String STATE = "state";
	private static final String EMAIL = "email";
	private static final String INDEX = ".ndx";
	private static final String REGEX = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)";

	public MyDatabase() {
		this.attributes = new ArrayList<String>();
		this.idIndex = new TreeMap<String, ArrayList<Long>>(
				new Comparator<String>() {

					@Override
					public int compare(String o1, String o2) {
						return Integer.parseInt(o1.substring(1, o1.length() - 1)) < Integer
								.parseInt(o2.substring(1, o2.length() - 1)) ? -1
								: Integer.parseInt(o1.substring(1,
										o1.length() - 1)) > Integer.parseInt(o2
										.substring(1, o2.length() - 1)) ? 1 : 0;
					}
				});
		this.lnameIndex = new TreeMap<String, ArrayList<Long>>();
		this.stateIndex = new TreeMap<String, ArrayList<Long>>();
		this.emailIndex = new TreeMap<String, ArrayList<Long>>();
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		MyDatabase myDB = new MyDatabase();

		// Read from CSV input and create data.db
		myDB.readFromCSV();

		// Select operation
		System.out.println("************************************************");
		System.out.println("1a. Select By ID");
		String id = "6";
		System.out.println("Given ID:: " + id);
		System.out.println("************************************************");
		String record = myDB.select(id, myDB.idIndex);
		if (record.equals(""))
			System.out.println("Record with ID::" + id + ", does not exist");
		else
			System.out.println("Record extracted::\n" + record);

		System.out.println("************************************************");
		System.out.println("1b. Select By Last Name");
		String lname = "Butt";
		System.out.println("Given Lastname:: " + lname);
		System.out.println("************************************************");
		record = myDB.select(lname, myDB.lnameIndex);
		if (record.equals(""))
			System.out.println("Record with Last Name::" + lname
					+ ", does not exist");
		else
			System.out.println("Record extracted::\n" + record);

		System.out.println("************************************************");
		System.out.println("1c. Select By State");
		String state = "LA";
		System.out.println("Given State:: " + state);
		System.out.println("************************************************");
		record = myDB.select(state, myDB.stateIndex);
		if (record.equals(""))
			System.out.println("Record with State::" + state
					+ ", does not exist");
		else
			System.out.println("Record extracted::\n" + record);

		// Insert Operation
		System.out.println("************************************************");
		record = "\"1\",\"John\",\"Doe\",\"Benton, John B Jr\",\"6649 N Blue Gum St\",\"New Orleans\",\"Orleans\",\"LA\",70116,\"504-621-8927\",\"504-845-1427\",\"jdoe@gmail.com\",\"http://www.bentonjohnbjr.com\"";
		System.out.println("2a. Insert duplicate ID record");
		System.out.println("************************************************");
		if (myDB.insert(record))
			System.out.println("Successfully inserted record::\n" + record);
		else
			System.out.println("Duplicate record::\n" + record);

		System.out.println("************************************************");
		record = "\"503\",\"John\",\"Doe\",\"Benton, John B Jr\",\"6649 N Blue Gum St\",\"New Orleans\",\"Orleans\",\"LA\",70116,\"504-621-8927\",\"504-845-1427\",\"jbutt@gmail.com\",\"http://www.bentonjohnbjr.com\"";
		System.out
				.println("2b. Insert duplicate ID record(EMAIL is not unique)");
		System.out.println("************************************************");
		if (myDB.insert(record))
			System.out.println("Successfully inserted record::\n" + record);
		else
			System.out.println("Duplicate record::\n" + record);

		System.out.println("************************************************");
		record = "\"503\",\"John\",\"Doe\",\"Benton, John B Jr\",\"6649 N Blue Gum St\",\"New Orleans\",\"Orleans\",\"LA\",70116,\"504-621-8927\",\"504-845-1427\",\"jdoe@gmail.com\",\"http://www.bentonjohnbjr.com\"";
		System.out.println("2c. Insert a record");
		System.out.println("************************************************");
		if (myDB.insert(record)) {
			System.out.println("Successfully inserted record::\n" + record);
			myDB.deleteIndexes();
			myDB.writeIndexes();
		} else
			System.out.println("Duplicate record::\n" + record);

		// Delete Operation
		System.out.println("************************************************");
		System.out.println("3a. Delete non-existent record");
		id = "502";
		System.out.println("Trying to delete record with ID::" + id);
		System.out.println("************************************************");
		record = myDB.delete(id);
		if (record.equals(""))
			System.out.println("Record with ID:" + id + ", does not exist");
		else {
			System.out.println("Successfully deleted record::\n" + record);
			myDB.deleteIndexes();
			myDB.writeIndexes();
		}

		System.out.println("************************************************");
		System.out.println("3b. Delete existing record");
		id = "5";
		System.out.println("Trying to delete record with ID::" + id);
		System.out.println("************************************************");
		record = myDB.delete(id);
		if (record.equals(""))
			System.out.println("Record with ID:" + id + ", does not exist");
		else {
			System.out.println("Successfully deleted record::\n" + record);
			myDB.deleteIndexes();
			myDB.writeIndexes();
		}

		// Modify/Update operation
		System.out.println("************************************************");
		System.out.println("4a. Modify non-existent record");
		id = "502";
		String field_name = "phone1";
		String new_value = "504-767-9196";
		System.out.println("Modify " + field_name + " with " + new_value
				+ " for a record with ID: " + id);
		System.out.println("************************************************");
		record = myDB.modify(id, field_name, new_value);
		if (record.equals(""))
			System.out.println("Record with ID:" + id + ", does not exist");
		else {
			System.out.println("Successfully modified record::\n" + record);
		}

		System.out.println("************************************************");
		System.out.println("4b. Modify existing record");
		id = "2";
		field_name = "phone1";
		new_value = "504-767-9196";
		System.out.println("Modify " + field_name + " with " + new_value
				+ " for a record with ID: " + id);
		System.out.println("************************************************");
		record = myDB.modify(id, field_name, new_value);
		if (record.equals(""))
			System.out.println("Record with ID:" + id + ", does not exist");
		else if (record.equals("-1"))
			System.out.println(new_value + " cannot be larger than old "
					+ field_name + " value");
		else
			System.out.println("Successfully modified record::\n" + record);

		System.out.println("************************************************");
		System.out
				.println("4c. Modify existing record with new value bigger than old value");
		id = "1";
		field_name = "email";
		new_value = "jbutt@hotmail.com";
		System.out.println("Modify " + field_name + " with " + new_value
				+ " for a record with ID: " + id);
		System.out.println("************************************************");
		record = myDB.modify(id, field_name, new_value);
		if (record.equals(""))
			System.out.println("Record with ID:" + id + ", does not exist");
		else if (record.equals("-1"))
			System.out.println(new_value + " cannot be larger than old "
					+ field_name + " value");
		else if (record.equals("-2"))
			System.out.println(new_value
					+ " is already an ID of another record");
		else {
			System.out.println("Successfully modified record::\n" + record);
			myDB.deleteIndexes();
			myDB.writeIndexes();
		}

		// Count operation
		System.out.println("************************************************");
		System.out.println("5. Number of records currently in the database:: "
				+ myDB.count());
		System.out.println("************************************************");

	}

	/**
	 * Count operation
	 * 
	 * @return
	 */
	private int count() {
		return idIndex.size();
	}

	/**
	 * Modify/Update operation
	 * 
	 * @param id
	 * @param field_name
	 * @param new_value
	 * @return
	 */
	private String modify(String id, String field_name, String new_value) {
		String record = select(id, idIndex);
		if (field_name.contains(ID)
				&& idIndex.containsKey("\"" + new_value + "\"")) {
			record = "-2";
		} else {
			if (record != null && record.length() > 0) {
				long offset = idIndex.get("\"" + id + "\"").get(0);
				try {
					file.seek(offset);
					for (String fields : record.split("\t")) {
						String field = fields.split("::")[0];
						String fieldValue = fields.split("::")[1];

						if (field.equalsIgnoreCase("\"" + field_name + "\"")) {
							if (field_name.contains(ID)) {
								ArrayList<Long> offsets = idIndex
										.get(fieldValue);
								offsets.remove(offset);
								if (offsets.size() == 0)
									idIndex.remove(fieldValue);
								else
									idIndex.put(fieldValue, offsets);
							} else if (field_name.contains(LNAME)) {
								ArrayList<Long> offsets = lnameIndex
										.get(fieldValue);
								offsets.remove(offset);
								if (offsets.size() == 0)
									lnameIndex.remove(fieldValue);
								else
									lnameIndex.put(fieldValue, offsets);
							} else if (field_name.contains(STATE)) {
								ArrayList<Long> offsets = stateIndex
										.get(fieldValue);
								offsets.remove(offset);
								if (offsets.size() == 0)
									stateIndex.remove(fieldValue);
								else
									stateIndex.put(fieldValue, offsets);
							} else if (field_name.contains(EMAIL)) {
								ArrayList<Long> offsets = emailIndex
										.get(fieldValue);
								offsets.remove(offset);
								if (offsets.size() == 0)
									emailIndex.remove(fieldValue);
								else
									emailIndex.put(fieldValue, offsets);
							}

							new_value = "\"" + new_value + "\"";
							if (new_value.length() <= fieldValue.length()) {
								for (int i = new_value.length(); i < fieldValue
										.length(); i++) {
									new_value = new_value + " ";
								}
								file.writeUTF(new_value);
							} else {

								record = "-1";
							}
							break;
						}

						file.readUTF();
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		return record;
	}

	/**
	 * Delete operation
	 * 
	 * @param id
	 * @return
	 */
	private String delete(String id) {
		String record = select(id, idIndex);
		if (record != null && record.length() > 0) {
			long offset = idIndex.get("\"" + id + "\"").get(0);
			try {
				file.seek(offset);
				idIndex.remove("\"" + id + "\"");
				for (String fields : record.split("\t")) {
					String field = fields.split("::")[0];
					String fieldValue = fields.split("::")[1];
					if (field.equalsIgnoreCase("\"" + LNAME + "\"")) {
						ArrayList<Long> offsets = lnameIndex.get(fieldValue);
						offsets.remove(offset);
						if (offsets.size() == 0)
							lnameIndex.remove(fieldValue);
						else
							lnameIndex.put(fieldValue, offsets);
					} else if (field.equalsIgnoreCase("\"" + STATE + "\"")) {
						ArrayList<Long> offsets = stateIndex.get(fieldValue);
						offsets.remove(offset);
						if (offsets.size() == 0)
							stateIndex.remove(fieldValue);
						else
							stateIndex.put(fieldValue, offsets);
					} else if (field.equalsIgnoreCase("\"" + EMAIL + "\"")) {
						ArrayList<Long> offsets = emailIndex.get(fieldValue);
						offsets.remove(offset);
						if (offsets.size() == 0)
							emailIndex.remove(fieldValue);
						else
							emailIndex.put(fieldValue, offsets);
					}

					String string = new String();
					for (int i = 0; i < fieldValue.length(); i++) {
						string = string + 0;
					}
					file.writeUTF(string);

				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return record;
	}

	/**
	 * Insert operation
	 * 
	 * @param record
	 * @return
	 */
	private boolean insert(String record) {
		Scanner scanner = null;
		try {
			if (idIndex.containsKey(record.split(REGEX)[attributes.indexOf("\""
					+ ID + "\"")])
					|| emailIndex.containsKey(record.split(REGEX)[attributes
							.indexOf("\"" + EMAIL + "\"")]))
				return false;
			int fieldCounter = 0;
			ArrayList<Long> offsets = new ArrayList<Long>();
			file.seek(file.length());
			long offset = file.getFilePointer();
			offsets.add(offset);
			for (String field : record.split(REGEX)) {

				file.seek(file.length());
				file.writeUTF(field.toLowerCase());

				if (fieldCounter == attributes.indexOf("\"" + ID + "\"")) {
					idIndex.put(field.toLowerCase(), offsets);
				} else if (fieldCounter == attributes.indexOf("\"" + LNAME
						+ "\"")) {
					if (lnameIndex.containsKey(field.toLowerCase())) {
						offsets = lnameIndex.get(field.toLowerCase());
						offsets.add(offset);
						lnameIndex.put(field.toLowerCase(), offsets);
					} else {
						lnameIndex.put(field.toLowerCase(), offsets);
					}
				} else if (fieldCounter == attributes.indexOf("\"" + STATE
						+ "\"")) {
					if (stateIndex.containsKey(field.toLowerCase())) {
						offsets = stateIndex.get(field.toLowerCase());
						offsets.add(offset);
						stateIndex.put(field.toLowerCase(), offsets);
					} else {
						stateIndex.put(field.toLowerCase(), offsets);
					}
				} else if (fieldCounter == attributes.indexOf("\"" + EMAIL
						+ "\"")) {
					if (emailIndex.containsKey(field.toLowerCase())) {
						offsets = emailIndex.get(field.toLowerCase());
						offsets.add(offset);
						emailIndex.put(field.toLowerCase(), offsets);
					} else {
						emailIndex.put(field.toLowerCase(), offsets);
					}
				}
				fieldCounter++;
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (scanner != null)
				scanner.close();
		}
		return true;
	}

	/**
	 * Select Operation
	 * 
	 * @param field
	 * @param map
	 * @return
	 */
	private String select(String field, TreeMap<String, ArrayList<Long>> map) {
		StringBuilder sb = new StringBuilder();
		ArrayList<Long> offsets = map.get("\"" + field.toLowerCase() + "\"");
		try {
			if (offsets != null && offsets.size() > 0) {
				for (Long offset : offsets) {
					file.seek(offset);
					for (String attribute : attributes) {
						if (file.getFilePointer() <= file.length()) {
							sb.append(attribute + "::");
							sb.append(file.readUTF() + "\t");
						}
					}
					sb.append("\n");
				}
				sb.delete(sb.length() - 2, sb.length());
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return sb.toString();
	}

	/**
	 * Delete the index files
	 */
	public void deleteIndexes() {
		if (idFile != null)
			idFile.close();

		File idIndexFile = new File(ID + INDEX);
		idIndexFile.delete();
		
		if (lnameFile != null)
			lnameFile.close();
		
		File lnameIndexFile = new File(LNAME + INDEX);
		lnameIndexFile.delete();

		if (stateFile != null)
			stateFile.close();

		File stateIndexFile = new File(STATE + INDEX);
		stateIndexFile.delete();
		
		if (emailFile != null)
			emailFile.close();

		File emailIndexFile = new File(EMAIL + INDEX);
		emailIndexFile.delete();

	}

	/**
	 * Get the data from CSV. And create data.db as the database. Also create
	 * index files based on id, last_name and state fields. Create only if not
	 * already created.
	 */
	private void readFromCSV() {
		boolean first = true;
		Scanner scanner = null;
		try {
			scanner = new Scanner(getClass().getClassLoader()
					.getResourceAsStream(INPUTCSV));
			file = new RandomAccessFile(DATABASE, "rw");
			if (file.length() == 0) {
				while (scanner.hasNextLine()) {
					String line = scanner.nextLine();
					if (first) {
						first = false;
						for (String attribute : line.split(",")) {
							attributes.add(attribute);
						}
					} else {
						int fieldIndex = 0;
						Long offset = file.getFilePointer();
						for (String field : line.split(REGEX)) {

							file.seek(file.length());
							file.writeUTF(field.toLowerCase());
							if (fieldIndex == attributes.indexOf("\"" + ID
									+ "\"")) {
								if (idIndex.containsKey(field.toLowerCase())) {
									ArrayList<Long> offsets = idIndex.get(field
											.toLowerCase());
									offsets.add(offset);
									idIndex.put(field.toLowerCase(), offsets);
								} else {
									ArrayList<Long> offsets = new ArrayList<Long>();
									offsets.add(offset);
									idIndex.put(field.toLowerCase(), offsets);
								}
							} else if (fieldIndex == attributes.indexOf("\""
									+ LNAME + "\"")) {
								if (lnameIndex.containsKey(field.toLowerCase())) {
									ArrayList<Long> offsets = lnameIndex
											.get(field.toLowerCase());
									offsets.add(offset);
									lnameIndex
											.put(field.toLowerCase(), offsets);
								} else {
									ArrayList<Long> offsets = new ArrayList<Long>();
									offsets.add(offset);
									lnameIndex
											.put(field.toLowerCase(), offsets);
								}
							} else if (fieldIndex == attributes.indexOf("\""
									+ STATE + "\"")) {
								if (stateIndex.containsKey(field.toLowerCase())) {
									ArrayList<Long> offsets = stateIndex
											.get(field.toLowerCase());
									offsets.add(offset);
									stateIndex
											.put(field.toLowerCase(), offsets);
								} else {
									ArrayList<Long> offsets = new ArrayList<Long>();
									offsets.add(offset);
									stateIndex
											.put(field.toLowerCase(), offsets);
								}
							} else if (fieldIndex == attributes.indexOf("\""
									+ EMAIL + "\"")) {
								if (emailIndex.containsKey(field.toLowerCase())) {
									ArrayList<Long> offsets = emailIndex
											.get(field.toLowerCase());
									offsets.add(offset);
									emailIndex
											.put(field.toLowerCase(), offsets);
								} else {
									ArrayList<Long> offsets = new ArrayList<Long>();
									offsets.add(offset);
									emailIndex
											.put(field.toLowerCase(), offsets);
								}
							}
							fieldIndex++;
						}
					}
				}
				System.out.println("Created new database: " + DATABASE);
				writeIndexes();
			} else {
				setAttributes();
				readIndexes();
			}
		} catch (Exception e) {
			System.out.println("Some problem with the input file");
		} finally {
			if (scanner != null)
				scanner.close();
		}
	}

	/**
	 * Set the field names from CSV
	 */
	private void setAttributes() {
		Scanner scanner = new Scanner(getClass().getClassLoader()
				.getResourceAsStream(INPUTCSV));
		if (scanner.hasNextLine()) {
			String line = scanner.nextLine();
			for (String attribute : line.split(",")) {
				attributes.add(attribute);
			}
		}
		scanner.close();
	}

	/**
	 * Create treeMap indexes from the index files
	 */
	private void readIndexes() {
		Scanner scanner = null;
		try {
			scanner = new Scanner(getClass().getClassLoader()
					.getResourceAsStream(ID + INDEX));
			while (scanner.hasNextLine()) {
				String line = scanner.nextLine();
				boolean first = true;
				String id = "";
				for (String string : line.split(";")) {
					if (first) {
						id = string;
						first = false;
					} else {
						if (idIndex.containsKey(id)) {
							ArrayList<Long> offsets = idIndex.get(id);
							offsets.add(Long.parseLong(string));
							idIndex.put(id, offsets);
						} else {
							ArrayList<Long> offsets = new ArrayList<Long>();
							offsets.add(Long.parseLong(string));
							idIndex.put(id, offsets);
						}
					}
				}
			}

			scanner = new Scanner(getClass().getClassLoader()
					.getResourceAsStream(LNAME + INDEX));
			while (scanner.hasNextLine()) {
				String line = scanner.nextLine();
				boolean first = true;
				String lname = "";
				for (String string : line.split(";")) {
					if (first) {
						lname = string;
						first = false;
					} else {
						if (lnameIndex.containsKey(lname)) {
							ArrayList<Long> offsets = lnameIndex.get(lname);
							offsets.add(Long.parseLong(string));
							lnameIndex.put(lname, offsets);
						} else {
							ArrayList<Long> offsets = new ArrayList<Long>();
							offsets.add(Long.parseLong(string));
							lnameIndex.put(lname, offsets);
						}
					}
				}
			}

			scanner = new Scanner(getClass().getClassLoader()
					.getResourceAsStream(STATE + INDEX));
			while (scanner.hasNextLine()) {
				String line = scanner.nextLine();
				boolean first = true;
				String state = "";
				for (String string : line.split(";")) {
					if (first) {
						state = string;
						first = false;
					} else {
						if (stateIndex.containsKey(state)) {
							ArrayList<Long> offsets = stateIndex.get(state);
							offsets.add(Long.parseLong(string));
							stateIndex.put(state, offsets);
						} else {
							ArrayList<Long> offsets = new ArrayList<Long>();
							offsets.add(Long.parseLong(string));
							stateIndex.put(state, offsets);
						}
					}
				}
			}

			scanner = new Scanner(getClass().getClassLoader()
					.getResourceAsStream(EMAIL + INDEX));
			while (scanner.hasNextLine()) {
				String line = scanner.nextLine();
				boolean first = true;
				String email = "";
				for (String string : line.split(";")) {
					if (first) {
						email = string;
						first = false;
					} else {
						if (emailIndex.containsKey(email)) {
							ArrayList<Long> offsets = emailIndex.get(email);
							offsets.add(Long.parseLong(string));
							emailIndex.put(email, offsets);
						} else {
							ArrayList<Long> offsets = new ArrayList<Long>();
							offsets.add(Long.parseLong(string));
							emailIndex.put(email, offsets);
						}
					}
				}
			}

			System.out
					.println("Generated indexes from the already created database and index files");
		} catch (Exception e) {
			System.out.println("Some problem with the index files");
		}
	}

	/**
	 * Create index files from treeMaps
	 */
	private void writeIndexes() {
		try {
			File idIndexFile = new File(ID + INDEX);
			idFile = new PrintWriter(idIndexFile, "UTF-8");

			for (String id : idIndex.keySet()) {
				idFile.print(id);
				for (Long offset : idIndex.get(id)) {
					idFile.print(';');
					idFile.print(offset);
				}
				idFile.println();
			}

			File lnameIndexFile = new File(LNAME + INDEX);
			lnameFile = new PrintWriter(lnameIndexFile, "UTF-8");

			for (String lname : lnameIndex.keySet()) {
				lnameFile.print(lname);
				for (Long offset : lnameIndex.get(lname)) {
					lnameFile.print(';');
					lnameFile.print(offset);
				}
				lnameFile.println();
			}

			File stateIndexFile = new File(STATE + INDEX);
			stateFile = new PrintWriter(stateIndexFile, "UTF-8");

			for (String state : stateIndex.keySet()) {
				stateFile.print(state);
				for (Long offset : stateIndex.get(state)) {
					stateFile.print(';');
					stateFile.print(offset);
				}
				stateFile.println();
			}

			File emailIndexFile = new File(EMAIL + INDEX);
			emailFile = new PrintWriter(emailIndexFile, "UTF-8");

			for (String email : emailIndex.keySet()) {
				emailFile.print(email);
				for (Long offset : emailIndex.get(email)) {
					emailFile.print(';');
					emailFile.print(offset);
				}
				emailFile.println();
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			idFile.close();
			lnameFile.close();
			stateFile.close();
			emailFile.close();
		}
	}
}
