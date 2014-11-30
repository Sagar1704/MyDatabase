package sagar.database.mydatabase;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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

	private ArrayList<String> attributes;
	private TreeMap<String, ArrayList<Long>> idIndex;
	private TreeMap<String, ArrayList<Long>> lnameIndex;
	private TreeMap<String, ArrayList<Long>> stateIndex;

	private static final String INPUTCSV = "us-500.csv";
	private static final String SRC = "src/";
	private static final String DATABASE = "data.db";
	private static final String ID = "id";
	private static final String LNAME = "last_name";
	private static final String STATE = "state";
	private static final String INDEX = ".ndx";

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
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		MyDatabase myDB = new MyDatabase();
		myDB.readFromCSV();
		System.out.println("1. Select By ID");
		String id = "1";
		System.out.println("Given ID:: " + id);
		System.out.println("Record extracted::\n" + myDB.getRecordByID(id));
	}

	private String getRecordByID(String id) {
		StringBuilder sb = new StringBuilder();
		ArrayList<Long> offsets = idIndex.get("\"" + id + "\"");
		try {
			for (Long offset : offsets) {
				file.seek(offset);
				for (String attribute : attributes) {
					sb.append(attribute + "::");
					sb.append(file.readUTF() + "\n");
				}
				sb.append("\n");
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return sb.toString();
	}

	public void deleteDatabase() {
		try {
			if (file != null)
				file.close();
			Path path = Paths.get(DATABASE);

			Files.delete(path);
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	/**
	 * Get the data from csv. And create data.db as the database. Also create
	 * index files based on id, last_name and state fields. Create only if not
	 * already created.
	 */
	private void readFromCSV() {
		boolean first = true;
		Scanner scanner = null;
		File database = null;
		try {
			scanner = new Scanner(getClass().getClassLoader()
					.getResourceAsStream(INPUTCSV));
			database = new File(SRC + DATABASE);
			file = new RandomAccessFile(database, "rw");
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
						for (String field : line
								.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)")) {

							file.seek(file.length());
							file.writeUTF(field);
							if (fieldIndex == attributes.indexOf("\"" + ID
									+ "\"")) {
								if (idIndex.containsKey(field)) {
									ArrayList<Long> offsets = idIndex
											.get(field);
									offsets.add(offset);
									idIndex.put(field, offsets);
								} else {
									ArrayList<Long> offsets = new ArrayList<Long>();
									offsets.add(offset);
									idIndex.put(field, offsets);
								}
							} else if (fieldIndex == attributes.indexOf("\""
									+ LNAME + "\"")) {
								if (lnameIndex.containsKey(field)) {
									ArrayList<Long> offsets = lnameIndex
											.get(field);
									offsets.add(offset);
									lnameIndex.put(field, offsets);
								} else {
									ArrayList<Long> offsets = new ArrayList<Long>();
									offsets.add(offset);
									lnameIndex.put(field, offsets);
								}
							} else if (fieldIndex == attributes.indexOf("\""
									+ STATE + "\"")) {
								if (stateIndex.containsKey(field)) {
									ArrayList<Long> offsets = stateIndex
											.get(field);
									offsets.add(offset);
									stateIndex.put(field, offsets);
								} else {
									ArrayList<Long> offsets = new ArrayList<Long>();
									offsets.add(offset);
									stateIndex.put(field, offsets);
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
	 * Create hashmap indexes from the index files
	 */
	private void readIndexes() {
		Scanner scanner = null;
		try {
			scanner = new Scanner(new File(SRC + ID + INDEX));
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

			scanner = new Scanner(new File(SRC + LNAME + INDEX));
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

			scanner = new Scanner(new File(SRC + STATE + INDEX));
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

			System.out
					.println("Generated indexes from the already created database and index files");
		} catch (Exception e) {
			System.out.println("Some problem with the index files");
		}
	}

	/**
	 * Create index files
	 */
	private void writeIndexes() {
		try {
			idFile = new PrintWriter(new File(SRC + ID + INDEX), "UTF-8");

			for (String id : idIndex.keySet()) {
				idFile.print(id);
				for (Long offset : idIndex.get(id)) {
					idFile.print(';');
					idFile.print(offset);
				}
				idFile.println();
			}

			System.out.println("Created ID index");

			lnameFile = new PrintWriter(new File(SRC + LNAME + INDEX), "UTF-8");

			for (String lname : lnameIndex.keySet()) {
				lnameFile.print(lname);
				for (Long offset : lnameIndex.get(lname)) {
					lnameFile.print(';');
					lnameFile.print(offset);
				}
				lnameFile.println();
			}

			System.out.println("Created Last Name index");

			stateFile = new PrintWriter(new File(SRC + STATE + INDEX), "UTF-8");

			for (String state : stateIndex.keySet()) {
				stateFile.print(state);
				for (Long offset : stateIndex.get(state)) {
					stateFile.print(';');
					stateFile.print(offset);
				}
				stateFile.println();
			}

			System.out.println("Created State index");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			idFile.close();
			lnameFile.close();
			stateFile.close();
		}
	}
}
