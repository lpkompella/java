import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Scanner;

public class sortTextFile {

    public static void main (String args[]) throws IOException {
                BufferedReader reader = null;
                PrintWriter outputStream = null;
                ArrayList<String> rows = new ArrayList<String>();

                try {


                    Scanner user = new Scanner( System.in );
                    String  inputFileName;

                    // prepare the input file
                    System.out.print("Input File Name: ");
                    inputFileName = user.nextLine().trim();

                    //Reading the text file to be sorted
                    reader  = new BufferedReader(new FileReader(inputFileName));
                    outputStream = new PrintWriter(new FileWriter("Output.txt"));
                    String line;
                    while ((line = reader .readLine()) != null) {
                     //Reading each line and creating an array list.
                        rows.add(line);
                    }

                    //System.out.println("Before Sorting : "+rows);

                    //Using Java collections to sort the file and ignore the case while sorting incase the same name is encountered with different case.
                    Collections.sort(rows,String::compareToIgnoreCase);

                   // System.out.println("After Sorting : " + rows);

                    String[] strArr= rows.toArray(new String[0]);

                    //Loop through each element in the sorted array
                    for (String cur : strArr)
                        outputStream.println(cur);
                    }
                catch(Exception e)
                {
                    System.out.println(e);
                }
                finally {
                    if (reader  != null) {
                        reader.close();
                    }
                    if (outputStream != null) {
                        outputStream.close();
                    }
                }
            }

        }
