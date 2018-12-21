import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.io.BufferedReader;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Scanner;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


/**
 * Takes CSV files and inputs them into a SQLite database
 * @author Connie Chi
 */
public class Main implements Serializable {

    public static void main(String[] args) {
        Runnable drawRunnable = new Runnable() {
            public void run() {
                main();
            }
        };
        ScheduledExecutorService exec = Executors.newScheduledThreadPool(1);
        exec.scheduleAtFixedRate(drawRunnable , 0, 5, TimeUnit.MINUTES);
    }

    /**
     * Adds all the files in the source folder recursively.
     * @param file Source directory for where to read files
     * @param all The collection datastructure for which to add files to
     */
    static void addTree(File file, Collection<File> all) {
        File[] children = file.listFiles();
        if (children != null) {
            for (File child : children) {
                all.add(child);
                addTree(child, all);
            }
        }
    }


    /**
     * Creates a database in the C:/sqlite/db folder
     * @param fileName Name of the database to be created
     * @return the connection to the database created
     */
    public static Connection createNewDatabase(String fileName) {
        String url = "jdbc:sqlite:C:/sqlite/db/" + fileName;
        try (Connection conn = DriverManager.getConnection(url)) {
//            if (conn != null) {
//                System.out.println("A new database has been created.");
//            }
            return conn;

        } catch (SQLException e) {
            System.out.println(e.getMessage());
            return null;
        }
    }

    /**
     * returns connection to the database in C:/sqlite/db
     * @param filename Name of database
     * @return Connection to database
     */
    public static Connection connect(String filename) {
        // SQLite connection string
        String url = "jdbc:sqlite:C://sqlite/db/" + filename;
        try {
            return DriverManager.getConnection(url);
        } catch (SQLException e) {
            System.out.println(e.getMessage());
            return null;
        }
    }

    public static void main() {
        Scanner reader = new Scanner(System.in);
        String source;
        try {
            source = (String) SerializableHelper.readObject(Utils.join("C:\\sqlite\\db", "source"));
//            System.out.println("Do you want to change source directory? (Y/N)");
//            char yn = reader.next().charAt(0);
//            boolean noPass = true;
//            while (noPass) {
//                if (yn == 'N' || yn == 'n'){
//                    noPass = false;
//                } else if (yn == 'Y' || yn == 'y') {
//                    System.out.println("Please give source directory folder for csv files");
//                    source = reader.next();
//                    noPass = false;
//                } else {
//                    System.out.println("Please give valid input");
//                    yn = reader.next().charAt(0);
//                }
//            }
        } catch (IllegalArgumentException e) {
            System.out.println("Please give source directory folder for csv files");
            source = reader.next();
        }
        int setID;
        ArrayList<File> inputted;
        //read serialized objects
        try {
            setID = (int) SerializableHelper.readObject(Utils.join(source, "setID"));
        } catch (IllegalArgumentException e) {
            setID = 0;
        }
        try {
            inputted = (ArrayList<File>) SerializableHelper.readObject(Utils.join(source, "inputted"));
        } catch (IllegalArgumentException e) {
            inputted = new ArrayList<>();
        }

        //reads all the files in source
        ArrayList<File> all = new ArrayList<File>();
        addTree(new File(source), all);
        //files that are just comma sepoerated files and have not been added previously
        ArrayList<File> csvFiles = new ArrayList<>();
        for (int i = 0; i < all.size(); i++) {
            String s = all.get(i).toString();
            String x = s.substring(s.length() - 3);
            if (x.compareTo("csv") == 0 && !inputted.contains(all.get(i))){
                csvFiles.add(all.get(i));
            }
        }
        reader.close();

        //creating new database and two tables
        try {
            createNewDatabase("main.db");
            Connection conn = connect("main.db");
            Statement stmt = conn.createStatement();
            stmt.execute("CREATE TABLE IF NOT EXISTS summary (\n" +
                    "setID integer, \n" +
                    "numRFepi integer, \n" +
                    "numLesions integer, \n" +
                    "numTransitions integer, \n" +
                    "RFtime text, \n" +
                    "AvgRFPower integer, \n" +
                    "numLesionsLess10g integer, \n" +
                    "LocDataRate integer, \n" +
                    "ECIdataRate integer, \n" +
                    "ContactForceDR integer, \n" +
                    "AmpereDataRate integer, \n" +
                    "LesionSpacParam integer, \n" +
                    "AwayTimeParam integer, \n" +
                    "MinLesionTimeParam integer \n" + ");");

            stmt.execute("CREATE TABLE IF NOT EXISTS info (\n" +
                    "setID integer, \n" +
                    "RFepi integer, \n" +
                    "LesionID integer, \n" +
                    "Date text, \n" +
                    "startTime text, \n" +
                    "endTime text, \n" +
                    "duration integer, \n" +
                    "isTrans integer, \n" +
                    "X integer, \n" +
                    "Y integer, \n" +
                    "Z integer, \n" +
                    "Energy integer, \n" +
                    "AvgPower integer, \n" +
                    "AvgTemp integer, \n" +
                    "MaxTemp integer, \n" +
                    "ImpMax integer, \n" +
                    "ImpMin integer, \n" +
                    "ImpDrop integer, \n" +
                    "ImpDropPerc integer, \n" +
                    "ECImax integer, \n" +
                    "ECIMin integer, \n" +
                    "ECIDrop integer, \n" +
                    "ECIDropPerc integer, \n" +
                    "AvgConForce integer, \n" +
                    "MinConForce integer, \n" +
                    "MaxConForce integer, \n" +
                    "FTI integer, \n" +
                    "LSI integer, \n" +
                    "FOREIGN KEY (setID) REFERENCES summary(setID)"+ ");");
        } catch(SQLException e) {
            System.out.println(e.getMessage());
        }
        //read each line for a file and input it into table
        for(File f: csvFiles) {
            inputted.add(f);
            try {
                BufferedReader br = new BufferedReader(new FileReader(f));
                String line;
                String[] sum = new String[13];
                int count  = 0;

                Connection conn = connect("main.db");
                while ((line = br.readLine()) != null) {

                    if (count < 13) {
                        //for the summary table
                        sum[count] = line.split(":")[1].substring(1);
                    } else if (count == 14) {
                        String info = "INSERT INTO summary VALUES(?,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
                        PreparedStatement pstmt = conn.prepareStatement(info);
                        for (int i = 0; i < sum.length; i++) {
                            if (i == 0) {
                                pstmt.setString(i + 1, Integer.toString(setID));
                            }
                            pstmt.setString(i + 2, sum[i]);
                        }
                        pstmt.executeUpdate();
                    } else if (count >= 15){
                        //for the info table
                        String[] values = line.split(",");
                        String in = "INSERT INTO info VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?," +
                                "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
                        PreparedStatement pInfo = conn.prepareStatement(in);
                        for(int i = 0; i < values.length; i++) {
                            if (i == 0) {
                                pInfo.setString(i + 1, Integer.toString(setID));
                            }
                            pInfo.setString(i + 2, values[i]);
                        }
                        pInfo.executeUpdate();
                    }
                    count++;
                }
                //DONT USE STRING CONCANTONATION
                br.close();
            } catch (FileNotFoundException e) {
                System.out.println(e.getMessage());
            } catch (IOException e) {
                System.out.println(e.getMessage());
            } catch (SQLException e) {
                System.out.println(e.getMessage());
            }
            setID++;


        }
        try {
            SerializableHelper.writeObject(setID, Utils.join(source, "setID"));
            SerializableHelper.writeObject(inputted, Utils.join(source, "inputted"));
            SerializableHelper.writeObject(source, Utils.join("C:\\sqlite\\db", "source"));
            System.out.println("Source location stored at C:\\sqlite\\db");
        } catch (IllegalArgumentException e) {
            System.out.println("Improper Source given! Are you sure that's the correct source location?");
        }

    }


}