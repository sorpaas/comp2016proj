import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Scanner;

/**
 * This is a flight manager to support:
 *   (1) add a flight
 *   (2) delete a flight (by flight_no)
 *   (3) print flight information (by flight_no)
 *   (4) select a flight (by source, dest, stop_no = 0)
 *   (5) select a flight (by source, dest, stop_no = 1)
 *
 * @author comp1160/2016
 */

public class FlightManager {

        Scanner in = null;
        Connection conn = null;
        int localPordId = 1521; // the port number to connect database, do not change the port
        int defaultForwardPort = 9001; // if 9001 is occupied by other process, change it (see tutorial)
        String host = "jumbo";

        String[] options = { // if you want to add an option, append to the end of this array
                        "add a flight",
                        "print flight information (by flight_no)",
                        "delete a flight (by flight_no)",
                        "select a flight (by source, dest, stop_no = 0)",
                        "select a flight (by source, dest, stop_no = 1)"
                };

        /*
         * Login the oracle system.
         * Do not change this function
         * @return boolean
         */
        public boolean login() {
                String username = null, password = null, URL = null;
//              username = replace_with_your_jumbo_username, e.g., e12345678;
//              password = replace_with_your_jumbo_password, e.g., e12345678;
                URL = "jdbc:oracle:thin:@" + host + ":" + localPordId + ":oracle10";
                try {
                        System.out.println("logining...");
                        conn = DriverManager.getConnection(URL, username, password);
                        return true;
                } catch (SQLException e) {
                        e.printStackTrace();
                        return false;
                }
        }

        /*
         * Show the options. If you want to add one more option, put into the options array above.
         */
        public void showOptions() {
                System.out.println("Please choose following option:");
                for (int i = 0; i < options.length; ++i) {
                        System.out.println("(" + (i + 1) + ") " + options[i]);
                }
        }


        /*
         * Run the manager
         */
        public void run() {
                while (true) {
                        showOptions();
                        String line = in.nextLine();
                        if (line.equalsIgnoreCase("exit")) return;
                        int choice = -1;
                        try {
                                choice = Integer.parseInt(line);
                        } catch (Exception e) {
                                System.out.println("This option is not available");
                                continue;
                        }
                        if (! (choice >= 1 && choice <= options.length)) {
                                System.out.println("This option is not available");
                                continue;
                        }
                        if (options[choice-1].equals("add a flight")) {
                                addFlight();
                        } else if (options[choice-1].equals("delete a flight (by flight_no)")) {
                                deleteFlight();
                        } else if (options[choice-1].equals("print flight information (by flight_no)")){
                                printFlightByNo();
                        } else if (options[choice-1].equals("select a flight (by source, dest, stop_no = 0)")) {
                                selectFlightsInZeroStop();
                        } else if (options[choice-1].equals("select a flight (by source, dest, stop_no = 1)")) {
                                selectFlightsInOneStop();
                        }
                }
        }

        /**
         * Print out the infomation of a flight given a flight_no
         * @param flight_no
         */
        private void printFlightInfo(String flight_no) {
                try {
                        Statement stm = conn.createStatement();
                        String sql = "SELECT * FROM FLIGHTS WHERE Flight_no = '" + flight_no + "'";
                        ResultSet rs = stm.executeQuery(sql);
                        if (!rs.next()) return;
                        String[] heads = {"Flight_no", "Depart_Time", "Arrive_Time", "Fare", "Source", "Dest"};
                        for (int i = 0; i < 6; ++i) { // flight table 6 attributes
                                try {
                                        System.out.println(heads[i] + " : " + rs.getString(i + 1)); // attribute id starts with 1
                                } catch (SQLException e) {
                                        e.printStackTrace();
                                }
                        }
                } catch (SQLException e1) {
                        e1.printStackTrace();
                }
        }

        /**
         * List all flights in the database.
         */
        private void listAllFlights() {
                System.out.println("All flights in the database now:");
                try {
                        Statement stm = conn.createStatement();
                        String sql = "SELECT Flight_no FROM FLIGHTS";
                        ResultSet rs = stm.executeQuery(sql);

                        int resultCount = 0;
                        while (rs.next()) {
                                System.out.println(rs.getString(1));
                                ++resultCount;
                        }
                        System.out.println("Total " + resultCount + " flight(s).");
                        rs.close();
                        stm.close();
                } catch (SQLException e) {
                        e.printStackTrace();
                }
        }

        /**
         * Select out a flight according to the flight_no.
         */
        private void printFlightByNo() {
                listAllFlights();
                System.out.println("Please input the flight_no to print info:");
                String line = in.nextLine();
                line = line.trim();
                if (line.equalsIgnoreCase("exit")) return;

                printFlightInfo(line);
        }

        /**
         * Given source and dest, select all the flights can arrive the dest directly.
         * For example, given HK, Tokyo, you may find
         * HK -> Tokyo
         * Your job to fill in this function.
         */
        private void selectFlightsInZeroStop() {
                System.out.println("Please input source, dest:");

                String line = in.nextLine();

                if (line.equalsIgnoreCase("exit")) return;

                String[] values = line.split(",");
                for (int i = 0; i < values.length; ++i) values[i] = values[i].trim();

                try {
                        /**
                         * Create the statement and sql
                         */
                        Statement stm = conn.createStatement();
                        String sql = "SELECT Flight_no FROM FLIGHTS WHERE source = '" + values[0] + "' AND dest = '" + values[1] + "'";
                        ResultSet rs = stm.executeQuery(sql);

                        int resultCount = 0; // a counter to count the number of result records
                        while (rs.next()) { // this is the result record iterator, see the tutorial for details
                                printFlightInfo(rs.getString(1));
                                ++resultCount;
                                System.out.println("=================================================");
                        }
                        System.out.println("Total " + resultCount + " choice(s).");
                        rs.close();
                        stm.close();
                } catch (SQLException e) {
                        e.printStackTrace();
                }
        }


        /**
         * Given source and dest, select all the flights can arrive the dest in one stop.
         * For example, given HK, Tokyo, you may find
         * HK -> Beijing, Beijing -> Tokyo
         * Your job to fill in this function.
         */
        private void selectFlightsInOneStop() {
                System.out.println("Please input source, dest:");

                String line = in.nextLine();

                if (line.equalsIgnoreCase("exit")) return;

                String[] values = line.split(",");
                for (int i = 0; i < values.length; ++i) values[i] = values[i].trim();

                try {
                        /**
                         * Similar to the 'selectFlightsInZeroStop' function
                         */
                        Statement stm = conn.createStatement();
                        String sql = "SELECT F1.Flight_no, F2.Flight_no FROM FLIGHTS F1, FLIGHTS F2 WHERE F1.source = '" + values[0] + "' AND F1.dest = F2.source And F1.arrive_time < F2.depart_time And F2.dest = '" + values[1] + "'";
                        System.out.println(sql);
                        ResultSet rs = stm.executeQuery(sql);

                        int resultCount = 0;
                        while (rs.next()) {
                                printFlightInfo(rs.getString(1));
                                System.out.println("-------------------------------------------------");
                                printFlightInfo(rs.getString(2));
                                ++resultCount;
                                System.out.println("=================================================");
                        }
                        System.out.println("Total " + resultCount + " choice(s).");
                        rs.close();
                        stm.close();
                } catch (SQLException e) {
                        e.printStackTrace();
                }
        }




        /**
         * Insert data into database
         * @return
         */
        private void addFlight() {
                /**
                 * A sample input is:
                 * CX109, 2015/03/15/13:00:00, 2015/03/15/19:00:00, 2000, Beijing, Tokyo
                 */
                System.out.println("Please input the flight_no, depart_time, arrive_time, fare, source, dest:");
                String line = in.nextLine();

                if (line.equalsIgnoreCase("exit")) return;
                String[] values = line.split(",");

                if (values.length < 6) {
                        System.out.println("The value number is expected to be 6");
                        return;
                }
                for (int i = 0; i < values.length; ++i) values[i] = values[i].trim();

                try {
                        Statement stm = conn.createStatement();
                        String sql = "INSERT INTO FLIGHTS VALUES(" +
                                        "'" + values[0] + "', "+ // this is flight no
                                        "to_date('" + values[1] + "', 'yyyy/mm/dd/hh24:mi:ss'), " + // this is depart_time
                                        "to_date('" + values[2] + "', 'yyyy/mm/dd/hh24:mi:ss'), " + // this is arrive_time
                                        values[3] + ", " + // this is fare
                                        "'" + values[4] + "', " + // this is source
                                        "'" + values[5] + "'" + // this is dest
                                        ")";
                        stm.executeUpdate(sql);
                        stm.close();
                        System.out.println("succeed to add flight ");
                        printFlightInfo(values[0]);
                } catch (SQLException e) {
                        e.printStackTrace();
                        System.out.println("fail to add a flight " + line);
                }
        }

        /**
         * Please fill in this function to delete a flight.
         */
        public void deleteFlight() {
                listAllFlights();
                System.out.println("Please input the flight_no to delete:");
                String line = in.nextLine();

                if (line.equalsIgnoreCase("exit")) return;
                line = line.trim();

                try {
                        Statement stm = conn.createStatement();
                        String sql = "DELETE FROM FLIGHTS WHERE FLIGHT_NO = '" + line + "'";

                        stm.executeUpdate(sql); // please pay attention that we use executeUpdate to update the database
                        stm.close();
                        System.out.println("succeed to delete flight " + line);
                } catch (SQLException e) {
                        e.printStackTrace();
                        System.out.println("fail to delete flight " + line);
                }
        }

        /**
         * Close the manager.
         * Do not change this function.
         */
        public void close() {
                System.out.println("Thanks for using this manager! 88...");
                try {
                        if (conn != null) conn.close();
                        in.close();
                } catch (SQLException e) {
                        e.printStackTrace();
                }
        }



        /**
         * Constructor of flight manager
         * Do not change this function.
         */
        public FlightManager() {
                System.out.println("Welcome to use this manager!");
            in = new Scanner(System.in);
            System.out.println("Using ssh tunnel or not? [t/f]");
            String line = in.nextLine();
            if (line.equalsIgnoreCase("t")) { // if using ssh tunnel
                localPordId = defaultForwardPort;
                host = "localhost";
            }
        }

        /**
         * Main function
         * @param args
         */
        public static void main(String[] args) {

                FlightManager manager = new FlightManager();
                if (!manager.login()) {
                        System.out.println("Login failed, please re-examine your username and password!");
                } else {
                        System.out.println("Login succeed!");
                        manager.run();
                        manager.close();
                }
        }
}
