package at.htl.waldl;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import javax.xml.transform.Result;
import java.sql.*;
import java.util.HashMap;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class WaldlTest {
    public static final String DRIVER_STRING = "org.apache.derby.jdbc.ClientDriver";
    public static final String CONNECTION_STRING = "jdbc:derby://localhost:1527/db;create=true";
    public static final String USER = "app";
    public static final String PASSWORD = "app";
    private static Connection conn;

    @BeforeClass
    public static void initJdbc(){
        try{
            Class.forName(DRIVER_STRING);
            conn = DriverManager.getConnection(CONNECTION_STRING, USER, PASSWORD);
        }catch (ClassNotFoundException e){
            e.printStackTrace();
        } catch (SQLException e) {
            System.err.println("Verbindung zur Datenbank nicht möglich\n"+ e.getMessage()+"\n");
            System.exit(1);
        }
    }

    public static void ddl() {
        try (Statement stmt = conn.createStatement()) {


            String carTable = "CREATE TABLE cars (" +
                    "id INT CONSTRAINT car_pk PRIMARY KEY," +
                    "brand varchar(255)," +
                    "car_state varchar(255)," +
                    "seats int," +
                    "kilometers_driven int" +
                    ")";
            stmt.execute(carTable);

            String driverTable = "Create TABLE drivers (" +
                    "id INT CONSTRAINT drivers_pk primary key ," +
                    "name varchar(255)," +
                    "main_car int," +
                    "sallary int," +
                    "CONSTRAINT drivers_fk_car foreign key (main_car) references cars(id)" +
                    ")";
            stmt.execute(driverTable);
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }
    }

    @AfterClass
    public static void teardownJdbc() {
        try (Statement stmt = conn.createStatement()) {
            String cleanupCarTable = "drop table cars";
            stmt.execute(cleanupCarTable);

            String cleanupDriverTable = "drop table drivers";
            stmt.execute(cleanupDriverTable);
        } catch (SQLException e) {
            e.printStackTrace();
        }

        try {
            if (conn != null && !conn.isClosed()) {
                conn.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void Test00_insertData() {
        int numberOfInserts = 0;
        try (Statement stmt = conn.createStatement()) {
            String sql = "INSERT INTO cars (id, brand, car_state, kilometers_driven) values(1, 'VW', 'new', 10000)";
            numberOfInserts += stmt.executeUpdate(sql);
            sql = "INSERT INTO cars (id, brand, car_state, kilometers_driven) values(2, 'Mercedes', 'old', 200000)";
            numberOfInserts += stmt.executeUpdate(sql);
            sql = "INSERT INTO cars (id, brand, car_state, kilometers_driven) values(3, 'Audi', 'new', 15000)";
            numberOfInserts += stmt.executeUpdate(sql);
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }
        assertThat(numberOfInserts, is(3));

        numberOfInserts = 0;
        try (Statement stmt = conn.createStatement()) {
            String sql = "INSERT INTO patient (id, name, main_car, sallary) values(1, 'Untersberger', 1, 2500)";
            numberOfInserts += stmt.executeUpdate(sql);
            sql = "INSERT INTO patient (id, name, main_car, sallary) values(2, 'Nobis', 2, 2000)";
            numberOfInserts += stmt.executeUpdate(sql);
            sql = "INSERT INTO patient (id, name, main_car, sallary) values(3, 'Waldl', 3, 3000)";
            numberOfInserts += stmt.executeUpdate(sql);
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }
        assertThat(numberOfInserts, is(3));
    }

    @Test
    public void Test10_verifyCarData() {
        try (
                Statement stmt = conn.createStatement();
                ResultSet doctorResultSet = stmt.executeQuery("SELECT id, brand, car_state, kilometers_driven FROM cars ORDER BY ID")
        ) {

            doctorResultSet.next();
            assertThat(doctorResultSet.getInt("id"), is(1));
            assertThat(doctorResultSet.getString("brand"), is("VW"));
            assertThat(doctorResultSet.getString("car_state"), is("new"));
            assertThat(doctorResultSet.getInt("kilometers_driven"), is(10000));

            doctorResultSet.next();
            assertThat(doctorResultSet.getInt("id"), is(2));
            assertThat(doctorResultSet.getString("brand"), is("Mercedes"));
            assertThat(doctorResultSet.getString("car_state"), is("old"));
            assertThat(doctorResultSet.getInt("kilometers_driven"), is(200000));

            doctorResultSet.next();
            assertThat(doctorResultSet.getInt("id"), is(3));
            assertThat(doctorResultSet.getString("brand"), is("Audi"));
            assertThat(doctorResultSet.getString("car_state"), is("new"));
            assertThat(doctorResultSet.getInt("kilometers_driven"), is(15000));

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void Test11_verifyDriverData() {
        try (
                Statement stmt = conn.createStatement();
                ResultSet doctorResultSet = stmt.executeQuery("SELECT id, name, main_car, sallary FROM cars ORDER BY ID")
        ) {

            doctorResultSet.next();
            assertThat(doctorResultSet.getInt("id"), is(1));
            assertThat(doctorResultSet.getString("name"), is("Untersberger"));
            assertThat(doctorResultSet.getInt("main_car"), is(1));
            assertThat(doctorResultSet.getInt("sallary"), is(2500));

            doctorResultSet.next();
            assertThat(doctorResultSet.getInt("id"), is(2));
            assertThat(doctorResultSet.getString("name"), is("Nobis"));
            assertThat(doctorResultSet.getInt("main_car"), is(2));
            assertThat(doctorResultSet.getInt("sallary"), is(2000));

            doctorResultSet.next();
            assertThat(doctorResultSet.getInt("id"), is(3));
            assertThat(doctorResultSet.getString("name"), is("Waldl"));
            assertThat(doctorResultSet.getInt("main_car"), is(3));
            assertThat(doctorResultSet.getInt("sallary"), is(3000));

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void Test20_verifyCarMetaData() {
        HashMap<String, Integer> columnsDefinitions = new HashMap<>();
        columnsDefinitions.put("ID", Types.INTEGER);
        columnsDefinitions.put("BRAND", Types.VARCHAR);
        columnsDefinitions.put("CAR_STATE", Types.VARCHAR);
        columnsDefinitions.put("KILOMETERS_DRIVEN", Types.INTEGER);

        try {
            DatabaseMetaData metaData = conn.getMetaData();
            ResultSet columns = metaData.getColumns(null, null, "CARS", null);

            while(columns.next()) {
                String columnName = columns.getString(4);
                int columnType = columns.getInt(5);

                assertThat(columnsDefinitions.containsKey(columnName), is(true));
                assertThat(columnsDefinitions.get(columnName), is(columnType));

                columnsDefinitions.remove(columnName);
            }
            assertThat(columnsDefinitions.isEmpty(), is(true));
            assertThat(columns.next(), is(false));

            columns.close();

            ResultSet primaryKeyColumn = metaData.getPrimaryKeys(null, null, "CARS");
            primaryKeyColumn.next();
            String primaryKeyName = primaryKeyColumn.getString(4);
            assertThat(primaryKeyName, is("ID"));

            primaryKeyColumn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void Test21_verifyDriverMetaData() {
        HashMap<String, Integer> columnsDefinitions = new HashMap<>();
        columnsDefinitions.put("ID", Types.INTEGER);
        columnsDefinitions.put("NAME", Types.VARCHAR);
        columnsDefinitions.put("MAIN_CAR", Types.INTEGER);
        columnsDefinitions.put("SALLARY", Types.INTEGER);

        try {
            DatabaseMetaData metaData = conn.getMetaData();
            ResultSet columns = metaData.getColumns(null, null, "DRIVERS", null);

            while(columns.next()) {
                String columnName = columns.getString(4);
                int columnType = columns.getInt(5);

                assertThat(columnsDefinitions.containsKey(columnName), is(true));
                assertThat(columnsDefinitions.get(columnName), is(columnType));

                columnsDefinitions.remove(columnName);
            }
            assertThat(columnsDefinitions.isEmpty(), is(true));
            assertThat(columns.next(), is(false));

            columns.close();

            ResultSet primaryKeyColumn = metaData.getPrimaryKeys(null, null, "DRIVERS");
            primaryKeyColumn.next();
            String primaryKeyName = primaryKeyColumn.getString(4);
            assertThat(primaryKeyName, is("ID"));

            primaryKeyColumn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}
