package com.verizon.database;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.AbstractMap.SimpleEntry;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class DBTableTest {

    private static String columnA = "A";
    private static String typeA = "Str";
    private static String columnB = "B";
    private static String typeB = "Int";


    @org.junit.jupiter.api.BeforeEach
    void setUp() {
        ArrayList<SimpleEntry<String, String>> names_and_types = new ArrayList<>();
        names_and_types.add(new SimpleEntry<>(columnA, typeA));
        names_and_types.add(new SimpleEntry<>(columnB, typeB));
        testTable = new DBTable(names_and_types);
        // Create 3 rows.
        List<SimpleEntry<String, Object>> row1 = new ArrayList<>();
        row1.add(new SimpleEntry<>(columnA, "One"));
        row1.add(new SimpleEntry<>(columnB, 1));
        testTable.InsertRow(row1);

        List<SimpleEntry<String, Object>> row2 = new ArrayList<>();
        row2.add(new SimpleEntry<>(columnA, "Two"));
        row2.add(new SimpleEntry<>(columnB, 2));
        testTable.InsertRow(row2);

        List<SimpleEntry<String, Object>> row3 = new ArrayList<>();
        row3.add(new SimpleEntry<>(columnA, "Three"));
        row3.add(new SimpleEntry<>(columnB, 1));
        testTable.InsertRow(row3);
    }

    @org.junit.jupiter.api.AfterEach
    void tearDown() {
    }

    @org.junit.jupiter.api.Test
    void insertRow() {
        List<SimpleEntry<String, Object>> row = new ArrayList<>();
        row.add(new SimpleEntry<>(columnA, "Four"));
        row.add(new SimpleEntry<>(columnB, 4));
        testTable.InsertRow(row);
        List<List<SimpleEntry<String, Object>>> all_rows = testTable.GetAllRows();
        assertEquals(4, all_rows.size());
        assertEquals("Four", all_rows.get(3).get(0).getValue());
        assertEquals(4, all_rows.get(3).get(1).getValue());
    }

    @org.junit.jupiter.api.Test
    void deleteRow() {
        assertTrue(testTable.DeleteRow(1));
        List<List<SimpleEntry<String, Object>>> all_rows = testTable.GetAllRows();
        assertEquals(2, all_rows.size());
        assertEquals("One", all_rows.get(0).get(0).getValue());
        assertEquals("Three", all_rows.get(1).get(0).getValue());
    }

    @org.junit.jupiter.api.Test
    void updateRow() {
        assertTrue(testTable.UpdateRow(1, new SimpleEntry<>(columnA, "Four")));
        List<List<SimpleEntry<String, Object>>> all_rows = testTable.GetAllRows();
        assertEquals(3, all_rows.size());
        assertEquals("Four", all_rows.get(1).get(0).getValue());
    }

    @org.junit.jupiter.api.Test
    void getAllRows() {
        List<List<SimpleEntry<String, Object>>> all_rows = testTable.GetAllRows();

        assertEquals(3, all_rows.size());

        assertEquals("One", all_rows.get(0).get(0).getValue());
        assertEquals(1, all_rows.get(0).get(1).getValue());

        assertEquals("Two", all_rows.get(1).get(0).getValue());
        assertEquals(2, all_rows.get(1).get(1).getValue());

        assertEquals("Three", all_rows.get(2).get(0).getValue());
        assertEquals(1, all_rows.get(2).get(1).getValue());
    }

    @org.junit.jupiter.api.Test
    void getAllRowsLimitCount() {
        List<List<SimpleEntry<String, Object>>> all_rows = testTable.GetAllRowsLimitCount(1);

        assertEquals(1, all_rows.size());

        assertEquals("One", all_rows.get(0).get(0).getValue());
        assertEquals(1, all_rows.get(0).get(1).getValue());
    }

    @org.junit.jupiter.api.Test
    void getSortedRowsLimitCount() {
        List<List<SimpleEntry<String, Object>>> all_rows = testTable.GetSortedRowsLimitCount(3, "A");
        assertEquals(3, all_rows.size());
        assertEquals("One", all_rows.get(0).get(0).getValue());
        assertEquals("Three", all_rows.get(1).get(0).getValue());
        assertEquals("Two", all_rows.get(2).get(0).getValue());
    }

    @org.junit.jupiter.api.Test
    void getGroupByCounts() {
        List<SimpleEntry<Object, Integer>> all_rows = testTable.GetGroupByCounts("B");
        assertEquals(1, all_rows.get(0).getKey());
        assertEquals(2, all_rows.get(0).getValue());
        assertEquals(2, all_rows.get(1).getKey());
        assertEquals(1, all_rows.get(1).getValue());
    }

    private DBTable testTable;
}