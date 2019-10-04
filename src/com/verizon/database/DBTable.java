package com.verizon.database;

import java.util.*;
import java.util.AbstractMap.SimpleEntry;


public class DBTable {
    // Create a table (2).
    public DBTable(ArrayList<SimpleEntry<String, String>> columns_and_types) {
        table = new ArrayList<>();
        deletedKeys = new HashSet<Integer>();
        value_to_index_maps = new HashMap<>();
        names_to_index = new HashMap<>();
        names_to_types = new HashMap<>();
        for (SimpleEntry<String, String> name_type_pair : columns_and_types) {
            names_to_types.put(name_type_pair.getKey(), name_type_pair.getValue());
            assert(name_type_pair.getValue().equals("Int") || name_type_pair.getValue().equals("Str"));
            names_to_index.put(name_type_pair.getKey(), names_to_index.size());
            value_to_index_maps.put(name_type_pair.getKey(), new TreeMap<Object, Set<Integer>>());
        }
    }

    // Implements (3) Insert row.
    public int InsertRow(List<SimpleEntry<String, Object>> row) {
        List<Object> new_row = new ArrayList<>(names_to_index.size());
        for (int i = names_to_index.size(); i > 0; --i) new_row.add(null);
        for (SimpleEntry<String, Object> name_value_pair : row) {
            if (!names_to_index.containsKey(name_value_pair.getKey())) {
                return -1;
            }
            Object value = name_value_pair.getValue();
            new_row.set(names_to_index.get(name_value_pair.getKey()), value);
            SortedMap<Object, Set<Integer>> index_map = value_to_index_maps.get(name_value_pair.getKey());
            if (!index_map.containsKey(value)) {
                index_map.put(value, new HashSet<Integer>());
            }
            index_map.get(value).add(table.size());
        }
        table.add(new_row);
        return table.size() - 1;
    }

    // Implements (4) Delete Row.
    public boolean DeleteRow(int rowKey) {
        if (rowKey > table.size()) {
            return false;
        }
        List<Object> row = table.get(rowKey);
        for (Map.Entry<String, Integer> name_index_pair : names_to_index.entrySet()) {
            String column_name = name_index_pair.getKey();
            Integer index = name_index_pair.getValue();
            if (!value_to_index_maps.get(column_name).get(row.get(index)).remove(rowKey)) {
                return false;
            }
        }
        return deletedKeys.add(rowKey);
    }

    // Implements (5) Update row.
    public boolean UpdateRow(int rowKey, SimpleEntry<String, Object> name_and_value) {
        if (rowKey >= table.size() || deletedKeys.contains(rowKey)) {
            return false;
        }
        if (!names_to_index.containsKey(name_and_value.getKey())) {
            return false;
        }
        List<Object> row = table.get(rowKey);
        Object value = name_and_value.getValue();
        Object old_value = row.set(names_to_index.get(name_and_value.getKey()), value);

        // System.out.println(table.get(rowKey).get(names_to_index.get(name_and_value.getKey())));
        SortedMap<Object, Set<Integer>> index_map = value_to_index_maps.get(name_and_value.getKey());
        index_map.get(old_value).remove(rowKey);
        if (!index_map.containsKey(value)) {
            index_map.put(value, new HashSet<Integer>());
        }
        index_map.get(value).add(rowKey);
        return true;
    }

    // Implements (6) Get All rows from a table, i.e.
    // "SELECT * FROM tableName"
    public List<List<SimpleEntry<String, Object>>> GetAllRows() {
        return GetAllRowsLimitCount(table.size() - deletedKeys.size());
    }

    // Implements (7) Get specific count of rows from a table, i.e.
    // "SELECT * FROM tableName LIMIT count"
    public List<List<SimpleEntry<String, Object>>> GetAllRowsLimitCount(int limit) {
        return GetRowsLimitCount(limit, null);
    }

    // Implements (8) SORT by a column, and GET specific count of rows from table, i.e.
    // "SELECT * FROM tableName ORDER by column_name LIMIT count"
    public List<List<SimpleEntry<String, Object>>> GetSortedRowsLimitCount(int limit, String column_name) {
        ValueIndexMapsIterator itr = new ValueIndexMapsIterator(value_to_index_maps, column_name);
        return GetRowsLimitCount(limit, itr);
    }

    // Implements (9) GroupBy a column, and GET specific count of rows from table, i.e.
    // "SELECT aggregate_key, COUNT(*) FROM table GROUP BY aggregate_key"
    public List<SimpleEntry<Object, Integer>> GetGroupByCounts(String column_name) {
        List<SimpleEntry<Object, Integer>> retList = new ArrayList<>();
        SortedMap<Object, Set<Integer>> index_map = value_to_index_maps.get(column_name);
        for (Map.Entry<Object, Set<Integer>> value_and_indices : index_map.entrySet()) {
            retList.add(new SimpleEntry<>(value_and_indices.getKey(), value_and_indices.getValue().size()));
        }
        return retList;
    }

    // Helper method that has real implementation of (6), (7), (8).
    private List<List<SimpleEntry<String, Object>>> GetRowsLimitCount(int limit,
                                                                      ValueIndexMapsIterator itr) {
        List<List<SimpleEntry<String, Object>>> returned_rows = new ArrayList<>();
        int total = 0;
        int index = -1;
        while (total < limit) {
            // If an iterator was provided, use it. Else simply iterate over primary key.
            if (itr != null) {
                if (itr.hasNext()) {
                    index = itr.getNextRowKey();
                } else {
                    break;
                }
            } else {
                ++index;
                if (index == table.size()) {
                    break;
                }
            }

            if (!deletedKeys.contains(index)) {
                total++;
                List<Object> row = table.get(index);
                List<SimpleEntry<String, Object>> returned_row = new ArrayList<>();
                for (String column_name : names_to_index.keySet()) {
                    Object value = row.get(names_to_index.get(column_name));
                    returned_row.add(new SimpleEntry<>(column_name, value));
                }
                returned_rows.add(returned_row);
            }
        }
        return returned_rows;
    }

    // Helper class to iterate over rows in order of the sorted values of a column.
    private static class ValueIndexMapsIterator {
        public ValueIndexMapsIterator(Map<String, SortedMap<Object, Set<Integer>>> value_to_index_maps,
                                      String column_name) {
            index_sets = value_to_index_maps.get(column_name).values();
            outer_itr = index_sets.iterator();
            inner_itr = null;
        }

        int getNextRowKey() {
            if (inner_itr != null && inner_itr.hasNext())
                return inner_itr.next();
            if (outer_itr.hasNext()) {
                inner_itr = outer_itr.next().iterator();
                return inner_itr.next();
            }
            return -1;
        }

        boolean hasNext() {
            return (inner_itr != null && inner_itr.hasNext()) || outer_itr.hasNext();
        }

        private Collection<Set<Integer>> index_sets;
        private Iterator<Set<Integer>> outer_itr;
        private Iterator<Integer> inner_itr;
    }

    // Data Members.
    private Map<String, String> names_to_types;
    private Map<String, Integer> names_to_index;
    private List<List<Object>> table;
    private Set<Integer> deletedKeys;
    private Map<String, SortedMap<Object, Set<Integer>>> value_to_index_maps;
}
