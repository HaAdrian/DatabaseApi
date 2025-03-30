package tech.hardenacke.database.implementation;

import com.google.gson.Gson;
import tech.hardenacke.database.DatabaseApi;
import tech.hardenacke.database.utils.Column;
import tech.hardenacke.database.utils.DatabaseObject;

import java.io.*;
import java.lang.reflect.Field;
import java.sql.*;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Copyright (c) 2025 by HaAdrian to present. All rights reserved.
 * Created: 30.03.2025 - 21:23
 *
 * @author HaAdrian
 */
public class DatabaseApiSimpleImplementation implements DatabaseApi {
    private final String url;
    private final String user;
    private final String password;
    private final String prefix;
    private final Map<Class<?>, String> sqlTypeMapping;
    private final Map<Class<?>, Function<Object, String>> columnTypeMapping;

    public DatabaseApiSimpleImplementation(String host, String port, String database, String user, String password, String prefix) {
        this.url = "jdbc:mysql://" + host + ":" + port + "/" + database;
        this.user = user;
        this.password = password;
        this.prefix = prefix;
        this.columnTypeMapping = new HashMap<>();
        this.sqlTypeMapping = new HashMap<>();
        sqlTypeMapping.put(int.class, "INT");
        sqlTypeMapping.put(Integer.class, "INT");
        sqlTypeMapping.put(long.class, "BIGINT");
        sqlTypeMapping.put(Long.class, "BIGINT");
        sqlTypeMapping.put(double.class, "DOUBLE");
        sqlTypeMapping.put(Double.class, "DOUBLE");
        sqlTypeMapping.put(boolean.class, "BOOLEAN");
        sqlTypeMapping.put(Boolean.class, "BOOLEAN");
    }

    @Override
    public Connection getConnection() {
        try {
            return DriverManager.getConnection(url, user, password);
        } catch (SQLException e) {
            throw new RuntimeException("Error connecting to the database", e);
        }
    }

    @Override
    public void createTable(Class<?> clazz) {
        if (!clazz.isAnnotationPresent(DatabaseObject.class))
            throw new IllegalArgumentException("Object is not annotated with tech.hardenacke.database.utils.DatabaseObject");

        List<Field> fields = computeColumnFields(clazz);
        List<String> columnDefinitions = new ArrayList<>();
        List<String> primaryKeys = new ArrayList<>();

        for (Field field : fields) {
            Column column = field.getAnnotation(Column.class);
            String colName = column.value();

            String sqlType = getSqlType(field);
            StringBuilder columnDef = new StringBuilder();
            columnDef.append(colName).append(" ").append(sqlType);

            if (column.primary()) primaryKeys.add(colName);
            if (column.autoIncrement()) {
                if (!sqlType.equals("INT") && !sqlType.equals("BIGINT"))
                    throw new IllegalArgumentException("AUTO_INCREMENT kann nur bei numerischen Feldern (INT/BIGINT) verwendet werden: " + field.getName());
                columnDef.append(" AUTO_INCREMENT");
            }
            columnDefinitions.add(columnDef.toString());
        }

        String tableName = prefix + clazz.getAnnotation(DatabaseObject.class).table();

        StringBuilder sql = new StringBuilder();
        sql.append("CREATE TABLE IF NOT EXISTS ").append(tableName).append(" (");
        sql.append(String.join(", ", columnDefinitions));

        if (!primaryKeys.isEmpty()) {
            sql.append(", PRIMARY KEY (").append(String.join(", ", primaryKeys)).append(")");
        }
        sql.append(");");

        executeUpdate(sql.toString(), null, fields);
    }

    @Override
    public void insert(Object object) {
        Class<?> clazz = object.getClass();
        if (!clazz.isAnnotationPresent(DatabaseObject.class))
            throw new IllegalArgumentException("Object is not annotated with tech.hardenacke.database.utils.DatabaseObject");

        List<Field> insertFields = computeColumnFields(clazz).stream().filter(field -> !field.getAnnotation(Column.class).autoIncrement()).toList();

        String columnNames = computeColumnNames(insertFields);

        String placeholders = computePlaceHolders(insertFields);

        String tableName = prefix + clazz.getAnnotation(DatabaseObject.class).table();

        String sql = "INSERT INTO " + tableName + " (" + columnNames + ") VALUES (" + placeholders + ")";
        executeUpdate(sql, object, insertFields);
    }

    @Override
    public void executeUpdate(String sql, Object object, List<Field> params) {
        try (Connection conn = getConnection(); PreparedStatement statement = conn.prepareStatement(sql)) {
            if (object != null) {
                for (int i = 0; i < params.size(); i++) {
                    Field field = params.get(i);
                    field.setAccessible(true);
                    Object fieldValue = field.get(object);
                    if (columnTypeMapping.containsKey(field.getType())) {
                        statement.setObject(i + 1, columnTypeMapping.get(field.getType()).apply(fieldValue));
                        continue;
                    }
                    if (field.getType() == boolean.class || field.getType() == Boolean.class) {
                        boolean boolValue = (Boolean) fieldValue;
                        statement.setInt(i + 1, boolValue ? 1 : 0);
                    } else if (field.getType() == Map.class || field.getType() == HashMap.class) {
                        Map<Object, Object> mapValue = (Map<Object, Object>) fieldValue;
                        statement.setString(i + 1, encodeHashMapToBase64(mapValue));
                    } else if (field.getType() == String.class || field.getType().isEnum()) {
                        String value = String.valueOf(fieldValue);
                        statement.setObject(i + 1, value);
                    } else {
                        String value = new Gson().toJson(fieldValue);
                        statement.setObject(i + 1, value);
                    }
                }
            }
            statement.executeUpdate();
        } catch (SQLException | IllegalAccessException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void update(Object object) {
        Class<?> clazz = object.getClass();
        if (!clazz.isAnnotationPresent(DatabaseObject.class))
            throw new IllegalArgumentException("Object is not annotated with tech.hardenacke.database.utils.DatabaseObject");

        List<Field> fields = computeColumnFields(clazz);

        List<String> updateColumns = new ArrayList<>();
        List<Field> updateFields = new ArrayList<>();
        List<String> primaryConditions = new ArrayList<>();
        List<Field> primaryFields = new ArrayList<>();

        for (Field field : fields) {
            Column column = field.getAnnotation(Column.class);
            String colName = column.value();

            if (column.primary()) {
                primaryConditions.add(colName + " = ?");
                primaryFields.add(field);
            } else if (!column.autoIncrement()) {
                updateColumns.add(colName + " = ?");
                updateFields.add(field);
            }
        }

        if (primaryConditions.isEmpty())
            return;

        String tableName = prefix + clazz.getAnnotation(DatabaseObject.class).table();

        String sql = "UPDATE " + tableName + " SET " + String.join(", ", updateColumns) + " WHERE " + String.join(" AND ", primaryConditions);

        List<Field> orderedParams = new ArrayList<>();
        orderedParams.addAll(updateFields);
        orderedParams.addAll(primaryFields);

        executeUpdate(sql, object, orderedParams);
    }

    @Override
    public void delete(Object object) {
        Class<?> clazz = object.getClass();
        if (!clazz.isAnnotationPresent(DatabaseObject.class))
            throw new IllegalArgumentException("Object is not annotated with tech.hardenacke.database.utils.DatabaseObject");

        List<Field> fields = computeColumnFields(clazz);

        List<String> primaryConditions = new ArrayList<>();
        for (Field field : fields) {
            Column column = field.getAnnotation(Column.class);
            if (column.primary()) {
                primaryConditions.add(column.value() + " = ?");
            }
        }

        if (primaryConditions.isEmpty()) {
            throw new IllegalArgumentException("Es muss mindestens ein Primary Key definiert sein, um den DELETE-Befehl auszuführen.");
        }

        List<Field> primaryFields = fields.stream().filter(f -> f.getAnnotation(Column.class).primary()).collect(Collectors.toList());

        String tableName = prefix + clazz.getAnnotation(DatabaseObject.class).table();
        String sql = "DELETE FROM " + tableName + " WHERE " + String.join(" AND ", primaryConditions);
        executeUpdate(sql, object, primaryFields);
    }

    @Override
    public <T> List<T> selectAll(Class<?> clazz) {
        if (!clazz.isAnnotationPresent(DatabaseObject.class))
            throw new IllegalArgumentException("Object is not annotated with tech.hardenacke.database.utils.DatabaseObject");
        return selectAllWithCondition(clazz, Collections.emptyMap());
    }

    @Override
    public <T> List<T> selectAllWithCondition(Class<?> clazz, Map<String, Object> conditions) {
        if (!clazz.isAnnotationPresent(DatabaseObject.class))
            throw new IllegalArgumentException("Object is not annotated with tech.hardenacke.database.utils.DatabaseObject");

        List<Field> fields = computeColumnFields(clazz);
        String tableName = prefix + clazz.getAnnotation(DatabaseObject.class).table();
        StringBuilder sqlBuilder = new StringBuilder("SELECT * FROM " + tableName);

        List<Object> params = new ArrayList<>();
        if (conditions != null && !conditions.isEmpty()) {
            sqlBuilder.append(" WHERE ");
            List<String> condList = new ArrayList<>();
            for (Map.Entry<String, Object> entry : conditions.entrySet()) {
                condList.add(entry.getKey() + " = ?");
                params.add(entry.getValue());
            }
            sqlBuilder.append(String.join(" AND ", condList));
        }

        String sql = sqlBuilder.toString();
        List<T> resultList = new ArrayList<>();

        try (Connection con = getConnection(); PreparedStatement statement = con.prepareStatement(sql)) {

            for (int i = 0; i < params.size(); i++)
                statement.setObject(i + 1, params.get(i));

            try (ResultSet rs = statement.executeQuery()) {
                while (rs.next()) {
                    @SuppressWarnings("unchecked") T instance = (T) clazz.getDeclaredConstructor().newInstance();
                    for (Field field : fields) {
                        field.setAccessible(true);
                        String colName = field.getAnnotation(Column.class).value();
                        Object value = rs.getObject(colName);

                        if (value != null && !field.getType().isAssignableFrom(value.getClass())) {
                            if (columnTypeMapping.containsKey(field.getType())) {
                                value = columnTypeMapping.get(field.getType()).apply(value.toString());
                            } else if (field.getType().isEnum()) {
                                value = Enum.valueOf((Class<Enum>) field.getType(), value.toString());
                            } else if (field.getType().equals(UUID.class)) {
                                value = UUID.fromString(value.toString());
                            } else if (field.getType().equals(Map.class)) {
                                value = decodeBase64ToHashMap(value.toString());
                            } else if (!field.getType().equals(String.class)) {
                                value = new Gson().fromJson(value.toString(), field.getType());
                            }
                        }

                        field.set(instance, value);
                    }
                    resultList.add(instance);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return resultList;
    }

    @Override
    public <T> T select(Class<?> clazz) {
        if (!clazz.isAnnotationPresent(DatabaseObject.class))
            throw new IllegalArgumentException("Object is not annotated with tech.hardenacke.database.utils.DatabaseObject");
        return selectWithCondition(clazz, Collections.emptyMap());
    }

    @Override
    public <V> V selectValue(Class<?> clazz, String columnName, Map<String, Object> conditions) {
        if (!clazz.isAnnotationPresent(DatabaseObject.class))
            throw new IllegalArgumentException("Object is not annotated with tech.hardenacke.database.utils.DatabaseObject");

        String tableName = prefix + clazz.getAnnotation(DatabaseObject.class).table();

        StringBuilder sqlBuilder = new StringBuilder("SELECT " + columnName + " FROM " + tableName);

        List<Object> params = new ArrayList<>();
        if (conditions != null && !conditions.isEmpty()) {
            sqlBuilder.append(" WHERE ");
            List<String> condList = new ArrayList<>();
            for (Map.Entry<String, Object> entry : conditions.entrySet()) {
                condList.add(entry.getKey() + " = ?");
                params.add(entry.getValue());
            }
            sqlBuilder.append(String.join(" AND ", condList));
        }

        String sql = sqlBuilder.toString();

        try (Connection con = getConnection(); PreparedStatement statement = con.prepareStatement(sql)) {

            for (int i = 0; i < params.size(); i++) {
                statement.setObject(i + 1, params.get(i));
            }

            try (ResultSet rs = statement.executeQuery()) {
                if (rs.next()) return (V) rs.getObject(columnName);
            }
        } catch (Exception e) {
            throw new RuntimeException("Fehler beim SELECT-Wert: " + e.getMessage(), e);
        }
        return null;
    }

    @Override
    public <T> T selectWithCondition(Class<?> clazz, Map<String, Object> conditions) {
        if (!clazz.isAnnotationPresent(DatabaseObject.class))
            throw new IllegalArgumentException("Object is not annotated with tech.hardenacke.database.utils.DatabaseObject");
        return (T) selectAllWithCondition(clazz, conditions).stream().findFirst().orElse(null);
    }

    @Override
    public boolean existsWithCondition(Class<?> clazz, Map<String, Object> conditions) {
        if (!clazz.isAnnotationPresent(DatabaseObject.class))
            throw new IllegalArgumentException("Object is not annotated with tech.hardenacke.database.utils.DatabaseObject");

        String tableName = prefix + clazz.getAnnotation(DatabaseObject.class).table();
        StringBuilder sqlBuilder = new StringBuilder("SELECT 1 FROM " + tableName);

        List<Object> params = new ArrayList<>();
        if (conditions != null && !conditions.isEmpty()) {
            sqlBuilder.append(" WHERE ");
            List<String> condList = new ArrayList<>();
            for (Map.Entry<String, Object> entry : conditions.entrySet()) {
                condList.add(entry.getKey() + " = ?");
                params.add(entry.getValue());
            }
            sqlBuilder.append(String.join(" AND ", condList));
        }

        sqlBuilder.append(" LIMIT 1");
        try (Connection con = getConnection(); PreparedStatement statement = con.prepareStatement(sqlBuilder.toString())) {

            for (int i = 0; i < params.size(); i++)
                statement.setObject(i + 1, params.get(i));

            try (ResultSet rs = statement.executeQuery()) {
                return rs.next();
            }
        } catch (Exception e) {
            throw new RuntimeException("Fehler beim existsWithCondition: " + e.getMessage(), e);
        }
    }

    @Override
    public <T> boolean exists(Object object) {
        Class<?> clazz = object.getClass();
        if (!clazz.isAnnotationPresent(DatabaseObject.class))
            throw new IllegalArgumentException("Object is not annotated with tech.hardenacke.database.utils.DatabaseObject");

        List<Field> fields = computeColumnFields(clazz);
        List<String> primaryConditions = new ArrayList<>();
        List<Object> params = new ArrayList<>();

        for (Field field : fields) {
            Column column = field.getAnnotation(Column.class);
            if (column.primary()) {
                primaryConditions.add(column.value() + " = ?");
                field.setAccessible(true);
                try {
                    params.add(field.get(object));
                } catch (IllegalAccessException e) {
                    throw new RuntimeException("Fehler beim Zugriff auf Feld: " + field.getName(), e);
                }
            }
        }

        if (primaryConditions.isEmpty())
            throw new IllegalArgumentException("Es muss mindestens ein Primary Key definiert sein, um den Existenzcheck durchzuführen.");

        String tableName = prefix + clazz.getAnnotation(DatabaseObject.class).table();
        String sql = "SELECT 1 FROM " + tableName + " WHERE " + String.join(" AND ", primaryConditions) + " LIMIT 1";

        try (Connection con = getConnection(); PreparedStatement statement = con.prepareStatement(sql)) {

            for (int i = 0; i < params.size(); i++)
                statement.setObject(i + 1, params.get(i));

            try (ResultSet rs = statement.executeQuery()) {
                return rs.next();
            }
        } catch (Exception e) {
            throw new RuntimeException("Fehler beim Existenzcheck: " + e.getMessage(), e);
        }
    }

    @Override
    public Map<Class<?>, String> getSqlTypeMapping() {
        return sqlTypeMapping;
    }

    @Override
    public Map<Class<?>, Function<Object, String>> getColumnTypeMapping() {
        return columnTypeMapping;
    }

    private List<Field> computeColumnFields(Class<?> clazz) {
        return Arrays.stream(clazz.getDeclaredFields()).filter(field -> field.isAnnotationPresent(Column.class)).toList();
    }

    private String computeColumnNames(List<Field> fields) {
        return fields.stream().map(field -> field.getAnnotation(Column.class).value()).collect(Collectors.joining(", "));
    }

    private String computePlaceHolders(List<Field> fields) {
        return fields.stream().map(field -> "?").collect(Collectors.joining(", "));
    }

    private String getSqlType(Field field) {
        return sqlTypeMapping.getOrDefault(field.getType(), "VARCHAR(255)");
    }

    public String encodeHashMapToBase64(Map<Object, Object> map) {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        try {
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
            objectOutputStream.writeObject(map);
            objectOutputStream.flush();

            byte[] mapBytes = byteArrayOutputStream.toByteArray();

            return Base64.getEncoder().encodeToString(mapBytes);
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    public Map<Object, Object> decodeBase64ToHashMap(String base64String) throws IOException, ClassNotFoundException {
        byte[] mapBytes = Base64.getDecoder().decode(base64String);

        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(mapBytes);
        ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream);
        return (Map<Object, Object>) objectInputStream.readObject();
    }
}
