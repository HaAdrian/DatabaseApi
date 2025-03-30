package tech.hardenacke.database;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Copyright (c) 2025 by HaAdrian to present. All rights reserved.
 * Created: 30.03.2025 - 21:22
 *
 * @author HaAdrian
 */
public interface DatabaseApi {
    Connection getConnection();

    void createTable(Class<?> clazz);

    void insert(Object object);

    void update(Object object);

    void delete(Object object);

    void executeUpdate(String sql, Object object, List<Field> params);

    <T> List<T> selectAll(Class<?> clazz);

    <T> List<T> selectAllWithCondition(Class<?> clazz, Map<String, Object> conditions);

    <T> T select(Class<?> clazz);

    <T> T selectValue(Class<?> clazz, String columnName, Map<String, Object> conditions);

    <T> T selectWithCondition(Class<?> clazz, Map<String, Object> conditions);

    boolean existsWithCondition(Class<?> clazz, Map<String, Object> conditions);

    <T> boolean exists(Object object);

    Map<Class<?>, String> getSqlTypeMapping();

    Map<Class<?>, Function<Object, String>> getColumnTypeMapping();
}
