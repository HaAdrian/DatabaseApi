package tech.hardenacke.database.utils;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Copyright (c) 2025 by HaAdrian to present. All rights reserved.
 * Created: 30.03.2025 - 21:21
 *
 * @author HaAdrian
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface DatabaseObject {
    String table();
}
