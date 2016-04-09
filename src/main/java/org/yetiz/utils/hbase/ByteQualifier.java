package org.yetiz.utils.hbase;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Created by yeti on 16/4/5.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface ByteQualifier {
	byte[] family();

	byte[] qualifier();

	String description();
}
