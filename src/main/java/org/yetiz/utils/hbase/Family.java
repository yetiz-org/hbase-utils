package org.yetiz.utils.hbase;

import org.apache.hadoop.hbase.io.compress.Compression;

import java.lang.annotation.*;

/**
 * Created by yeti on 2016/4/8.
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD})
public @interface Family {
	String family();

	Compression.Algorithm compression() default Compression.Algorithm.LZ4;
}
