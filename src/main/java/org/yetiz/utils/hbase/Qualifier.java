package org.yetiz.utils.hbase;

import java.lang.annotation.*;

/**
 * Created by yeti on 16/4/5.
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD})
public @interface Qualifier {

	String qualifier();

	String description();
}
