package com.alibaba.otter.node.etl.load.loader.common;


import com.alibaba.otter.shared.common.model.config.data.DataMediaType;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * load action 处理类注解
 *
 * @see DataBatchLoader
 */
@Target(TYPE)
@Retention(RUNTIME)
@Documented
public @interface LoadAction {

    /**
     * actionName 需要与被注解的类在spring中配置的id一致
     * @return
     */
    String actionName() default "";

    /**
     * actionClass
     */
    //Class<?> actionClass();

    /**
     * 处理的数据类型
     * @return
     */
    DataMediaType[] mediaType() default {};
}
