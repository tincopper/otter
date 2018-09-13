package com.alibaba.otter.node.etl.load.loader.common;

/**
 * 数据过滤处理
 * @author tangzhongyuan
 * @create 2018-09-12 11:06
 **/
public interface LoadDataFilter<T> {

    void doFilter(T t);
}
