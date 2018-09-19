package com.alibaba.otter.node.etl.load.loader.hdfs;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.node.etl.load.exception.LoadException;
import com.alibaba.otter.shared.etl.model.EventColumn;
import com.alibaba.otter.shared.etl.model.EventData;
import com.alibaba.otter.shared.etl.model.EventType;
import org.apache.commons.lang.time.DateFormatUtils;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * hdfs load operation
 * @author tangzhongyuan 2018-09-10 16:58
 **/
public class HdfsLoadDataUtil {
    
    /**
     * 冷数据存储路径
     * 0 : database name
     * 1 : currnet data (YYYYMMDD)
     * 2 : table name
     */
    public static final String COLD_DATA_PATH = "/user/edts/original/cold/{0}/{1}/{2}/cold.txt";

    /**
     * 增量数据存储路径
     * 0 : database name
     * 1 : currnet data (YYYYMMDD)
     * 2 : table name
     */
    public static final String ADD_DATA_PATH = "/user/edts/original/add/{0}/{1}/{2}/add.txt";

    /**
     * 约定的文件分隔符
     */
    private static final String SEP = "\001\002\n";

    private static final String TRUNCATE = EventType.TRUNCATE.getValue();
    private static final String UPDATE = EventType.UPDATE.getValue();
    private static final String INSERT = EventType.INSERT.getValue();
    private static final String DELETE = EventType.DELETE.getValue();

    private static final String TABLENAME = "TN";
    private static final String TIMESTAMP = "TS";

    public static String getTruncateData(String tableName) {
        JSONObject truncate = new JSONObject();
        JSONObject table = new JSONObject();
        truncate.put(TIMESTAMP, System.currentTimeMillis());
        truncate.put(TRUNCATE, table);
        table.put(TABLENAME, tableName);
        return truncate.toJSONString() + SEP;
    }

    private static String getInsertData(List<EventColumn> columns) {
        return commonLoadData(columns, INSERT);
    }

    private static String getDeleteData(List<EventColumn> columns) {
        return commonLoadData(columns, DELETE);
    }

    private static String getUpdateData(List<EventColumn> columns) {
        return commonLoadData(columns, UPDATE);
    }

    private static String commonLoadData(List<EventColumn> columns, String eventType) {
        JSONObject root = new JSONObject();
        JSONObject table = new JSONObject();
        root.put(TIMESTAMP, System.currentTimeMillis());
        root.put(eventType, table);
        for (EventColumn column : columns) {
            table.put(column.getColumnName(), column.getColumnValue());
        }
        return root.toJSONString() + SEP;
    }

    public static String prepareDMLLoadData(EventData data) {

        EventType type = data.getEventType();
        // 注意insert/update语句对应的字段数序都是将主键排在后面
        List<EventColumn> columns = new ArrayList<EventColumn>();
        if (type.isInsert()) {
            columns.addAll(data.getColumns()); // insert为所有字段
            columns.addAll(data.getKeys());
            return getInsertData(columns);
        }

        if (type.isDelete()) {
            columns.addAll(data.getKeys());
            return getDeleteData(columns);
        }

        if (type.isUpdate()) {
            columns.addAll(data.getColumns()); // insert为所有字段
            columns.addAll(data.getKeys());
            return getUpdateData(columns);
        }

        throw new LoadException("# ERROR not found this event type", type.getValue());
    }

    public static String batchprepareDMLLoadData(List<EventData> datas) {
        StringBuffer sb = new StringBuffer(1024);
        for (EventData data : datas) {
            String curLoadData = HdfsLoadDataUtil.prepareDMLLoadData(data);
            sb.append(curLoadData);
        }
        return sb.toString();
    }

    public static String coldDataLoadPath(EventData data) {
        return MessageFormat.format(COLD_DATA_PATH, data.getSchemaName(),
                DateFormatUtils.format(new Date(), "yyyyMMdd"), data.getTableName());
    }

    public static String addDataLoadPath(EventData data) {
        return MessageFormat.format(ADD_DATA_PATH, data.getSchemaName(),
                DateFormatUtils.format(new Date(), "yyyyMMdd"), data.getTableName());
    }

    public static String getDataLoadPath(EventData data) {
        if (data.isRemedy()) {
            return HdfsLoadDataUtil.coldDataLoadPath(data);
        }
        return HdfsLoadDataUtil.addDataLoadPath(data);
    }
}