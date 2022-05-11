package org.apache.dolphinscheduler.plugin.task.datax.datasource.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.collections.CollectionUtils;
import org.apache.dolphinscheduler.plugin.datasource.api.datasource.starrocks.StarRocksConnectionParam;
import org.apache.dolphinscheduler.plugin.datasource.api.utils.DatasourceUtil;
import org.apache.dolphinscheduler.plugin.task.datax.DataxParameters;
import org.apache.dolphinscheduler.plugin.task.datax.DataxUtils;
import org.apache.dolphinscheduler.spi.datasource.BaseConnectionParam;
import org.apache.dolphinscheduler.spi.enums.DbType;
import org.apache.dolphinscheduler.spi.task.request.DataxTaskExecutionContext;
import org.apache.dolphinscheduler.spi.task.request.TaskRequest;
import org.apache.dolphinscheduler.spi.utils.JSONUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.apache.dolphinscheduler.plugin.datasource.api.utils.PasswordUtils.decodePassword;

public class StarRocksBuildDataXJob extends AbsDataSourceBuildDataXJob {

    @Override
    public List<ObjectNode> buildDataxJobContentJson(TaskRequest taskExecutionContext, DataxParameters dataXParameters) {

        DataxTaskExecutionContext dataxTaskExecutionContext = taskExecutionContext.getDataxTaskExecutionContext();
        BaseConnectionParam dataSourceCfg = (BaseConnectionParam) DatasourceUtil.buildConnectionParams(
                DbType.of(dataxTaskExecutionContext.getSourcetype()),
                dataxTaskExecutionContext.getSourceConnectionParams());

        StarRocksConnectionParam dataTargetCfg = (StarRocksConnectionParam) DatasourceUtil.buildConnectionParams(
                DbType.of(dataxTaskExecutionContext.getTargetType()),
                dataxTaskExecutionContext.getTargetConnectionParams());

        List<ObjectNode> readerConnArr = new ArrayList<>();
        ObjectNode readerConn = JSONUtils.createObjectNode();

        ArrayNode sqlArr = readerConn.putArray("querySql");
        for (String sql : new String[]{dataXParameters.getSql()}) {
            sqlArr.add(sql);
        }

        ArrayNode urlArr = readerConn.putArray("jdbcUrl");
        urlArr.add(DatasourceUtil.getJdbcUrl(DbType.valueOf(dataXParameters.getDsType()), dataSourceCfg));

        readerConnArr.add(readerConn);

        ObjectNode readerParam = JSONUtils.createObjectNode();
        readerParam.put("username", dataSourceCfg.getUser());
        readerParam.put("password", decodePassword(dataSourceCfg.getPassword()));
        readerParam.putArray("connection").addAll(readerConnArr);

        ObjectNode reader = JSONUtils.createObjectNode();
        reader.put("name", DataxUtils.getReaderPluginName(DbType.of(dataxTaskExecutionContext.getSourcetype())));
        reader.set("parameter", readerParam);

        ObjectNode writerParam = JSONUtils.createObjectNode();
        writerParam.put("username", dataTargetCfg.getUser());
        writerParam.put("password", decodePassword(dataTargetCfg.getPassword()));
        writerParam.put("database", dataTargetCfg.getDatabase());

        String[] columns = parsingSqlColumnNames(DbType.of(dataxTaskExecutionContext.getSourcetype()),
                DbType.of(dataxTaskExecutionContext.getTargetType()),
                dataSourceCfg, dataXParameters.getSql());

        ArrayNode columnArr = writerParam.putArray("column");
        for (String column : columns) {
            columnArr.add(column);
        }

        writerParam.put("table", dataXParameters.getTargetTable());
        writerParam.put("jdbcUrl", DatasourceUtil.getJdbcUrl(DbType.valueOf(dataXParameters.getDtType()), dataTargetCfg));
        ArrayNode loadUrlArray = writerParam.putArray("loadUrl");
        ObjectMapper objectMapper = new ObjectMapper();
        List<String> loadUrlList;
        try {
            loadUrlList = objectMapper.readValue(dataTargetCfg.getLoadUrl(), List.class);
            loadUrlList.stream().forEach(loadUrl -> loadUrlArray.add(loadUrl));
        } catch (JsonProcessingException e) {
            logger.error(e.getMessage());
            loadUrlList = new ArrayList();
        }
        writerParam.putPOJO("loadProps", new HashMap<>());

        if (CollectionUtils.isNotEmpty(dataXParameters.getPreStatements())) {
            ArrayNode preSqlArr = writerParam.putArray("preSql");
            for (String preSql : dataXParameters.getPreStatements()) {
                preSqlArr.add(preSql);
            }

        }

        if (CollectionUtils.isNotEmpty(dataXParameters.getPostStatements())) {
            ArrayNode postSqlArr = writerParam.putArray("postSql");
            for (String postSql : dataXParameters.getPostStatements()) {
                postSqlArr.add(postSql);
            }
        }

        ObjectNode writer = JSONUtils.createObjectNode();
        writer.put("name", DataxUtils.getWriterPluginName(DbType.of(dataxTaskExecutionContext.getTargetType())));
        writer.set("parameter", writerParam);

        List<ObjectNode> contentList = new ArrayList<>();
        ObjectNode content = JSONUtils.createObjectNode();
        content.set("reader", reader);
        content.set("writer", writer);
        contentList.add(content);

        return contentList;
    }
}
