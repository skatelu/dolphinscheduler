package org.apache.dolphinscheduler.plugin.task.datax.datasource.impl;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.statement.*;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.collections.CollectionUtils;
import org.apache.dolphinscheduler.plugin.datasource.api.plugin.DataSourceClientProvider;
import org.apache.dolphinscheduler.plugin.datasource.api.utils.DatasourceUtil;
import org.apache.dolphinscheduler.plugin.task.datax.DataxParameters;
import org.apache.dolphinscheduler.plugin.task.datax.DataxUtils;
import org.apache.dolphinscheduler.plugin.task.datax.datasource.DataSourceBuildDataXJob;
import org.apache.dolphinscheduler.spi.datasource.BaseConnectionParam;
import org.apache.dolphinscheduler.spi.enums.DbType;
import org.apache.dolphinscheduler.spi.task.TaskConstants;
import org.apache.dolphinscheduler.spi.task.request.DataxTaskExecutionContext;
import org.apache.dolphinscheduler.spi.task.request.TaskRequest;
import org.apache.dolphinscheduler.spi.utils.JSONUtils;
import org.apache.dolphinscheduler.spi.utils.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static org.apache.dolphinscheduler.plugin.datasource.api.utils.PasswordUtils.decodePassword;

public abstract class AbsDataSourceBuildDataXJob implements DataSourceBuildDataXJob {
    protected final Logger logger = LoggerFactory.getLogger(String.format(TaskConstants.TASK_LOG_LOGGER_NAME_FORMAT, getClass()));


    @Override
    public List<ObjectNode> buildDataxJobContentJson(TaskRequest taskExecutionContext, DataxParameters dataXParameters) {

        DataxTaskExecutionContext dataxTaskExecutionContext = taskExecutionContext.getDataxTaskExecutionContext();
        BaseConnectionParam dataSourceCfg = (BaseConnectionParam) DatasourceUtil.buildConnectionParams(
                DbType.of(dataxTaskExecutionContext.getSourcetype()),
                dataxTaskExecutionContext.getSourceConnectionParams());

        BaseConnectionParam dataTargetCfg = (BaseConnectionParam) DatasourceUtil.buildConnectionParams(
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

        List<ObjectNode> writerConnArr = new ArrayList<>();
        ObjectNode writerConn = JSONUtils.createObjectNode();
        ArrayNode tableArr = writerConn.putArray("table");
        tableArr.add(dataXParameters.getTargetTable());

        writerConn.put("jdbcUrl", DatasourceUtil.getJdbcUrl(DbType.valueOf(dataXParameters.getDtType()), dataTargetCfg));
        writerConnArr.add(writerConn);

        ObjectNode writerParam = JSONUtils.createObjectNode();
        writerParam.put("username", dataTargetCfg.getUser());
        writerParam.put("password", decodePassword(dataTargetCfg.getPassword()));
        // TODO 在此处添加 Mysql 写入方式
        if (StringUtils.isNotBlank(dataXParameters.getWriteMode())) {
            writerParam.put("writeMode", dataXParameters.getWriteMode());
        }
//        writerParam.put("writeMode",)

        String[] columns = parsingSqlColumnNames(DbType.of(dataxTaskExecutionContext.getSourcetype()),
                DbType.of(dataxTaskExecutionContext.getTargetType()),
                dataSourceCfg, dataXParameters.getSql());

        ArrayNode columnArr = writerParam.putArray("column");
        for (String column : columns) {
            columnArr.add(column);
        }
        writerParam.putArray("connection").addAll(writerConnArr);

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


    /**
     * parsing synchronized column names in SQL statements
     *
     * @param sourceType the database type of the data source
     * @param targetType the database type of the data target
     * @param dataSourceCfg the database connection parameters of the data source
     * @param sql sql for data synchronization
     * @return Keyword converted column names
     */
    protected String[] parsingSqlColumnNames(DbType sourceType, DbType targetType, BaseConnectionParam dataSourceCfg, String sql) {
        String[] columnNames = tryGrammaticalAnalysisSqlColumnNames(sourceType, sql);

        if (columnNames == null || columnNames.length == 0) {
            logger.info("try to execute sql analysis query column name");
            columnNames = tryExecuteSqlResolveColumnNames(sourceType, dataSourceCfg, sql);
        }

        notNull(columnNames, String.format("parsing sql columns failed : %s", sql));

        return DataxUtils.convertKeywordsColumns(targetType, columnNames);
    }


    /**
     * try grammatical parsing column
     *
     * @param dbType database type
     * @param sql sql for data synchronization
     * @return column name array
     * @throws RuntimeException if error throws RuntimeException
     */
    private String[] tryGrammaticalAnalysisSqlColumnNames(DbType dbType, String sql) {
        String[] columnNames;

        try {
            SQLStatementParser parser = DataxUtils.getSqlStatementParser(dbType, sql);
            if (parser == null) {
                logger.warn("database driver [{}] is not support grammatical analysis sql", dbType);
                return new String[0];
            }

            SQLStatement sqlStatement = parser.parseStatement();
            SQLSelectStatement sqlSelectStatement = (SQLSelectStatement) sqlStatement;
            SQLSelect sqlSelect = sqlSelectStatement.getSelect();

            List<SQLSelectItem> selectItemList = null;
            if (sqlSelect.getQuery() instanceof SQLSelectQueryBlock) {
                SQLSelectQueryBlock block = (SQLSelectQueryBlock) sqlSelect.getQuery();
                selectItemList = block.getSelectList();
            } else if (sqlSelect.getQuery() instanceof SQLUnionQuery) {
                SQLUnionQuery unionQuery = (SQLUnionQuery) sqlSelect.getQuery();
                SQLSelectQueryBlock block = (SQLSelectQueryBlock) unionQuery.getRight();
                selectItemList = block.getSelectList();
            }

            notNull(selectItemList,
                    String.format("select query type [%s] is not support", sqlSelect.getQuery().toString()));

            columnNames = new String[selectItemList.size()];
            for (int i = 0; i < selectItemList.size(); i++) {
                SQLSelectItem item = selectItemList.get(i);

                String columnName = null;

                if (item.getAlias() != null) {
                    columnName = item.getAlias();
                } else if (item.getExpr() != null) {
                    if (item.getExpr() instanceof SQLPropertyExpr) {
                        SQLPropertyExpr expr = (SQLPropertyExpr) item.getExpr();
                        columnName = expr.getName();
                    } else if (item.getExpr() instanceof SQLIdentifierExpr) {
                        SQLIdentifierExpr expr = (SQLIdentifierExpr) item.getExpr();
                        columnName = expr.getName();
                    }
                } else {
                    throw new RuntimeException(
                            String.format("grammatical analysis sql column [ %s ] failed", item.toString()));
                }

                if (columnName == null) {
                    throw new RuntimeException(
                            String.format("grammatical analysis sql column [ %s ] failed", item.toString()));
                }

                columnNames[i] = columnName;
            }
        } catch (Exception e) {
            logger.warn(e.getMessage(), e);
            return new String[0];
        }

        return columnNames;
    }


    /**
     * try to execute sql to resolve column names
     *
     * @param baseDataSource the database connection parameters
     * @param sql sql for data synchronization
     * @return column name array
     */
    public String[] tryExecuteSqlResolveColumnNames(DbType sourceType, BaseConnectionParam baseDataSource, String sql) {
        String[] columnNames;
        sql = String.format("SELECT t.* FROM ( %s ) t WHERE 0 = 1", sql);
        sql = sql.replace(";", "");

        try (
                Connection connection = DataSourceClientProvider.getInstance().getConnection(sourceType, baseDataSource);
                PreparedStatement stmt = connection.prepareStatement(sql);
                ResultSet resultSet = stmt.executeQuery()) {

            ResultSetMetaData md = resultSet.getMetaData();
            int num = md.getColumnCount();
            columnNames = new String[num];
            for (int i = 1; i <= num; i++) {
                columnNames[i - 1] = md.getColumnName(i);
            }
        } catch (SQLException e) {
            logger.warn(e.getMessage(), e);
            return null;
        }

        return columnNames;
    }

    private void notNull(Object obj, String message) {
        if (obj == null) {
            throw new RuntimeException(message);
        }
    }
}
