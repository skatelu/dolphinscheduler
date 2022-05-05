package org.apache.dolphinscheduler.plugin.datasource.api.datasource.starrocks;

import org.apache.commons.collections4.MapUtils;
import org.apache.dolphinscheduler.plugin.datasource.api.datasource.AbstractDatasourceProcessor;
import org.apache.dolphinscheduler.plugin.datasource.api.datasource.BaseDataSourceParamDTO;
import org.apache.dolphinscheduler.plugin.datasource.api.utils.PasswordUtils;
import org.apache.dolphinscheduler.spi.datasource.BaseConnectionParam;
import org.apache.dolphinscheduler.spi.datasource.ConnectionParam;
import org.apache.dolphinscheduler.spi.enums.DbType;
import org.apache.dolphinscheduler.spi.utils.Constants;
import org.apache.dolphinscheduler.spi.utils.JSONUtils;
import org.apache.dolphinscheduler.spi.utils.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class StarRocksDatasourceProcessor extends AbstractDatasourceProcessor {
    private final Logger logger = LoggerFactory.getLogger(StarRocksDatasourceProcessor.class);

    private static final String ALLOW_LOAD_LOCAL_IN_FILE_NAME = "allowLoadLocalInfile";

    private static final String AUTO_DESERIALIZE = "autoDeserialize";

    private static final String ALLOW_LOCAL_IN_FILE_NAME = "allowLocalInfile";

    private static final String ALLOW_URL_IN_LOCAL_IN_FILE_NAME = "allowUrlInLocalInfile";

    private static final String APPEND_PARAMS = "allowLoadLocalInfile=false&autoDeserialize=false&allowLocalInfile=false&allowUrlInLocalInfile=false";

    @Override
    public BaseDataSourceParamDTO createDatasourceParamDTO(String connectionJson) {
        StarRocksConnectionParam connectionParams = (StarRocksConnectionParam) createConnectionParams(connectionJson);
        StarRocksDatasourceParamDTO starRocksDatasourceParamDTO = new StarRocksDatasourceParamDTO();

        starRocksDatasourceParamDTO.setUserName(connectionParams.getUser());
        starRocksDatasourceParamDTO.setDatabase(connectionParams.getDatabase());
        starRocksDatasourceParamDTO.setOther(parseOther(connectionParams.getOther()));

        String address = connectionParams.getAddress();
        String[] hostSeperator = address.split(Constants.DOUBLE_SLASH);
        String[] hostPortArray = hostSeperator[hostSeperator.length - 1].split(Constants.COMMA);
        starRocksDatasourceParamDTO.setPort(Integer.parseInt(hostPortArray[0].split(Constants.COLON)[1]));
        starRocksDatasourceParamDTO.setHost(hostPortArray[0].split(Constants.COLON)[0]);

        return starRocksDatasourceParamDTO;
    }

    @Override
    public BaseConnectionParam createConnectionParams(BaseDataSourceParamDTO dataSourceParam) {
        StarRocksDatasourceParamDTO starRocksDatasourceParamDTO = (StarRocksDatasourceParamDTO) dataSourceParam;
        String address = String.format("%s%s:%s", Constants.JDBC_MYSQL, starRocksDatasourceParamDTO.getHost(), starRocksDatasourceParamDTO.getPort());
        String jdbcUrl = String.format("%s/%s", address, starRocksDatasourceParamDTO.getDatabase());

        StarRocksConnectionParam starRocksConnectionParam = new StarRocksConnectionParam();
        starRocksConnectionParam.setJdbcUrl(jdbcUrl);
        starRocksConnectionParam.setDatabase(starRocksDatasourceParamDTO.getDatabase());
        starRocksConnectionParam.setAddress(address);
        starRocksConnectionParam.setUser(starRocksDatasourceParamDTO.getUserName());
        starRocksConnectionParam.setPassword(PasswordUtils.encodePassword(starRocksDatasourceParamDTO.getPassword()));
        starRocksConnectionParam.setDriverClassName(getDatasourceDriver());
        starRocksConnectionParam.setValidationQuery(getValidationQuery());
        starRocksConnectionParam.setOther(transformOther(starRocksDatasourceParamDTO.getOther()));
        starRocksConnectionParam.setProps(starRocksDatasourceParamDTO.getOther());

        return starRocksConnectionParam;
    }

    @Override
    public ConnectionParam createConnectionParams(String connectionJson) {
        return JSONUtils.parseObject(connectionJson, StarRocksConnectionParam.class);
    }

    @Override
    public String getDatasourceDriver() {
        return Constants.COM_MYSQL_CJ_JDBC_DRIVER;
    }

    @Override
    public String getValidationQuery() {
        return Constants.MYSQL_VALIDATION_QUERY;
    }

    @Override
    public String getJdbcUrl(ConnectionParam connectionParam) {
        StarRocksConnectionParam starRocksConnectionParam = (StarRocksConnectionParam) connectionParam;
        String jdbcUrl = starRocksConnectionParam.getJdbcUrl();
        if (!StringUtils.isEmpty(starRocksConnectionParam.getOther())) {
            return String.format("%s?%s&%s", jdbcUrl, starRocksConnectionParam.getOther(), APPEND_PARAMS);
        }
        return String.format("%s?%s", jdbcUrl, APPEND_PARAMS);
    }

    @Override
    public Connection getConnection(ConnectionParam connectionParam) throws ClassNotFoundException, SQLException {
        StarRocksConnectionParam starRocksConnectionParam = (StarRocksConnectionParam) connectionParam;
        Class.forName(getDatasourceDriver());
        String user = starRocksConnectionParam.getUser();
        if (user.contains(AUTO_DESERIALIZE)) {
            logger.warn("sensitive param : {} in username field is filtered", AUTO_DESERIALIZE);
            user = user.replace(AUTO_DESERIALIZE, "");
        }
        String password = PasswordUtils.decodePassword(starRocksConnectionParam.getPassword());
        if (password.contains(AUTO_DESERIALIZE)) {
            logger.warn("sensitive param : {} in password field is filtered", AUTO_DESERIALIZE);
            password = password.replace(AUTO_DESERIALIZE, "");
        }
        return DriverManager.getConnection(getJdbcUrl(connectionParam), user, password);
    }

    @Override
    public DbType getDbType() {
        return DbType.STARROCKS;
    }

    private String transformOther(Map<String, String> paramMap) {
        if (MapUtils.isEmpty(paramMap)) {
            return null;
        }
        Map<String, String> otherMap = new HashMap<>();
        paramMap.forEach((k, v) -> {
            if (!checkKeyIsLegitimate(k)) {
                return;
            }
            otherMap.put(k, v);
        });
        if (MapUtils.isEmpty(otherMap)) {
            return null;
        }
        StringBuilder stringBuilder = new StringBuilder();
        otherMap.forEach((key, value) -> stringBuilder.append(String.format("%s=%s&", key, value)));
        return stringBuilder.toString();
    }

    private static boolean checkKeyIsLegitimate(String key) {
        return !key.contains(ALLOW_LOAD_LOCAL_IN_FILE_NAME)
                && !key.contains(AUTO_DESERIALIZE)
                && !key.contains(ALLOW_LOCAL_IN_FILE_NAME)
                && !key.contains(ALLOW_URL_IN_LOCAL_IN_FILE_NAME);
    }

    private Map<String, String> parseOther(String other) {
        if (StringUtils.isEmpty(other)) {
            return null;
        }
        Map<String, String> otherMap = new LinkedHashMap<>();
        for (String config : other.split("&")) {
            otherMap.put(config.split("=")[0], config.split("=")[1]);
        }
        return otherMap;
    }
}
