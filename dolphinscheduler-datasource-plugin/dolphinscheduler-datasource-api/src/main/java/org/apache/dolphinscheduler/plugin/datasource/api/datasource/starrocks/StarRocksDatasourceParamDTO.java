package org.apache.dolphinscheduler.plugin.datasource.api.datasource.starrocks;

import org.apache.dolphinscheduler.plugin.datasource.api.datasource.BaseDataSourceParamDTO;
import org.apache.dolphinscheduler.spi.enums.DbType;

public class StarRocksDatasourceParamDTO extends BaseDataSourceParamDTO {
    @Override
    public String toString() {
        return "StarRocksDatasourceParamDTO{"
                + "name='" + name + '\''
                + ", note='" + note + '\''
                + ", host='" + host + '\''
                + ", port=" + port
                + ", database='" + database + '\''
                + ", userName='" + userName + '\''
                + ", password='" + password + '\''
                + ", other='" + other + '\''
                + '}';
    }

    @Override
    public DbType getType() {
        return DbType.STARROCKS;
    }
}
