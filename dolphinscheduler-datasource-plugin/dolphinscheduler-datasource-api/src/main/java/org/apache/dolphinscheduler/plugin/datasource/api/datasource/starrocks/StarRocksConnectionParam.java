package org.apache.dolphinscheduler.plugin.datasource.api.datasource.starrocks;

import org.apache.dolphinscheduler.spi.datasource.BaseConnectionParam;

public class StarRocksConnectionParam extends BaseConnectionParam {

    @Override
    public String toString() {
        return "StarRocksConnectionParam{"
                + "user='" + user + '\''
                + ", password='" + password + '\''
                + ", address='" + address + '\''
                + ", database='" + database + '\''
                + ", jdbcUrl='" + jdbcUrl + '\''
                + ", driverLocation='" + driverLocation + '\''
                + ", driverClassName='" + driverClassName + '\''
                + ", validationQuery='" + validationQuery + '\''
                + ", other='" + other + '\''
                + '}';
    }

}
