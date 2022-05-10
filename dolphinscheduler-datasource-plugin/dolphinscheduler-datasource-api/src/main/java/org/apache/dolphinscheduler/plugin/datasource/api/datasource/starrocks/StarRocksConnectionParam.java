package org.apache.dolphinscheduler.plugin.datasource.api.datasource.starrocks;

import org.apache.dolphinscheduler.spi.datasource.BaseConnectionParam;

public class StarRocksConnectionParam extends BaseConnectionParam {

    private String loadUrl;

    public String getLoadUrl() {
        return loadUrl;
    }

    public void setLoadUrl(String loadUrl) {
        this.loadUrl = loadUrl;
    }


    @Override
    public String toString() {
        return "StarRocksConnectionParam{"
                + "user='" + user + '\''
                + ", password='" + password + '\''
                + ", address='" + address + '\''
                + ", database='" + database + '\''
                + ", jdbcUrl='" + jdbcUrl + '\''
                + ", loadUrl=" + loadUrl
                + ", driverLocation='" + driverLocation + '\''
                + ", driverClassName='" + driverClassName + '\''
                + ", validationQuery='" + validationQuery + '\''
                + ", other='" + other + '\''
                + '}';
    }

}
