package org.apache.dolphinscheduler.plugin.task.datax.datasource.impl;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.dolphinscheduler.plugin.task.datax.DataxParameters;
import org.apache.dolphinscheduler.spi.task.request.TaskRequest;

import java.util.List;

public class MysqlBuildDataXJob extends AbsDataSourceBuildDataXJob {

    @Override
    public List<ObjectNode> buildDataxJobContentJson(TaskRequest taskExecutionContext, DataxParameters dataXParameters) {
        return super.buildDataxJobContentJson(taskExecutionContext, dataXParameters);
    }

}
