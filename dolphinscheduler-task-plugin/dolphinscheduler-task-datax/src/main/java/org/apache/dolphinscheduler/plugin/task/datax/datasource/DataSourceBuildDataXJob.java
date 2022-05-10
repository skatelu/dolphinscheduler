package org.apache.dolphinscheduler.plugin.task.datax.datasource;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.dolphinscheduler.plugin.task.datax.DataxParameters;
import org.apache.dolphinscheduler.spi.task.request.TaskRequest;

import java.util.List;

public interface DataSourceBuildDataXJob {

    List<ObjectNode> buildDataxJobContentJson(TaskRequest taskExecutionContext, DataxParameters dataXParameters);



}
