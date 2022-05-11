/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.dolphinscheduler.plugin.task.datax;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.io.FileUtils;
import org.apache.dolphinscheduler.plugin.task.api.AbstractTaskExecutor;
import org.apache.dolphinscheduler.plugin.task.api.ShellCommandExecutor;
import org.apache.dolphinscheduler.plugin.task.api.TaskResponse;
import org.apache.dolphinscheduler.plugin.task.datax.datasource.DataSourceBuildDataXJob;
import org.apache.dolphinscheduler.plugin.task.datax.datasource.impl.MysqlBuildDataXJob;
import org.apache.dolphinscheduler.plugin.task.datax.datasource.impl.StarRocksBuildDataXJob;
import org.apache.dolphinscheduler.plugin.task.util.MapUtils;
import org.apache.dolphinscheduler.plugin.task.util.OSUtils;
import org.apache.dolphinscheduler.spi.enums.Flag;
import org.apache.dolphinscheduler.spi.task.AbstractParameters;
import org.apache.dolphinscheduler.spi.task.Property;
import org.apache.dolphinscheduler.spi.task.paramparser.ParamUtils;
import org.apache.dolphinscheduler.spi.task.paramparser.ParameterUtils;
import org.apache.dolphinscheduler.spi.task.request.TaskRequest;
import org.apache.dolphinscheduler.spi.utils.JSONUtils;
import org.apache.dolphinscheduler.spi.utils.StringUtils;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.dolphinscheduler.spi.task.TaskConstants.EXIT_CODE_FAILURE;
import static org.apache.dolphinscheduler.spi.task.TaskConstants.RWXR_XR_X;

public class DataxTask extends AbstractTaskExecutor {
    /**
     * jvm parameters
     */
    public static final String JVM_PARAM = " --jvm=\"-Xms%sG -Xmx%sG\" ";
    /**
     * python process(datax only supports version 2.7 by default)
     */
    private static final String DATAX_PYTHON = "python2.7";
    private static final Pattern PYTHON_PATH_PATTERN = Pattern.compile("/bin/python[\\d.]*$");
    /**
     * datax path
     */
    private static final String DATAX_PATH = "${DATAX_HOME}/bin/datax.py";
    /**
     * datax channel count
     */
    private static final int DATAX_CHANNEL_COUNT = 1;

    /**
     * datax parameters
     */
    private DataxParameters dataXParameters;

    /**
     * shell command executor
     */
    private ShellCommandExecutor shellCommandExecutor;

    /**
     * taskExecutionContext
     */
    private TaskRequest taskExecutionContext;

    /**
     * 根据不同数据源，创建不同Job的工作类
     */
    private static Map<String, DataSourceBuildDataXJob> dataSourceBuildDataXJobMap = new HashMap<>();

    /**
     * constructor
     *
     * @param taskExecutionContext taskExecutionContext
     */
    public DataxTask(TaskRequest taskExecutionContext) {
        super(taskExecutionContext);
        this.taskExecutionContext = taskExecutionContext;

        this.shellCommandExecutor = new ShellCommandExecutor(this::logHandle,
                taskExecutionContext, logger);
    }

    /**
     * init DataX config
     */
    @Override
    public void init() {
        logger.info("datax task params {}", taskExecutionContext.getTaskParams());
        dataXParameters = JSONUtils.parseObject(taskExecutionContext.getTaskParams(), DataxParameters.class);

        if (!dataXParameters.checkParameters()) {
            throw new RuntimeException("datax task params is not valid");
        }
        // 将需要的 构建 DataX  job.json 的类放入Map中，后续进行使用
        buildDataSourceBuildDataXJobMap(dataXParameters.getDsType(), dataXParameters.getDtType());
    }

    /**
     * run DataX process
     *
     * @throws Exception if error throws Exception
     */
    @Override
    public void handle() throws Exception {
        try {

            // replace placeholder,and combine local and global parameters
            Map<String, Property> paramsMap = ParamUtils.convert(taskExecutionContext, getParameters());
            if (MapUtils.isEmpty(paramsMap)) {
                paramsMap = new HashMap<>();
            }
            if (MapUtils.isNotEmpty(taskExecutionContext.getParamsMap())) {
                paramsMap.putAll(taskExecutionContext.getParamsMap());
            }

            // run datax procesDataSourceService.s
            String jsonFilePath = buildDataxJsonFile(paramsMap);
            String shellCommandFilePath = buildShellCommandFile(jsonFilePath, paramsMap);
            TaskResponse commandExecuteResult = shellCommandExecutor.run(shellCommandFilePath);

            setExitStatusCode(commandExecuteResult.getExitStatusCode());
            setAppIds(commandExecuteResult.getAppIds());
            setProcessId(commandExecuteResult.getProcessId());
        } catch (Exception e) {
            setExitStatusCode(EXIT_CODE_FAILURE);
            throw e;
        }
    }

    /**
     * cancel DataX process
     *
     * @param cancelApplication cancelApplication
     * @throws Exception if error throws Exception
     */
    @Override
    public void cancelApplication(boolean cancelApplication)
            throws Exception {
        // cancel process
        shellCommandExecutor.cancelApplication();
    }

    /**
     * build datax configuration file
     *
     * @return datax json file name
     * @throws Exception if error throws Exception
     */
    private String buildDataxJsonFile(Map<String, Property> paramsMap)
            throws Exception {
        // generate json
        String fileName = String.format("%s/%s_job.json",
                taskExecutionContext.getExecutePath(),
                taskExecutionContext.getTaskAppId());
        String json;

        Path path = new File(fileName).toPath();
        if (Files.exists(path)) {
            return fileName;
        }

        if (dataXParameters.getCustomConfig() == Flag.YES.ordinal()) {
            json = dataXParameters.getJson().replaceAll("\\r\\n", "\n");
        } else {
            ObjectNode job = JSONUtils.createObjectNode();
            job.putArray("content").addAll(buildDataxJobContentJson());
            job.set("setting", buildDataxJobSettingJson());

            ObjectNode root = JSONUtils.createObjectNode();
            root.set("job", job);
            root.set("core", buildDataxCoreJson());
            json = root.toString();
        }

        // replace placeholder
        json = ParameterUtils.convertParameterPlaceholders(json, ParamUtils.convert(paramsMap));

        logger.debug("datax job json : {}", json);

        // create datax json file
        FileUtils.writeStringToFile(new File(fileName), json, StandardCharsets.UTF_8);
        return fileName;
    }

    /**
     * build datax job config
     *
     * @return collection of datax job config JSONObject
     * @throws SQLException if error throws SQLException
     */
    private List<ObjectNode> buildDataxJobContentJson() {
        String dtType = dataXParameters.getDtType();
        DataSourceBuildDataXJob dataSourceBuildDataXJob;
        if (StringUtils.equalsIgnoreCase(dtType, "STARROCKS")) {
            dataSourceBuildDataXJob = dataSourceBuildDataXJobMap.get(dtType);
        } else {
            dataSourceBuildDataXJob = dataSourceBuildDataXJobMap.get("MYSQL");
        }
        return dataSourceBuildDataXJob.buildDataxJobContentJson(taskExecutionContext, dataXParameters);
    }

    /**
     * build datax setting config
     *
     * @return datax setting config JSONObject
     */
    private ObjectNode buildDataxJobSettingJson() {

        ObjectNode speed = JSONUtils.createObjectNode();

        speed.put("channel", DATAX_CHANNEL_COUNT);

        if (dataXParameters.getJobSpeedByte() > 0) {
            speed.put("byte", dataXParameters.getJobSpeedByte());
        }

        if (dataXParameters.getJobSpeedRecord() > 0) {
            speed.put("record", dataXParameters.getJobSpeedRecord());
        }

        ObjectNode errorLimit = JSONUtils.createObjectNode();
        errorLimit.put("record", 0);
        errorLimit.put("percentage", 0);

        ObjectNode setting = JSONUtils.createObjectNode();
        setting.set("speed", speed);
        setting.set("errorLimit", errorLimit);

        return setting;
    }

    private ObjectNode buildDataxCoreJson() {

        ObjectNode speed = JSONUtils.createObjectNode();
        speed.put("channel", DATAX_CHANNEL_COUNT);

        if (dataXParameters.getJobSpeedByte() > 0) {
            speed.put("byte", dataXParameters.getJobSpeedByte());
        }

        if (dataXParameters.getJobSpeedRecord() > 0) {
            speed.put("record", dataXParameters.getJobSpeedRecord());
        }

        ObjectNode channel = JSONUtils.createObjectNode();
        channel.set("speed", speed);

        ObjectNode transport = JSONUtils.createObjectNode();
        transport.set("channel", channel);

        ObjectNode core = JSONUtils.createObjectNode();
        core.set("transport", transport);

        return core;
    }

    /**
     * create command
     *
     * @return shell command file name
     * @throws Exception if error throws Exception
     */
    private String buildShellCommandFile(String jobConfigFilePath, Map<String, Property> paramsMap)
            throws Exception {
        // generate scripts
        String fileName = String.format("%s/%s_node.%s",
                taskExecutionContext.getExecutePath(),
                taskExecutionContext.getTaskAppId(),
                OSUtils.isWindows() ? "bat" : "sh");

        Path path = new File(fileName).toPath();

        if (Files.exists(path)) {
            return fileName;
        }

        // datax python command
        StringBuilder sbr = new StringBuilder();
        sbr.append(getPythonCommand());
        sbr.append(" ");
        sbr.append(DATAX_PATH);
        sbr.append(" ");
        sbr.append(loadJvmEnv(dataXParameters));
        sbr.append(jobConfigFilePath);

        // replace placeholder
        String dataxCommand = ParameterUtils.convertParameterPlaceholders(sbr.toString(), ParamUtils.convert(paramsMap));

        logger.debug("raw script : {}", dataxCommand);

        // create shell command file
        Set<PosixFilePermission> perms = PosixFilePermissions.fromString(RWXR_XR_X);
        FileAttribute<Set<PosixFilePermission>> attr = PosixFilePermissions.asFileAttribute(perms);

        if (OSUtils.isWindows()) {
            Files.createFile(path);
        } else {
            Files.createFile(path, attr);
        }

        Files.write(path, dataxCommand.getBytes(), StandardOpenOption.APPEND);

        return fileName;
    }

    public String getPythonCommand() {
        String pythonHome = System.getenv("PYTHON_HOME");
        return getPythonCommand(pythonHome);
    }

    public String getPythonCommand(String pythonHome) {
        if (StringUtils.isEmpty(pythonHome)) {
            return DATAX_PYTHON;
        }
        String pythonBinPath = "/bin/" + DATAX_PYTHON;
        Matcher matcher = PYTHON_PATH_PATTERN.matcher(pythonHome);
        if (matcher.find()) {
            return matcher.replaceAll(pythonBinPath);
        }
        return Paths.get(pythonHome, pythonBinPath).toString();
    }

    public String loadJvmEnv(DataxParameters dataXParameters) {
        int xms = Math.max(dataXParameters.getXms(), 1);
        int xmx = Math.max(dataXParameters.getXmx(), 1);
        return String.format(JVM_PARAM, xms, xmx);
    }

    @Override
    public AbstractParameters getParameters() {
        return dataXParameters;
    }

    private void buildDataSourceBuildDataXJobMap(String dsType, String dtType) {
        if (!dataSourceBuildDataXJobMap.containsKey(dsType)) {
            if (StringUtils.equalsIgnoreCase(dsType, "STARROCKS")) {
                dataSourceBuildDataXJobMap.put("STARROCKS", new StarRocksBuildDataXJob());
            } else {
                dataSourceBuildDataXJobMap.put("MYSQL", new MysqlBuildDataXJob());
            }
        }

        if (!dataSourceBuildDataXJobMap.containsKey(dtType)) {
            if (StringUtils.equalsIgnoreCase(dtType, "STARROCKS")) {
                dataSourceBuildDataXJobMap.put("STARROCKS", new StarRocksBuildDataXJob());
            } else {
                dataSourceBuildDataXJobMap.put("MYSQL", new MysqlBuildDataXJob());
            }
        }
    }

}
