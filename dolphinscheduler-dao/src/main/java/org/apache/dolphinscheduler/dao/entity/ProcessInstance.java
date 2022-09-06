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

package org.apache.dolphinscheduler.dao.entity;

import org.apache.dolphinscheduler.common.enums.CommandType;
import org.apache.dolphinscheduler.common.enums.FailureStrategy;
import org.apache.dolphinscheduler.common.enums.Flag;
import org.apache.dolphinscheduler.common.enums.Priority;
import org.apache.dolphinscheduler.common.enums.TaskDependType;
import org.apache.dolphinscheduler.common.enums.WarningType;
import org.apache.dolphinscheduler.common.utils.DateUtils;
import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.plugin.task.api.enums.ExecutionStatus;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Objects;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Strings;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import org.apache.commons.lang3.StringUtils;

/**
 * process instance
 */
@Data
@TableName("t_ds_process_instance")
public class ProcessInstance {

    /**
     * id
     */
    @TableId(value = "id", type = IdType.AUTO)
    private int id;

    /**
     * process definition code
     */
    private Long processDefinitionCode;

    /**
     * process definition version
     */
    private int processDefinitionVersion;

    /**
     * process state
     */
    private ExecutionStatus state;

    /**
     * state history
     */
    private String stateHistory;

    /**
     * state desc list from state history
     */
    @TableField(exist = false)
    private List<StateDesc> stateDescList;

    /**
     * recovery flag for failover
     */
    private Flag recovery;
    /**
     * start time
     */
    private Date startTime;

    /**
     * end time
     */
    private Date endTime;

    /**
     * run time
     */
    private int runTimes;

    /**
     * name
     */
    private String name;

    /**
     * host
     */
    private String host;

    /**
     * process definition structure
     */
    @TableField(exist = false)
    private ProcessDefinition processDefinition;
    /**
     * process command type
     */
    private CommandType commandType;

    /**
     * command parameters
     */
    private String commandParam;

    /**
     * node depend type
     */
    private TaskDependType taskDependType;

    /**
     * task max try times
     */
    private int maxTryTimes;

    /**
     * failure strategy when task failed.
     */
    private FailureStrategy failureStrategy;

    /**
     * warning type
     */
    private WarningType warningType;

    /**
     * warning group
     */
    private Integer warningGroupId;

    /**
     * schedule time
     */
    private Date scheduleTime;

    /**
     * command start time
     */
    private Date commandStartTime;

    /**
     * user define parameters string
     */
    private String globalParams;

    /**
     * dagData
     */
    @TableField(exist = false)
    private DagData dagData;

    /**
     * executor id
     */
    private int executorId;

    /**
     * executor name
     */
    @TableField(exist = false)
    private String executorName;

    /**
     * tenant code
     */
    @TableField(exist = false)
    private String tenantCode;

    /**
     * queue
     */
    @TableField(exist = false)
    private String queue;

    /**
     * process is sub process
     */
    private Flag isSubProcess;

    /**
     * task locations for web
     */
    @TableField(exist = false)
    private String locations;

    /**
     * history command
     */
    private String historyCmd;

    /**
     * depend processes schedule time
     */
    @TableField(exist = false)
    private String dependenceScheduleTimes;

    /**
     * process duration
     *
     * @return
     */
    @TableField(exist = false)
    private String duration;

    /**
     * process instance priority
     */
    private Priority processInstancePriority;

    /**
     * worker group
     */
    private String workerGroup;

    /**
     * environment code
     */
    private Long environmentCode;

    /**
     * process timeout for warning
     */
    private int timeout;

    /**
     * tenant id
     */
    private int tenantId;

    /**
     * varPool string
     */
    private String varPool;
    /**
     * serial queue next processInstanceId
     */
    private int nextProcessInstanceId;

    /**
     * dry run flag
     */
    private int dryRun;

    /**
     * re-start time
     */
    private Date restartTime;

    /**
     * workflow block flag
     */
    @TableField(exist = false)
    private boolean isBlocked;

    public ProcessInstance() {

    }

    /**
     * set the process name with process define version and timestamp
     *
     * @param processDefinition processDefinition
     */
    public ProcessInstance(ProcessDefinition processDefinition) {
        this.processDefinition = processDefinition;
        // todo: the name is not unique
        this.name = String.join("-",
                processDefinition.getName(),
                String.valueOf(processDefinition.getVersion()),
                DateUtils.getCurrentTimeStamp());
    }

    public boolean isProcessInstanceStop() {
        return this.state.typeIsFinished();
    }

    /**
     * add command to history
     *
     * @param cmd cmd
     */
    public void addHistoryCmd(CommandType cmd) {
        if (!Strings.isNullOrEmpty(this.historyCmd)) {
            this.historyCmd = String.format("%s,%s", this.historyCmd, cmd.toString());
        } else {
            this.historyCmd = cmd.toString();
        }
    }

    /**
     * check this process is start complement data
     *
     * @return whether complement data
     */
    public boolean isComplementData() {
        if (Strings.isNullOrEmpty(this.historyCmd)) {
            return false;
        }
        return historyCmd.startsWith(CommandType.COMPLEMENT_DATA.toString());
    }

    /**
     * get current command type,
     * if start with complement data,return complement
     *
     * @return CommandType
     */
    public CommandType getCmdTypeIfComplement() {
        if (isComplementData()) {
            return CommandType.COMPLEMENT_DATA;
        }
        return commandType;
    }

    /**
     * set state with desc
     * @param state
     * @param stateDesc
     */
    public void setStateWithDesc(ExecutionStatus state, String stateDesc) {
        this.setState(state);
        if (StringUtils.isEmpty(this.getStateHistory())) {
            stateDescList = new ArrayList<>();
        } else if (stateDescList == null) {
            stateDescList = JSONUtils.toList(this.getStateHistory(), StateDesc.class);
        }
        stateDescList.add(new StateDesc(new Date(), state, stateDesc));
        this.setStateHistory(JSONUtils.toJsonString(stateDescList));
    }

    @Override
    public String toString() {
        return "ProcessInstance{"
                + "id=" + id
                + ", state=" + state
                + ", recovery=" + recovery
                + ", startTime=" + startTime
                + ", endTime=" + endTime
                + ", runTimes=" + runTimes
                + ", name='" + name + '\''
                + ", host='" + host + '\''
                + ", processDefinition="
                + processDefinition
                + ", commandType="
                + commandType
                + ", commandParam='"
                + commandParam
                + '\''
                + ", taskDependType="
                + taskDependType
                + ", maxTryTimes="
                + maxTryTimes
                + ", failureStrategy="
                + failureStrategy
                + ", warningType="
                + warningType
                + ", warningGroupId="
                + warningGroupId
                + ", scheduleTime="
                + scheduleTime
                + ", commandStartTime="
                + commandStartTime
                + ", globalParams='"
                + globalParams
                + '\''
                + ", executorId="
                + executorId
                + ", tenantCode='"
                + tenantCode
                + '\''
                + ", queue='"
                + queue
                + '\''
                + ", isSubProcess="
                + isSubProcess
                + ", locations='"
                + locations
                + '\''
                + ", historyCmd='"
                + historyCmd
                + '\''
                + ", dependenceScheduleTimes='"
                + dependenceScheduleTimes
                + '\''
                + ", duration="
                + duration
                + ", processInstancePriority="
                + processInstancePriority
                + ", workerGroup='"
                + workerGroup
                + '\''
                + ", timeout="
                + timeout
                + ", tenantId="
                + tenantId
                + ", processDefinitionCode='"
                + processDefinitionCode
                + '\''
                + ", processDefinitionVersion='"
                + processDefinitionVersion
                + '\''
                + ", dryRun='"
                + dryRun
                + '\''
                + '}'
                + ", restartTime='"
                + restartTime
                + '\''
                + ", isBlocked="
                + isBlocked
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ProcessInstance that = (ProcessInstance) o;

        return id == that.id;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class StateDesc {

        Date time;
        ExecutionStatus state;
        String desc;
    }
}
