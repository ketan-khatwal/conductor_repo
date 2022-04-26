package com.netflix.conductor.core.execution.mapper;

import com.netflix.conductor.common.metadata.tasks.TaskType;
import com.netflix.conductor.common.metadata.workflow.WorkflowTask;
import com.netflix.conductor.core.exception.TerminateWorkflowException;
import com.netflix.conductor.model.TaskModel;
import com.netflix.conductor.model.WorkflowModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Component
public class GotoTaskMapper implements TaskMapper{
    Logger logger = LoggerFactory.getLogger(GotoTaskMapper.class);
    @Override
    public TaskType getTaskType() {
        return TaskType.GOTO;
    }

    @Override
    public List<TaskModel> getMappedTasks(TaskMapperContext taskMapperContext) throws TerminateWorkflowException {
        logger.debug("TaskMapperContext {} in GotoTaskMapper", taskMapperContext);
        List<TaskModel> tasksToBeScheduled = new ArrayList<>();
        WorkflowTask taskToSchedule = taskMapperContext.getTaskToSchedule();
        WorkflowModel workflowInstance = taskMapperContext.getWorkflowInstance();
        Map<String, Object> taskInput = taskMapperContext.getTaskInput();
        int retryCount = taskMapperContext.getRetryCount();
        int iterationCount = taskMapperContext.getIterationCount();
        String taskId = taskMapperContext.getTaskId();

        TaskModel gotoTask = new TaskModel();
        gotoTask.setTaskType(TaskType.GOTO.name());
        gotoTask.setTaskDefName(TaskType.GOTO.name());
        gotoTask.setReferenceTaskName(taskToSchedule.getTaskReferenceName());
        gotoTask.setWorkflowInstanceId(workflowInstance.getWorkflowId());
        gotoTask.setWorkflowType(workflowInstance.getWorkflowName());
        gotoTask.setCorrelationId(workflowInstance.getCorrelationId());
        gotoTask.setScheduledTime(System.currentTimeMillis());
        gotoTask.setInputData(taskInput);
        gotoTask.setTaskId(taskId);
        gotoTask.setRetryCount(retryCount);
        gotoTask.setStatus(TaskModel.Status.SCHEDULED);
        gotoTask.setWorkflowTask(taskToSchedule);
        gotoTask.setIterationCount(iterationCount);
        tasksToBeScheduled.add(gotoTask);


        // get the next task to be executed from goto task
        WorkflowTask nextWorkflowTask = workflowInstance.getWorkflowDefinition().getTaskByRefName(taskToSchedule.getGotoTask());
        System.out.println(nextWorkflowTask);
        //get the current iterationCount of next task to be executed if it is already executed.
        TaskModel nextTask = workflowInstance.getTaskByRefName(nextWorkflowTask.getTaskReferenceName());
        System.out.println(nextTask);
        int nextTaskIterationCount = 0;

        if(nextTask != null)
        {
            nextTask.setStatus(TaskModel.Status.SCHEDULED);
            nextTaskIterationCount = nextTask.getIterationCount() + 1;
        }


        List<TaskModel> nextTasks = taskMapperContext.getDeciderService().getTasksToBeScheduled(workflowInstance, nextWorkflowTask, retryCount, taskMapperContext.getRetryTaskId(), nextTaskIterationCount); //get next task to be scheduled with iterationCount incremented by one

        if(!nextTasks.isEmpty()) {

            Map<String, Object> gotoTaskInputMap = gotoTask.getInputData();
            if(gotoTaskInputMap != null) {
                replaceNextTasksInputWithGotoTaskInput(gotoTaskInputMap, nextTasks); // update next task's input with GOTO task's input if they have same input key
            }

            tasksToBeScheduled.addAll(nextTasks);
        }

        return tasksToBeScheduled;
    }

    private void replaceNextTasksInputWithGotoTaskInput(Map<String, Object> gotoTaskInputMap, List<TaskModel> nextTasks) {
        if (gotoTaskInputMap.size() == 0) {
            return;
        }

        for (Map.Entry<String, Object> entry : gotoTaskInputMap.entrySet()) {
            String keyToSearch = entry.getKey();
            Object valueToReplace = entry.getValue();

            for (TaskModel t : nextTasks) {
                Map<String, Object> taskInputMap = t.getInputData();
                if (taskInputMap.containsKey(keyToSearch)) {
                    taskInputMap.replace(keyToSearch, valueToReplace);
                }
            }

        }

    }

}
