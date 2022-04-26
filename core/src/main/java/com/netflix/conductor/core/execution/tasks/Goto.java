package com.netflix.conductor.core.execution.tasks;

import com.netflix.conductor.core.execution.WorkflowExecutor;
import com.netflix.conductor.model.TaskModel;
import com.netflix.conductor.model.WorkflowModel;
import org.springframework.stereotype.Component;

import static com.netflix.conductor.common.metadata.tasks.TaskType.TASK_TYPE_GOTO;

@Component(TASK_TYPE_GOTO)
public class Goto extends WorkflowSystemTask{
    public Goto() {
        super("GOTO");
    }

    @Override
    public boolean execute(WorkflowModel workflow, TaskModel task, WorkflowExecutor provider) {
        task.setStatus(TaskModel.Status.COMPLETED);
        return true;
    }
}
