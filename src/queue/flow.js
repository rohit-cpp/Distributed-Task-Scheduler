import { FlowProducer } from 'bullmq';
import { connection } from '../config/index.js';

export const taskFlow = new FlowProducer({ connection });

export async function createJobChain(parentData, childData, parentName = 'parent-task', childName = 'child-task') {
  return taskFlow.add({
    name: parentName,
    data: parentData,
    queueName: 'tasks',
    children: [
      {
        name: childName,
        data: childData,
        queueName: 'tasks',
      },
    ],
  });
}
