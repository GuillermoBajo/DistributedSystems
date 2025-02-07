# Distributed Systems

This repository contains implementations of popular distributed systems algorithms. It includes the **Raft consensus algorithm with Kubernetes integration** and **Ricart-Agrawala** in Go for distributed mutual exclusion.

## **Ricart-Agrawala**:

- Enables mutual exclusion in distributed systems without a central coordinator.
- Uses request, permission, and release messages to control access to shared resources.
- Reduces the number of messages required compared to other distributed mutual exclusion algorithms.
- Ensures fairness in resource access among participating nodes.

## **Raft with Kubernetes**:

- Provides a robust consensus protocol to ensure data consistency in distributed systems.
- Simplifies cluster deployment and management with Kubernetes.
- Enables automatic failure recovery through leader election and log replication.
- Offers a scalable solution for container management and orchestration.
