# Sistemas Distribuidos

Este repositorio contiene implementaciones de algoritmos populares de sistemas distribuidos. Incluye el **algoritmo de consenso Raft con integración de Kubernetes** y el **Ricart-Agrawala** en Go para exclusión mutua distribuida.

## **Ricart-Agrawala**:

- Permite la exclusión mutua en sistemas distribuidos sin un coordinador central.
- Utiliza mensajes de solicitud, permiso y liberación para controlar el acceso a recursos compartidos.
- Reduce la cantidad de mensajes necesarios en comparación con otros algoritmos de exclusión mutua distribuidos.
- Garantiza la equidad en el acceso a los recursos entre los nodos participantes.

## **Raft con Kubernetes**:

- Proporciona un protocolo de consenso robusto para garantizar la consistencia de datos en sistemas distribuidos.
- Simplifica el despliegue y la gestión de clústeres con Kubernetes.
- Permite la recuperación automática ante fallos mediante la elección de líderes y la replicación de registros.
- Ofrece una solución escalable para la gestión y orquestación de contenedores.
