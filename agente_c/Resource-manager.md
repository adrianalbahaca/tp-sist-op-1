# Adaptación del Resource Manager

- Job Owner innecesario. Ya la tabla de conexiones hace el trabajo de mantener el control de las cosas
- Posible simplificación de OutRequests. Hacer alguna marca de dónde viene cada solicitud
- Cola de Pending Requests con límite de capacidad?
- protocol.c -> parsing.c
- AUMENTAR CANTIDAD DE DOCUMENTACIÓN SOBRE EL PROPÓSITO DE CADA VAINA!!! (Nota personal para mí porque escupo ideas y no anoto nada xd)
- Mejor usar semáforos y variables de condición para Exclusión Mutua
- `avanzar_reserva` puede mejorarse y simplificarse?
