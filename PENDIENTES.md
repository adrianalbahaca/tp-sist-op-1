# Lista de pendientes

- [ ] Rediseñar el Resource Manager para simplificar las estructuras
  - [ ] *Revisar cola* -> *Función para imprimir toda la cola*
  - [ ] ¿Posible cola circular con capacidad limitada? -> Muy probable
  - [ ] ¿Uso de monitores? -> Probable -> ¿Cómo armar un monitor para estas estructuras?
  - [ ] ¿Colas con exclusión mutua? -> **SI** -> *Problema de productor-consumidor*
  - [ ] ¿Qué estructuras están de más?
- [ ] Corregir funciones de testeo de deadlock para el Erlang
- [ ] Corregir el script de testeo para deadlock
- [ ] Verificar posible deadlock en Resource Manager después de simplificar sus estructuras
  - [ ] ¿Uso de monitores? -> **Investigar más [Adrian]**
- [ ] Revisión del informe después de los cambios en el Resource Manager
