# TP-Money-Laundering-Analysis
Repositorio para el trabajo final de la materia "Sitemas distribuidos" de la Facultad de Ingenieria Universidad de Buenos Aires - Catedra Roca

# Descripción
El presente trabajo apunta al desarrollo de un sistema distribuido que pueda detectar patrones básicos de lavado de activos guiado por 3 hitos:

- [Diseño](https://drive.google.com/file/d/1LbNoOUZPO24Qp6EHgYHAkvKqdJMf92Vk/view)
- Escalabilidad
- Tolerancia

# Caracteristicas del sistema
- Procesar datasets de transacciones de gran tamaño de forma distribuida.
- Soportar múltiples requisitos analíticos (filtrado, agregación, ranking, joins).
- Garantizar tolerancia a fallos, manejo de duplicados e idempotencia.
- Escalado horizontal e independientemente para cada tipo de nodo.

# Arquitectura general del sistema
**insertar diagrama de robustes gral

## Ejecución

`make up` : Inicia los contenedores del sistema y comienza a seguir los logs de todos ellos en un solo flujo de salida.

`make down`:   Detiene los contenedores y libera los recursos asociados.

`make logs`: Sigue los logs de todos los contenedores en un solo flujo de salida.

`make test`: Inicia los contenedores del sistema, espera a que los clientes finalicen, compara los resultados con una ejecución serial y detiene los contenederes.

`make switch`: Permite alternar rápidamente entre los archivos de docker compose de los distintos escenarios provistos.

# Integrantes
- Carbajal Robles Kevin Emir 
- Rea Matias 
- Sendra Alejo

# Referencias
[1] Altman, E. (s.f.). IBM Transactions for Anti Money Laundering (AML) [Dataset]. Kaggle.
https://www.kaggle.com/datasets/ealtman2019/ibm-transactions-for-anti-money-laundering-aml

[2] Frankfurter. (s.f.). Frankfurter API: Foreign exchange rates API. https://api.frankfurter.dev/

[3] pablodroca. (s.f.). Money laundering analysis [Notebook]. Kaggle.
https://www.kaggle.com/code/pablodroca/money-laundering-analysis
