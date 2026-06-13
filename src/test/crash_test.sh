#!/bin/bash

CRASH_POINT=$1   # ej: CRASH_AFTER_LOG
CRASH_AT=$2      # ej: 3 (crashear en la 3ra invocación)

echo "=== Test: $CRASH_POINT luego de $CRASH_AT invocaciones ==="

# Primera corrida: crashea en el punto indicado
export $CRASH_POINT=$CRASH_AT
docker compose up counter_q2 &
sleep 10  # darle tiempo a procesar algunos mensajes

# Verificar que murió
if docker compose ps | grep counter_q2 | grep -q "exited"; then
    echo "✓ Worker crasheó como se esperaba"
else
    echo "✗ Worker no crasheó, revisar configuración"
    exit 1
fi

# Segunda corrida: sin crash, debe restaurar y terminar correctamente
unset $CRASH_POINT
docker compose up counter_q2
echo "=== Verificar resultados en los joiners ==="