package main

import (
	"fmt"
	"net"
	"net/rpc"
	"os"
	"raft/internal/comun/check"
	"raft/internal/comun/rpctimeout"
	"raft/internal/raft"
	"strconv"
	"strings"
)

func main() {
    // Crear un mapa para almacenar datos en el contenedor
    contenedor := make(map[string]string)

    dns := "service-raft.default.svc.cluster.local"

    meStr := os.Args[1]
    name := strings.Split(meStr, "-")[0]
    me, err := strconv.Atoi(strings.Split(meStr, "-")[1])
    check.CheckError(err, "Main atoi error")

    nodos := make([]rpctimeout.HostPort, 0)
    for i := 0; i < 3; i++ {
        nodos = append(nodos, rpctimeout.MakeHostPort(name+"-"+strconv.Itoa(i)+"."+dns, ":6000"))
    }
	fmt.Println("Nodos: ", nodos)
	
	// Crear un canal para recibir operaciones a aplicar en el nodo Raft
	chAplicarOperacion := make(chan raft.AplicaOperacion, 1000)
	fmt.Println(me)
	// Configurar el nodo Raft y registrar en el servicio RPC
	nr := raft.NuevoNodo(nodos, me, chAplicarOperacion)
	rpc.Register(nr)

	fmt.Println("Replica escucha en :", me, " de ", nodos)

	// Iniciar goroutine para aplicar operaciones recibidas en el canal
	go aplicarOperacion(contenedor, chAplicarOperacion, nr)

	// Configurar el servidor RPC y comenzar a escuchar en la dirección especificada
	l, err := net.Listen("tcp", string(nodos[me]))
	check.CheckError(err, "Main listen error:")

	fmt.Println("Replica escucha en :", nodos[me])

	// Aceptar conexiones RPC de forma continua
	for {
		rpc.Accept(l)
	}
}

// aplicarOperacion es una función que se encarga de aplicar las operaciones recibidas en el canal de entrada.
// Recibe un mapa de claves y valores, un canal de operaciones a aplicar y un puntero a un NodoRaft.
// La función verifica el tipo de operación (escribir o leer) y busca el valor asociado a la clave en el registro del NodoRaft.
// Si no se encuentra un valor asociado, se utiliza el valor de la operación actual.
// Finalmente, el valor devuelto se envía al canal de operaciones aplicadas.
func aplicarOperacion(cont map[string]string, canal chan raft.AplicaOperacion, nr *raft.NodoRaft) {
	for {
		valorADevolver := ""
		operacion := <-nr.AplicarOperacion

		// Verificar el tipo de operación (escribir o leer)
		if operacion.Operacion.Operacion == "escribir" {
			// Buscar el valor asociado a la clave en el registro del nodo Raft
			for i := nr.LastApplied; i > 0 && valorADevolver == ""; i-- {
				if nr.Log[nr.LastApplied-1].Operacion.Clave == operacion.Operacion.Clave {
					valorADevolver = nr.Log[nr.LastApplied-1].Operacion.Valor
				}
			}

			// Si no se encuentra un valor asociado, utilizar el valor de la operación actual
			if valorADevolver == "" {
				valorADevolver = operacion.Operacion.Valor
			}

		} else if operacion.Operacion.Operacion == "leer" {
			// Buscar el valor asociado a la clave en el registro del nodo Raft para operaciones de lectura
			for i := nr.LastApplied; i > 0 && valorADevolver == ""; i-- {
				if nr.Log[nr.LastApplied-1].Operacion.Clave == operacion.Operacion.Clave {
					valorADevolver = nr.Log[nr.LastApplied-1].Operacion.Valor
				}
			}
		}

		// Enviar el valor devuelto al canal de operaciones aplicadas
		nr.Commited <- valorADevolver
	}
}
