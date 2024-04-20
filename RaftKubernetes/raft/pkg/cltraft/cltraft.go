package main

import (
	"fmt"
	"os"
	"raft/internal/comun/check"
	"raft/internal/comun/rpctimeout"
	"raft/internal/raft"
	"strconv"
	"time"
)

func main() {
	dns := os.Args[1]
    prefijo := os.Args[2]
    port := ":" + os.Args[3]

	nodos := make([]rpctimeout.HostPort, 0)
	for i := 0; i < 3; i++ {
		nodos = append(nodos, rpctimeout.MakeHostPort(prefijo + "-" + strconv.Itoa(i)+ "." + dns, port))
	}

	time.Sleep(10 * time.Second)

	// Operaciones a enviar
	op1 := raft.TipoOperacion{Operacion: "escribir", Clave: "a", Valor: "1"}
	op2 := raft.TipoOperacion{Operacion: "leer", Clave: "a", Valor: ""}

	// for enviando operaciones hasta que reply . Error == nil
	var reply raft.ResultadoRemoto
	for i, nodo := range nodos {
		fmt.Printf("Enviando op1 a %d\n", i)

		err := nodo.CallTimeout("NodoRaft.SometerOperacionRaft", op1,
			&reply, 5000*time.Millisecond)
		check.CheckError(err, "Error en llamada a nodo.SometerOperacionRaft")

		if reply.EsLider {
			fmt.Printf("Nodo %d es lider. Respuesta: %s\n", i, reply.ValorADevolver)

			fmt.Printf("Enviando op2 a %d\n", i)
			err = nodo.CallTimeout("NodoRaft.SometerOperacionRaft", op2, &reply, 5*time.Second)
			check.CheckError(err, "Error en llamada a nodo.SometerOperacionRaft")

			fmt.Printf("Respuesta en op2: %s\n", reply.ValorADevolver)
			break

		} else {
			fmt.Printf("Nodo %d no es lider.\n", i)
		}
	}
}
