package testintegracionraft1

import (
	"fmt"
	"raft/internal/comun/check"

	//"log"
	//"crypto/rand"
	// "os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"raft/internal/comun/rpctimeout"
	"raft/internal/despliegue"
	"raft/internal/raft"
)

const (
	//hosts: 192.168.3.2
	MAQUINA1 = "192.168.3.6"
	MAQUINA2 = "192.168.3.7"
	MAQUINA3 = "192.168.3.8"

	//puertos
	PUERTOREPLICA1 = "31072"
	PUERTOREPLICA2 = "31073"
	PUERTOREPLICA3 = "31074"

	//nodos replicas
	REPLICA1 = MAQUINA1 + ":" + PUERTOREPLICA1
	REPLICA2 = MAQUINA2 + ":" + PUERTOREPLICA2
	REPLICA3 = MAQUINA3 + ":" + PUERTOREPLICA3

	// paquete main de ejecutables relativos a PATH previo
	EXECREPLICA = "cmd/srvraft/main.go"

	// comandos completo a ejecutar en máquinas remota con ssh. Ejemplo :
	// 				cd $HOME/raft; go run cmd/srvraft/main.go 127.0.0.1:29001

	// Ubicar, en esta constante, nombre de fichero de vuestra clave privada local
	// emparejada con la clave pública en authorized_keys de máquinas remotas

	PRIVKEYFILE = "id_ed25519"
)

// PATH de los ejecutables de modulo golang de servicio Raft
var HOME string = "/misc/alumnos/sd/sd2324/a842748/"
var PATH string = filepath.Join(HOME, "p3")
// go run cmd/srvraft/main.go 0 127.0.0.1:29001 127.0.0.1:29002 127.0.0.1:29003
var EXECREPLICACMD string = "cd " + PATH + "; go run " + EXECREPLICA

// TEST primer rango
func TestPrimerasPruebas(t *testing.T) { // (m *testing.M) {
	// <setup code>
	// Crear canal de resultados de ejecuciones ssh en maquinas remotas
	cfg := makeCfgDespliegue(t,
		3,
		[]string{REPLICA1, REPLICA2, REPLICA3},
		[]bool{true, true, true})

	// tear down code
	// eliminar procesos en máquinas remotas
	defer cfg.stop()

	// Run test sequence
	
	// Test1 : No debería haber ningun primario, si SV no ha recibido aún latidos
	t.Run("T1:soloArranqueYparada",
		func(t *testing.T) { cfg.soloArranqueYparadaTest1(t) })
	
	// Test2 : No debería haber ningun primario, si SV no ha recibido aún latidos
	t.Run("T2:ElegirPrimerLider",
		func(t *testing.T) { cfg.elegirPrimerLiderTest2(t) })

	// Test3: tenemos el primer primario correcto
	t.Run("T3:FalloAnteriorElegirNuevoLider",
		func(t *testing.T) { cfg.falloAnteriorElegirNuevoLiderTest3(t) })

// 	// Test4: Tres operaciones comprometidas en configuración estable
	t.Run("T4:tresOperacionesComprometidasEstable",
		func(t *testing.T) { cfg.tresOperacionesComprometidasEstable(t) })

	}
// TEST primer rango
func TestAcuerdosConFallos(t *testing.T) { // (m *testing.M) {
	// <setup code>
	// Crear canal de resultados de ejecuciones ssh en maquinas remotas
	cfg := makeCfgDespliegue(t,
		3,
		[]string{REPLICA1, REPLICA2, REPLICA3},
		[]bool{true, true, true})

	// tear down code
	// eliminar procesos en máquinas remotas
	defer cfg.stop()

	// Test5: Se consigue acuerdo a pesar de desconexiones de seguidor
	t.Run("T5:AcuerdoAPesarDeDesconexionesDeSeguidor ",
		func(t *testing.T) { cfg.AcuerdoApesarDeSeguidor(t) })

	t.Run("T6:SinAcuerdoPorFallos ",
		func(t *testing.T) { cfg.SinAcuerdoPorFallos(t) })

	t.Run("T7:SometerConcurrentementeOperaciones ",
		func(t *testing.T) { cfg.SometerConcurrentementeOperaciones(t) })

}

// ---------------------------------------------------------------------
//
// Canal de resultados de ejecución de comandos ssh remotos
type canalResultados chan string

func (cr canalResultados) stop() {
	close(cr)

	// Leer las salidas obtenidos de los comandos ssh ejecutados
	for s := range cr {
		fmt.Println(s)
	}
}

// ---------------------------------------------------------------------
// Operativa en configuracion de despliegue y pruebas asociadas
type configDespliegue struct {
	t           *testing.T
	conectados  []bool
	numReplicas int
	nodosRaft   []rpctimeout.HostPort
	cr          canalResultados
}

// Crear una configuracion de despliegue
func makeCfgDespliegue(t *testing.T, n int, nodosraft []string,
	conectados []bool) *configDespliegue {
	cfg := &configDespliegue{}
	cfg.t = t
	cfg.conectados = conectados
	cfg.numReplicas = n
	cfg.nodosRaft = rpctimeout.StringArrayToHostPortArray(nodosraft)
	cfg.cr = make(canalResultados, 2000)

	return cfg
}

func (cfg *configDespliegue) stop() {
	//cfg.stopDistributedProcesses()

	time.Sleep(50 * time.Millisecond)

	cfg.cr.stop()
}

// --------------------------------------------------------------------------
// FUNCIONES DE SUBTESTS

// Se pone en marcha una replica ?? - 3 NODOS RAFT
func (cfg *configDespliegue) soloArranqueYparadaTest1(t *testing.T) {
	t.Skip("SKIPPED soloArranqueYparadaTest1")

	fmt.Println(t.Name(), ".....................")

	cfg.t = t // Actualizar la estructura de datos de tests para errores

	// Poner en marcha replicas en remoto con un tiempo de espera incluido
	cfg.startDistributedProcesses()

	time.Sleep(10 * time.Second)

	// Parar réplicas almacenamiento en remoto
	cfg.stopDistributedProcesses()

	fmt.Println(".............", t.Name(), "Superado")
}

// Primer lider en marcha - 3 NODOS RAFT
func (cfg *configDespliegue) elegirPrimerLiderTest2(t *testing.T) {
	t.Skip("SKIPPED ElegirPrimerLiderTest2")

	fmt.Println(t.Name(), ".....................")

	cfg.startDistributedProcesses()
	time.Sleep(10 * time.Second)

	// Se ha elegido lider ?
	fmt.Printf("Probando lider en curso\n")
	cfg.pruebaUnLider(3)

	// Parar réplicas alamcenamiento en remoto
	cfg.stopDistributedProcesses() // Parametros

	fmt.Println(".............", t.Name(), "Superado")
}

// Fallo de un primer lider y reeleccion de uno nuevo - 3 NODOS RAFT
func (cfg *configDespliegue) falloAnteriorElegirNuevoLiderTest3(t *testing.T) {
	t.Skip("SKIPPED FalloAnteriorElegirNuevoLiderTest3")

	fmt.Println(t.Name(), ".....................")

	cfg.startDistributedProcesses()
	time.Sleep(10 * time.Second)

	fmt.Printf("Lider inicial\n")
	lider := cfg.pruebaUnLider(3)

	fmt.Printf("El líder es el nodo %d\n", lider)

	// Se provoca un fallo en el líder
	cfg.pararNodo(lider)
	fmt.Printf("Nodo %d parado\n", lider)
	time.Sleep(10 * time.Second)

	// Se relanza el líder
	cfg.startOneDistributedProcess(lider)
	// Se comprueba si se ha elegido un nuevo líder

	fmt.Printf("Comprobar nuevo lider\n")
	lider = cfg.pruebaUnLider(3)
	fmt.Printf("El líder es el nodo %d\n", lider)

	// Parar réplicas almacenamiento en remoto
	cfg.stopDistributedProcesses() //parametros

	fmt.Println(".............", t.Name(), "Superado")
}

// 3 operaciones comprometidas con situacion estable y sin fallos - 3 NODOS RAFT
func (cfg *configDespliegue) tresOperacionesComprometidasEstable(t *testing.T) {
	t.Skip("SKIPPED tresOperacionesComprometidasEstable")

	fmt.Println(t.Name(), ".....................")

	// Iniciar procesos distribuidos
	cfg.startDistributedProcesses()
	time.Sleep(10 * time.Second)

	lider := cfg.pruebaUnLider(3)
	fmt.Printf("El lider inicial es el nodo %d\n", lider)

	// Comprobar que el lider lee el valor correcto en las variables correspondientes
	cfg.checkOp(lider, 0, raft.TipoOperacion{Operacion: "escribir", Clave: "clav1", Valor: "c"})
	cfg.checkOp(lider, 1, raft.TipoOperacion{Operacion: "leer", Clave: "clav1", Valor: "c"})
	cfg.checkOp(lider, 2, raft.TipoOperacion{Operacion: "escribir", Clave: "clav3", Valor: "b"})

	// Parar procesos distribuidos
	cfg.stopDistributedProcesses()

	fmt.Println(".............", t.Name(), "Supeado")
}

// Se consigue acuerdo a pesar de desconexiones de seguidor -- 3 NODOS RAFT
func (cfg *configDespliegue) AcuerdoApesarDeSeguidor(t *testing.T) {
	//t.Skip("SKIPPED AcuerdoApesarDeSeguidor")

	fmt.Println(t.Name(), ".....................")

	// Iniciar procesos distribuidos
	cfg.startDistributedProcesses()
	time.Sleep(10 * time.Second)

	lider := cfg.pruebaUnLider(3)
	fmt.Printf("El lider inicial es el nodo %d\n", lider)

	cfg.checkOp(lider, 0, raft.TipoOperacion{Operacion: "escribir", Clave: "clav5", Valor: "5"})

	// Matamos un nodo Raft
	nodoDesconectado := cfg.apagarUnNodo(lider)
	time.Sleep(3 * time.Second)

	// Comprobar varios acuerdos con una réplica desconectada
	cfg.checkOp(lider, 1, raft.TipoOperacion{Operacion: "escribir", Clave: "clav1", Valor: "3"})
	cfg.checkOp(lider, 2, raft.TipoOperacion{Operacion: "escribir", Clave: "clav1", Valor: "7"})
	
	time.Sleep(2 * time.Second) 

	// Reconectar nodo Raft previamente desconectado y comprobar varios acuerdos
	fmt.Printf("Vamos a reconectar el nodo %d\n", nodoDesconectado)
	cfg.encenderUnNodo(nodoDesconectado)

	time.Sleep(2 * time.Second) 

	cfg.checkOp(lider, 3, raft.TipoOperacion{Operacion: "leer", Clave: "clav1", Valor: ""})
	cfg.checkOp(lider, 4, raft.TipoOperacion{Operacion: "escribir", Clave: "clav7", Valor: "1"})
	cfg.checkOp(lider, 5, raft.TipoOperacion{Operacion: "leer", Clave: "clav3", Valor: "1"})

	cfg.stopDistributedProcesses()

	fmt.Println(".............", t.Name(), "Superado")

}

// NO se consigue acuerdo al desconectarse mayoría de seguidores -- 3 NODOS RAFT
func (cfg *configDespliegue) SinAcuerdoPorFallos(t *testing.T) {
	t.Skip("SKIPPED SinAcuerdoPorFallos")

	fmt.Println(t.Name(), ".....................")

	// Iniciar procesos distribuidos
	cfg.startDistributedProcesses()
	time.Sleep(10 * time.Second)

	//  Obtener un lider y, a continuación desconectar 2 de los nodos Raft
	lider := cfg.pruebaUnLider(3)
	fmt.Printf("El lider inicial es el nodo %d\n", lider)

	cfg.checkOp(lider, 0, raft.TipoOperacion{Operacion: "escribir", Clave: "b", Valor: "5"})
	time.Sleep(1 * time.Second)

	cfg.apagarSeguidores(lider)

	// Comprobar varios acuerdos con 2 réplicas desconectada 
	cfg.someterOperacionRepsDesconectadas(lider, 1, raft.TipoOperacion{Operacion: "escribir", Clave: "x", Valor: "9"})
	cfg.someterOperacionRepsDesconectadas(lider, 2, raft.TipoOperacion{Operacion: "escribir", Clave: "y", Valor: "2"})
	cfg.someterOperacionRepsDesconectadas(lider, 3, raft.TipoOperacion{Operacion: "escribir", Clave: "z", Valor: "5"})

	// Reconectar los nodos Raft desconectados y probar varios acuerdos
	cfg.encenderSeguidores(lider)
	time.Sleep(5 * time.Second)

	cfg.checkOp(lider, 4, raft.TipoOperacion{Operacion: "leer", Clave: "x", Valor: "9"})
	cfg.checkOp(lider, 5, raft.TipoOperacion{Operacion: "leer", Clave: "y", Valor: "2"})
	cfg.checkOp(lider, 6, raft.TipoOperacion{Operacion: "leer", Clave: "z", Valor: "5"})
	cfg.checkOp(lider, 7, raft.TipoOperacion{Operacion: "leer", Clave: "b", Valor: "5"})

	cfg.stopDistributedProcesses()

	fmt.Println(".............", t.Name(), "Superado")
}

// Se somete 5 operaciones de forma concurrente -- 3 NODOS RAFT
func (cfg *configDespliegue) SometerConcurrentementeOperaciones(t *testing.T) {
	t.Skip("SKIPPED SometerConcurrentementeOperaciones")

	fmt.Println(t.Name(), ".....................")

	// Iniciar procesos distribuidos
	cfg.startDistributedProcesses()
	time.Sleep(10 * time.Second)

	//  Obtener un lider y, a continuación desconectar 2 de los nodos Raft
	lider := cfg.pruebaUnLider(3)
	fmt.Printf("El lider inicial es el nodo %d\n", lider)

	cfg.checkOp(lider, 0, raft.TipoOperacion{Operacion: "escribir", Clave: "b", Valor: "5"})
	time.Sleep(1 * time.Second)

	// Someter 5 operaciones concurrentes
	go cfg.someterOperacion(lider, raft.TipoOperacion{Operacion: "escribir", Clave: "x", Valor: "9"})
	go cfg.someterOperacion(lider, raft.TipoOperacion{Operacion: "escribir", Clave: "y", Valor: "2"})
	go cfg.someterOperacion(lider, raft.TipoOperacion{Operacion: "escribir", Clave: "z", Valor: "5"})
	go cfg.someterOperacion(lider, raft.TipoOperacion{Operacion: "leer", Clave: "a", Valor: ""})
	go cfg.someterOperacion(lider, raft.TipoOperacion{Operacion: "leer", Clave: "c", Valor: ""})

	time.Sleep(5 * time.Second)

	cfg.checkEstadoRegistro(6)

	cfg.stopDistributedProcesses()

	fmt.Println(".............", t.Name(), "Superado")
}




// --------------------------------------------------------------------------
// FUNCIONES DE APOYO
// Comprobar que hay un solo lider
// probar varias veces si se necesitan reelecciones
func (cfg *configDespliegue) pruebaUnLider(numreplicas int) int {
	for iters := 0; iters < 10; iters++ {
		time.Sleep(500 * time.Millisecond)
		mapaLideres := make(map[int][]int)
		for i := 0; i < numreplicas; i++ {
			if cfg.conectados[i] {
				if _, mandato, eslider, _ := cfg.obtenerEstadoRemoto(i); eslider {
					mapaLideres[mandato] = append(mapaLideres[mandato], i)
				}
			}
		}

		ultimoMandatoConLider := -1
		for mandato, lideres := range mapaLideres {
			if len(lideres) > 1 {
				cfg.t.Fatalf("mandato %d tiene %d (>1) lideres",
					mandato, len(lideres))
			}
			if mandato > ultimoMandatoConLider {
				ultimoMandatoConLider = mandato
			}
		}

		if len(mapaLideres) != 0 {

			return mapaLideres[ultimoMandatoConLider][0] // Termina

		}
	}
	cfg.t.Fatalf("un lider esperado, ninguno obtenido")

	return -1 // Termina
}


// obtenerEstadoRemoto obtiene el estado remoto de un nodo específico.
// Devuelve el ID del nodo, el mandato, si es líder y el ID del líder.
func (cfg *configDespliegue) obtenerEstadoRemoto(
	indiceNodo int) (int, int, bool, int) {
	var reply raft.EstadoRemoto
	err := cfg.nodosRaft[indiceNodo].CallTimeout("NodoRaft.ObtenerEstadoNodo",
		raft.Vacio{}, &reply, 10*time.Millisecond)
	check.CheckError(err, "Error en llamada RPC ObtenerEstadoRemoto")

	return reply.IdNodo, reply.Mandato, reply.EsLider, reply.IdLider
}


// start  gestor de vistas; mapa de replicas y maquinas donde ubicarlos;
// y lista clientes (host:puerto)
func (cfg *configDespliegue) startDistributedProcesses() {
	//cfg.t.Log("Before starting following distributed processes: ", cfg.nodosRaft)
  
	for i, endPoint := range cfg.nodosRaft {
		despliegue.ExecMutipleHosts(EXECREPLICACMD+
			" "+strconv.Itoa(i)+" "+
			rpctimeout.HostPortArrayToString(cfg.nodosRaft),
			[]string{endPoint.Host()}, cfg.cr, PRIVKEYFILE)

		// dar tiempo para se establezcan las replicas
		//time.Sleep(500 * time.Millisecond)
	}

	// aproximadamente 500 ms para cada arranque por ssh en portatil
	time.Sleep(2500 * time.Millisecond)
}


// start  gestor de vistas; mapa de replicas y maquinas donde ubicarlos;
// y lista clientes (host:puerto)
func (cfg *configDespliegue) startOneDistributedProcess(nodo int) {
	//cfg.t.Log("Before starting following distributed processes: ", cfg.nodosRaft)
	
	for i, endPoint := range cfg.nodosRaft {
		if nodo == i {
			despliegue.ExecMutipleHosts(EXECREPLICACMD+
				" "+strconv.Itoa(i)+" "+
				rpctimeout.HostPortArrayToString(cfg.nodosRaft),
				[]string{endPoint.Host()}, cfg.cr, PRIVKEYFILE)
		}
	}
	// aproximadamente 500 ms para cada arranque por ssh en portatil
	time.Sleep(2500 * time.Millisecond)
}


// stopDistributedProcesses detiene los procesos distribuidos en los nodos Raft.
func (cfg *configDespliegue) stopDistributedProcesses() {
	var reply raft.Vacio

	for _, endPoint := range cfg.nodosRaft {
		err := endPoint.CallTimeout("NodoRaft.ParaNodo",
			raft.Vacio{}, &reply, 10*time.Millisecond)
		check.CheckError(err, "Error en llamada RPC Para nodo")
	}
}


// Comprobar estado remoto de un nodo con respecto a un estado prefijado
func (cfg *configDespliegue) comprobarEstadoRemoto(idNodoDeseado int,
	mandatoDeseado int, esLiderDeseado bool, IdLiderDeseado int) {
	idNodo, mandato, esLider, idLider := cfg.obtenerEstadoRemoto(idNodoDeseado)

	//cfg.t.Log("Estado replica 0: ", idNodo, mandato, esLider, idLider, "\n")

	if idNodo != idNodoDeseado || mandato != mandatoDeseado ||
		esLider != esLiderDeseado || idLider != IdLiderDeseado {
		cfg.t.Fatalf("Estado incorrecto en replica %d en subtest %s",
			idNodoDeseado, cfg.t.Name())
	}

}


// pararNodo detiene un nodo específico en el clúster Raft.
// Recibe como parámetro el número de nodo a detener.
// Realiza una llamada RPC al nodo para ejecutar el método ParaNodo.
// Retorna un error en caso de que ocurra algún problema durante la llamada RPC.
func (cfg *configDespliegue) pararNodo(node int) {
	var reply raft.Vacio
	err := cfg.nodosRaft[node].CallTimeout("NodoRaft.ParaNodo",
		raft.Vacio{}, &reply, 10*time.Millisecond)
	check.CheckError(err, "Error en llamada RPC Para nodo")
}


// apagarUnNodo apaga un nodo en el clúster de Raft.
// Recibe el índice del líder como parámetro y devuelve el índice del nodo apagado.
// Si no se puede apagar ningún nodo, devuelve -1.
func (cfg *configDespliegue) apagarUnNodo(lider int) int {
	var reply raft.Vacio
	var res int = -1
	for i := 0; i < 3; i++ {
		if i != lider {
			for j, endPoint := range cfg.nodosRaft {
				if j == i {
					err := endPoint.CallTimeout("NodoRaft.KillNode", raft.Vacio{}, &reply, 10*time.Millisecond)
					check.CheckError(err, "Error en llamada RPC KillNode\n")
					cfg.conectados[i] = false
				}
			}
			res = i
			fmt.Printf("Nodo %d matado\n", i)
			break
		}
	}
	return res
}


// apagarSeguidores apaga los seguidores del líder especificado.
// El parámetro "lider" indica el índice del líder en la lista de nodosRaft.
// Los seguidores se apagan llamando al método remoto "NodoRaft.KillNode" en cada nodoRaft
// excepto en el líder. Después de apagar un seguidor, se marca como desconectado en la
// configuración de despliegue.
func (cfg *configDespliegue) apagarSeguidores(lider int) {
	var reply raft.Vacio
	for i := 0; i < 3; i++ {
		if i != lider {
			for j, endPoint := range cfg.nodosRaft {
				if j == i {
					err := endPoint.CallTimeout("NodoRaft.KillNode", raft.Vacio{}, &reply, 10*time.Millisecond)
					check.CheckError(err, "Error en llamada RPC KillNode\n")
					cfg.conectados[i] = false
				}
			}
			fmt.Printf("Nodo %d matado\n", i)
		}
	}
}


// encenderUnNodo enciende un nodo específico en el clúster Raft.
// Recibe el identificador del nodo a encender.
// Si el nodo se encuentra en la lista de nodos del clúster, se realiza una llamada RPC para revivirlo.
// La función actualiza el estado de conexión del nodo en la configuración del despliegue.
// Imprime un mensaje indicando que el nodo ha sido revivido.
func (cfg *configDespliegue) encenderUnNodo(idNodo int) {
	var reply raft.Vacio
	for j, endPoint := range cfg.nodosRaft {
		if j == idNodo {
			err := endPoint.CallTimeout("NodoRaft.ReviveNode", raft.Vacio{}, &reply, 10*time.Millisecond)
			check.CheckError(err, "Error en llamada RPC ReviveUnNodo\n")
			cfg.conectados[j] = true
			fmt.Printf("Nodo %d revivido\n", j)
			break
		}
	}
}


// encenderSeguidores enciende los seguidores en el clúster de nodos Raft, excepto el líder especificado.
// Recibe el índice del líder como parámetro.
// Realiza llamadas RPC para revivir los nodos y actualiza el estado de conexión de los nodos en la configuración.
func (cfg *configDespliegue) encenderSeguidores(lider int) {
	var reply raft.Vacio
	for i := 0; i < 3; i++ {
		if i != lider {
			for j, endPoint := range cfg.nodosRaft {
				if j == i {
					err := endPoint.CallTimeout("NodoRaft.ReviveNode", raft.Vacio{}, &reply, 10*time.Millisecond)
					check.CheckError(err, "Error en llamada RPC ReviveUnNodo\n")
					cfg.conectados[j] = true
				}
			}
			fmt.Printf("Nodo %d revivido\n", i)
		}
	}
}


// someterOperacion realiza una operación en el líder del clúster Raft y devuelve los resultados.
// Recibe el índice del líder y la operación a realizar.
// Retorna el índice del registro, el mandato, si el líder es el actual, el ID del líder y el valor a devolver.
func (cfg *configDespliegue) someterOperacion(indiceLider int, operacion raft.TipoOperacion) (int, int, bool, int, string) {
	var reply raft.ResultadoRemoto
	err := cfg.nodosRaft[indiceLider].CallTimeout("NodoRaft.SometerOperacionRaft", operacion, &reply, 10000*time.Millisecond)
	check.CheckError(err, "Error en llamada RPC SometerOperacionRaft\n")

	fmt.Printf("Operación sometida correctamente. Operacion: %v \n", operacion)

	return reply.IndiceRegistro, reply.Mandato, reply.EsLider, reply.IdLider, reply.ValorADevolver
}


// someterOperacionRepsDesconectadas es una función que se utiliza para someter una operación a los nodos Raft desconectados en un despliegue de prueba.
// Recibe el ID del líder, el índice del registro y la operación a someter.
// Si la operación se somete correctamente, no se produce ningún error.
// Si no se puede llegar a un acuerdo para someter la nueva entrada, se imprime un mensaje indicando el índice del registro.
func (cfg *configDespliegue) someterOperacionRepsDesconectadas(idLider int, indiceRegistro int, operacion raft.TipoOperacion) {
	var reply raft.ResultadoRemoto
	err := cfg.nodosRaft[idLider].CallTimeout("NodoRaft.SometerOperacionRaft", operacion, &reply, 10000*time.Millisecond)
	if err == nil {
		cfg.t.Fatalf("Error: Operación incorrecta en el subtest %s\n", cfg.t.Name())

	} else {
		fmt.Printf("No se pudo llegar a un acuerdo para someter la nueva entrada de indice %d (ok)\n", indiceRegistro)
	}

}


// checkOp verifica si una operación se somete correctamente y produce los resultados esperados.
// Recibe el índice del líder, el índice del registro, y el tipo de operación a someter.
// Imprime el índice y el índice del registro.
// Si la operación no se somete correctamente o produce resultados incorrectos, se produce un error fatal.
// En caso contrario, se imprime un mensaje indicando que la operación se sometió correctamente.
func (cfg *configDespliegue) checkOp(idxLider int, indiceRegistro int, operacion raft.TipoOperacion) {
    indice, _, _, idLider, valorADevolver := cfg.someterOperacion(idxLider, operacion)
	fmt.Printf("Indice: %d, IndiceRegistro: %d\n", indice, indiceRegistro)
    if indice != indiceRegistro || idxLider != idLider || valorADevolver != operacion.Valor {
        cfg.t.Fatalf("Operación no sometida correctamente en índice %d. Operacion: %v\n", indiceRegistro, operacion)
    } else {
		fmt.Printf("Operación sometida correctamente en índice %d. Operacion: %v \n", indiceRegistro, operacion)
	}
}


// checkEstadoRegistro verifica el estado del registro para un índice de registro dado en todas las réplicas.
// Si el índice de registro en la réplica 0 no coincide con el índice de registro dado, se produce un error fatal.
// Además, si el estado del registro en alguna réplica difiere del estado en la réplica 0, se produce un error fatal.
func (cfg *configDespliegue) checkEstadoRegistro(indiceRegistro int) {
	indices := make([]int, cfg.numReplicas)
	mandatos := make([]int, cfg.numReplicas)

	for i, _ := range cfg.nodosRaft {
		indices[i], mandatos[i] = cfg.obtenerEstadoRegistro(i)
	}

	if indices[0] != indiceRegistro {
		cfg.t.Fatalf("Indice de registro incorrecto en replica 0 en subtest %s", cfg.t.Name())
	}

	for i := 1; i < cfg.numReplicas; i++ {
		if indices[i] != indices[0] || mandatos[i] != mandatos[0] {
			cfg.t.Fatalf("Estado de registro incorrecto en replica %d en subtest %s", i, cfg.t.Name())
		}
	}
}


// obtenerEstadoRegistro devuelve el estado del registro para el nodo especificado.
// Recibe el ID del nodo y devuelve el índice y el término del registro.
func (cfg *configDespliegue) obtenerEstadoRegistro(idNodo int) (int, int) {
	var reply raft.ResultadoRegistro
	err := cfg.nodosRaft[idNodo].CallTimeout("NodoRaft.ObtenerEstadoRegistro", raft.Vacio{}, &reply, 50*time.Millisecond)
	check.CheckError(err, "Error en llamada RPC ObtenerEstadoRegistro")

	return reply.Index, reply.Term
}
