// Escribir vuestro código de funcionalidad Raft en este fichero
//

package raft

//
// API
// ===
// Este es el API que vuestra implementación debe exportar
//
// nodoRaft = NuevoNodo(...)
//   Crear un nuevo servidor del grupo de elección.
//
// nodoRaft.Para()
//   Solicitar la parado de un servidor
//
// nodo.ObtenerEstado() (yo, mandato, esLider)
//   Solicitar a un nodo de elección por "yo", su mandato en curso,
//   y si piensa que es el msmo el lider
//
// nodoRaft.SometerOperacion(operacion interface()) (indice, mandato, esLider)

// type AplicaOperacion

import (
	// "errors"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"

	//"crypto/rand"
	"sync"
	"time"

	//"net/rpc"

	//"raft/internal/comun/check"
	"raft/internal/comun/rpctimeout"
)

const (
	// Constante para fijar valor entero no inicializado
	IntNOINICIALIZADO = -1

	//  false deshabilita por completo los logs de depuracion
	// Aseguraros de poner kEnableDebugLogs a false antes de la entrega
	kEnableDebugLogs = true

	// Poner a true para logear a stdout en lugar de a fichero
	kLogToStdout = false

	// Cambiar esto para salida de logs en un directorio diferente
	kLogOutputDir = "./logs_raft/"

	// Tiempo de espera para la reeleccion
	ReelectionTimer = 2.5

)

type TipoOperacion struct {
	Operacion string // La operaciones posibles son "leer" y "escribir"
	Clave     string // en el caso de la lectura Clave = ""
	Valor     string // en el caso de la lectura Valor = ""
}

// A medida que el nodo Raft conoce las operaciones de las  entradas de registro
// comprometidas, envía un AplicaOperacion, con cada una de ellas, al canal
// "canalAplicar" (funcion NuevoNodo) de la maquina de estados
type AplicaOperacion struct {
	Indice    int // en la entrada de registro
	Operacion TipoOperacion // Operacion a aplicar
}

type Entry struct {
	Term      int           // Term en el que se ha creado la entrada
	Operacion TipoOperacion // Operacion a realizar
	Index	  int 			// Indice de la entrada
}

// Tipo de dato Go que representa un solo nodo (réplica) de raft
type NodoRaft struct {
	Mux sync.Mutex // Mutex para proteger acceso a estado compartido

	// Host:Port de todos los nodos (réplicas) Raft, en mismo orden
	Nodos   []rpctimeout.HostPort 	// Host:Port de todos los nodos (réplicas) Raft, en mismo orden
	Yo      int 					// indice de este nodos en campo array "nodos"
	IdLider int 					// indice del lider en campo array "nodos"

	// Utilización opcional de este logger para depuración
	// Cada nodo Raft tiene su propio registro de trazas (logs)
	Logger *log.Logger

	// Vuestros datos aqui.
	FollowerChan chan bool // Canal para pasar a follower
	LeaderChan   chan bool // Canal para pasar a leader
	Heartbeat    chan bool // Canal para enviar heartbeats

	Rol string // Rol del nodo (Follower, Candidate, Leader)

	// RaftState
	CurrentTerm int 		// Ultimo mandato conocido
	VotedFor    int 		// Indice del nodo al que se ha votado en el mandato. Lider en ese momento.
	Log 		[]Entry		// Entradas de registro.

	// NodeState
	CommitIndex int 		// Indice del registro mas alto conocido que se ha comprometido
	LastApplied int 		// Indice del registro mas alto conocido que se ha aplicado a la maquina de estados
	NumVotes    int 		// Numero de votos recibidos en la eleccion actual
	NumSucess   int			// Numero de sucess recibidos en el appendEntries 

	// LeaderState
	NextIndex  []int 		// Para cada nodo, indice del siguiente registro a enviar. Inicializado a LastLogIndex + 1
	MatchIndex []int 		// Para cada nodo, indice del ultimo registro que se sabe que el nodo ha replicado en el cluster. Inicializado a 0

	AplicarOperacion chan AplicaOperacion // Canal para pasar operaciones a aplicar a la maquina de estados
	Commited   		 chan string		  // Canal para pasar el valor a devolver al cliente

	alive bool	// Para saber si el nodo esta vivo o no
}


// Creacion de un nuevo nodo de eleccion
//
// Tabla de <Direccion IP:puerto> de cada nodo incluido a si mismo.
//
// <Direccion IP:puerto> de este nodo esta en nodos[yo]
//
// Todos los arrays nodos[] de los nodos tienen el mismo orden

// canalAplicar es un canal donde, en la practica 5, se recogerán las
// operaciones a aplicar a la máquina de estados. Se puede asumir que
// este canal se consumira de forma continúa.
//
// NuevoNodo() debe devolver resultado rápido, por lo que se deberían
// poner en marcha Gorutinas para trabajos de larga duracion
func NuevoNodo(nodos []rpctimeout.HostPort, yo int,
	canalAplicarOperacion chan AplicaOperacion) *NodoRaft {
	nr := &NodoRaft{}
	nr.Nodos = nodos
	nr.Yo = yo
	nr.IdLider = -1

	if kEnableDebugLogs {
		nombreNodo := nodos[yo].Host() + "_" + nodos[yo].Port()
		logPrefix := fmt.Sprintf("%s", nombreNodo)

		fmt.Println("LogPrefix: ", logPrefix)

		if kLogToStdout {
			nr.Logger = log.New(os.Stdout, nombreNodo+" -->> ",
				log.Lmicroseconds|log.Lshortfile)
		} else {
			err := os.MkdirAll(kLogOutputDir, os.ModePerm)
			if err != nil {
				panic(err.Error())
			}
			logOutputFile, err := os.OpenFile(fmt.Sprintf("%s/%s.txt",
				kLogOutputDir, logPrefix), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0755)
			if err != nil {
				panic(err.Error())
			}
			nr.Logger = log.New(logOutputFile,
				logPrefix+" -> ", log.Lmicroseconds|log.Lshortfile)
		}
		nr.Logger.Println("logger initialized")
	}else {
		nr.Logger = log.New(ioutil.Discard, "", 0)
	}

	// Codigo de inicialización de los atributos de un nodo Raft
	nr.FollowerChan = make(chan bool)
	nr.LeaderChan = make(chan bool)
	nr.Heartbeat = make(chan bool)
	nr.Rol = "Follower"
	nr.CurrentTerm = 0
	nr.VotedFor = -1	
	nr.Log = []Entry{}
	nr.alive = true

	nr.CommitIndex = 0
	nr.LastApplied = 0
	nr.NumVotes = 0
	nr.NumSucess = 0

	nr.LastApplied = 0
	nr.NextIndex = make([]int, len(nr.Nodos))
	nr.MatchIndex = make([]int, len(nr.Nodos))

	nr.AplicarOperacion = canalAplicarOperacion
	nr.Commited = make(chan string)

	time.Sleep(3 * time.Second)
	go nr.automataRaft()

	return nr
}



// para es un método que detiene la ejecución del nodo actual.
// Crea una goroutine que duerme durante 5 milisegundos y luego sale del programa.
func (nr *NodoRaft) para() {
	go func() { time.Sleep(5 * time.Millisecond); os.Exit(0) }()
}


// obtenerEstado devuelve el estado actual del nodo Raft.
// Devuelve los siguientes valores:
// - yo: el identificador del nodo.
// - mandato: el término actual del nodo.
// - esLider: un indicador booleano que indica si el nodo es el líder.
// - idLider: el identificador del líder actual.
func (nr *NodoRaft) obtenerEstado() (int, int, bool, int) {
	var yo int = nr.Yo
	var mandato int = nr.CurrentTerm
	var esLider bool
	var idLider int = nr.IdLider

	if nr.Yo == nr.IdLider {
		esLider = true
	}else {
		esLider = false
	}
	return yo, mandato, esLider, idLider
}


// El servicio que utilice Raft (base de datos clave/valor, por ejemplo)
// Quiere buscar un acuerdo de posicion en registro para siguiente operacion
// solicitada por cliente.

// Si el nodo no es el lider, devolver falso
// Sino, comenzar la operacion de consenso sobre la operacion y devolver en
// cuanto se consiga
//
// No hay garantia que esta operacion consiga comprometerse en una entrada de
// de registro, dado que el lider puede fallar y la entrada ser reemplazada
// en el futuro.
// Primer valor devuelto es el indice del registro donde se va a colocar
// la operacion si consigue comprometerse.
// El segundo valor es el mandato en curso
// El tercer valor es true si el nodo cree ser el lider
// Cuarto valor es el lider, es el indice del líder si no es él
func (nr *NodoRaft) someterOperacion(operacion TipoOperacion) (int, int, bool, int, string) {
	// Si el nodo no está activo, esperar y devolver un resultado indicando que está muerto
	if !nr.alive {
		time.Sleep(400 * time.Millisecond)
		return -1, -1, false, -1, "muerto"
	}

	// Inicializar variables
	indice := -1
	mandato := nr.CurrentTerm
	EsLider := nr.Yo == nr.IdLider
	idLider := nr.IdLider
	valorADevolver := ""

	// Verificar si el nodo actual es el líder
	if EsLider {
		// Bloquear el acceso al log mientras se realiza la operación
		nr.Mux.Lock()

		// Obtener el índice actualizado después de la operación
		indice = nr.CommitIndex
		// Crear una nueva entrada en el log con la operación proporcionada
		entry := Entry{Term: mandato, Operacion: operacion, Index: indice}
		// Agregar la entrada al log
		nr.Log = append(nr.Log, entry)

		// Registrar la operación y el estado actualizado del log
		nr.Logger.Printf("Entrada añadida al log, ENTRADA NUEVA: (Indice: %d, Mandato: %d, Operacion: %s, Valor: %s, Clave: %s)\n"+
			"Log actualizado, actualmente es: %v\n", entry.Index, entry.Term, entry.Operacion.Operacion, entry.Operacion.Valor, entry.Operacion.Clave, nr.Log)

		// Desbloquear el acceso al log
		nr.Mux.Unlock()

		// Esperar a que la operación se confirme y obtener el valor devuelto
		valorADevolver = <-nr.Commited
		// Actualizar el ID del líder
		idLider = nr.Yo
	}

	// Devolver los resultados del proceso de sometimiento de operación
	return indice, mandato, EsLider, idLider, valorADevolver
}


// -----------------------------------------------------------------------
// LLAMADAS RPC al API
//
// Si no tenemos argumentos o respuesta estructura vacia (tamaño cero)
type Vacio struct{}

// ParaNodo detiene el nodo Raft.
func (nr *NodoRaft) ParaNodo(args Vacio, reply *Vacio) error {
	defer nr.para()
	return nil
}

type EstadoParcial struct {
	Mandato int
	EsLider bool
	IdLider int
}

type EstadoRemoto struct {
	IdNodo int
	EstadoParcial
}

type ResultadoRemoto struct {
	ValorADevolver string
	IndiceRegistro int
	EstadoParcial
}

type ResultadoRegistro struct {
	Index int
	Term  int
}

// ObtenerEstadoNodo obtiene y devuelve el estado actual del nodo Raft.
func (nr *NodoRaft) ObtenerEstadoNodo(args Vacio, reply *EstadoRemoto) error {
	// Obtener el estado del nodo y asignarlo a la respuesta remota
	reply.IdNodo, reply.Mandato, reply.EsLider, reply.IdLider = nr.obtenerEstado()
	return nil
}

// ObtenerEstadoRegistro obtiene y devuelve el estado actual del registro del nodo Raft.
func (nr *NodoRaft) ObtenerEstadoRegistro(args Vacio, reply *ResultadoRegistro) error {
	// Bloquear el acceso al log mientras se obtiene el estado del registro
	nr.Mux.Lock()
	reply.Term, reply.Index = nr.getEstadoRegistro()
	nr.Mux.Unlock()
	return nil
}

// getEstadoRegistro devuelve el término y el índice del estado actual del registro del nodo Raft.
func (nr *NodoRaft) getEstadoRegistro() (int, int) {
	if len(nr.Log) != 0 {
		return nr.Log[nr.CommitIndex-1].Term, nr.CommitIndex
	}
	return 0, -1
}

// SometerOperacionRaft lleva a cabo el proceso de sometimiento de una operación al nodo Raft.
func (nr *NodoRaft) SometerOperacionRaft(operacion TipoOperacion, reply *ResultadoRemoto) error {
	// Obtener resultados del proceso de sometimiento de operación y asignarlos a la respuesta remota
	reply.IndiceRegistro, reply.Mandato, reply.EsLider, reply.IdLider, reply.ValorADevolver = nr.someterOperacion(operacion)
	return nil
}


// -----------------------------------------------------------------------
// LLAMADAS RPC protocolo RAFT
//
// Structura de ejemplo de argumentos de RPC PedirVoto.
//
// Recordar
// -----------
// Nombres de campos deben comenzar con letra mayuscula !
type ArgsPeticionVoto struct {
	// Vuestros datos aqui
	Term         int // Termino del candidato
	CandidateId  int // Id del candidato que solicita el voto
	LastLogIndex int // Indice del ultimo registro del candidato
	LastLogTerm  int // Termino del ultimo registro del candidato
}

// Structura de ejemplo de respuesta de RPC PedirVoto,
//
// Recordar
// -----------
// Nombres de campos deben comenzar con letra mayuscula !
type RespuestaPeticionVoto struct {
	// Vuestros datos aqui
	Term        int  // Termino actual del nodo para que el candidato se actualice
	VoteGranted bool // True si el voto se ha concedido
}


// solicitarVotos es una función que se encarga de enviar solicitudes de voto a todos los nodos en el clúster.
// Si el nodo no está activo, la función espera y sale.
// Utiliza goroutines para enviar las solicitudes de voto de forma concurrente.
// Registra la solicitud de voto enviada en el registro de eventos del nodo.
func solicitarVotos(nr *NodoRaft) {
	// Si el nodo no está activo, esperar y salir de la función
	if !nr.alive {
		time.Sleep(400 * time.Millisecond)
		return
	}

	// Declarar variables para la respuesta y el mutex
	var reply RespuestaPeticionVoto
	var mx sync.Mutex

	// Iterar sobre todos los nodos en el clúster
	for i := 0; i < len(nr.Nodos); i++ {
		// Evitar enviar una solicitud de voto a sí mismo
		if i != nr.Yo {
			// Iniciar una goroutine para enviar la solicitud de voto al nodo i
			go enviarPeticionVoto(nr, i, &mx, &reply, &ArgsPeticionVoto{
				Term:         nr.CurrentTerm,
				CandidateId:  nr.Yo,
				LastLogIndex: len(nr.Log) - 1,
				LastLogTerm:  0,
			})
			// Registrar la solicitud de voto enviada
			nr.Logger.Printf("Peticion de voto enviada a %d. Los argumentos son: Term: %d, Yo: %d, LastLogIndex: %d, LastLogTerm: %d\n",
				i, nr.CurrentTerm, nr.Yo, len(nr.Log), 0)
		}
	}
}



// enviarPeticionVoto envía una solicitud de voto al nodo especificado en el clúster Raft.
// Devuelve true si la solicitud fue enviada y procesada correctamente, false en caso contrario.
func enviarPeticionVoto(nr *NodoRaft, node int, mx *sync.Mutex, reply *RespuestaPeticionVoto, args *ArgsPeticionVoto) bool {
	// Si el nodo no está activo, esperar y devolver false
	if !nr.alive {
		time.Sleep(400 * time.Millisecond)
		return false
	}

	// Actualizar LastLogTerm si el log no está vacío
	if len(nr.Log) > 0 {
		args.LastLogTerm = nr.Log[len(nr.Log)-1].Term
	}

	// Realizar la llamada remota para enviar la solicitud de voto al nodo
	err := nr.Nodos[node].CallTimeout("NodoRaft.PedirVoto", args, reply, 300*time.Millisecond)

	// Verificar si hubo un error en la llamada remota
	if err != nil {
		return false
	}

	// Bloquear el acceso a secciones críticas con el mutex
	mx.Lock()

	// Actualizar el CurrentTerm si el término en la respuesta es mayor
	if reply.Term > nr.CurrentTerm {
		nr.CurrentTerm = reply.Term
		nr.Logger.Printf("Su mandato es mayor: Mi mandato es: %d, el suyo es: %d (entonces, vuelvo a follower)\n", nr.CurrentTerm, reply.Term)
		nr.FollowerChan <- true
	} else if reply.VoteGranted {
		// Si se otorga el voto, incrementar el contador de votos y verificar la mayoría
		nr.NumVotes++
		nr.Logger.Printf("Me ha concedido su voto %d\n", node)
		// Si se alcanza la mayoría, el nodo se convierte en líder
		if nr.NumVotes > len(nr.Nodos)/2 {
			nr.IdLider = nr.Yo
			nr.LeaderChan <- true
		}
	} else {
		// Si no se otorga el voto, volver a estado de seguidor
		nr.FollowerChan <- true
	}

	// Desbloquear el acceso a secciones críticas
	mx.Unlock()

	// Devolver true indicando que la solicitud fue enviada y procesada correctamente
	return true
}



// Metodo para RPC PedirVoto
// PedirVoto procesa una solicitud de voto recibida de un candidato en el clúster Raft.
func (nr *NodoRaft) PedirVoto(peticion *ArgsPeticionVoto, reply *RespuestaPeticionVoto) error {
	// Si el nodo no está activo, esperar y salir de la función sin procesar la solicitud
	if !nr.alive {
		time.Sleep(500 * time.Millisecond)
		return nil
	}

	// Bloquear el acceso al estado crítico del nodo con el mutex
	nr.Mux.Lock()

	// Responder con false si el término en la solicitud es menor que el término actual
	if peticion.Term < nr.CurrentTerm {
		reply.VoteGranted = false
		reply.Term = nr.CurrentTerm
		nr.Logger.Printf("Voto denegado a %d, porque su término es menor al mío\n", peticion.CandidateId)

	// Verificar si el término en la solicitud es mayor y comprobaciones adicionales para conceder el voto
	} else if nr.CurrentTerm < peticion.Term && shouldGrantVote(nr, peticion) {
		// Actualizar el término, otorgar el voto, y notificar el cambio a seguidor
		nr.CurrentTerm = peticion.Term
		reply.Term = nr.CurrentTerm
		nr.VotedFor = peticion.CandidateId
		nr.Logger.Printf("Voto concedido a %d\n", peticion.CandidateId)
		nr.FollowerChan <- true
		reply.VoteGranted = true
	}

	// Desbloquear el acceso al estado crítico del nodo
	nr.Mux.Unlock()

	return nil
}

// If voted for is null or candidateId, and candidates log is at least as up-to-date as receivers log, grant vote
func shouldGrantVote(nr *NodoRaft, peticion *ArgsPeticionVoto) bool {
	if nr.VotedFor == -1 || nr.VotedFor == peticion.CandidateId || len(nr.Log) == 0 || peticion.LastLogTerm >= nr.Log[len(nr.Log)-1].Term || 
	(peticion.LastLogTerm == nr.Log[len(nr.Log)-1].Term && peticion.LastLogIndex >= len(nr.Log)-1) {
		return true
	}
	return false
}



// automataRaft es la implementación del autómata de estado del protocolo Raft para el nodo Raft.
// Controla el comportamiento del nodo Raft en los diferentes roles: Follower, Candidate y Leader.
func (nr *NodoRaft) automataRaft() {
	// Identificación del nodo para mayor claridad en el log
	nr.Logger.Printf("Soy el nodo %d\n", nr.Yo)

	for {
		// Verificar si el nodo está activo
		if nr.alive {
			// Aplicar operaciones si hay cambios en el CommitIndex
			if nr.CommitIndex > nr.LastApplied {
				nr.Mux.Lock()
				nr.LastApplied++
				nr.AplicarOperacion <- AplicaOperacion{Indice: nr.LastApplied, Operacion: nr.Log[nr.LastApplied-1].Operacion}
				nr.Mux.Unlock()
			}

			// Si el nodo es seguidor;
			if nr.Rol == "Follower" {
				select {
				case <-nr.Heartbeat:
					nr.Rol = "Follower"
				case <-nr.FollowerChan:
					nr.Rol = "Follower"
				
				// Si expira el timeout, el nodo se convierte en candidato
				case <-time.After(generarTimeout()):
					if nr.alive {
						nr.Mux.Lock()
						nr.Rol = "Candidate"
						nr.IdLider = -1
						nr.Mux.Unlock()
						nr.Logger.Printf("Timeout expirado. Paso a presentarme como candidato\n")
					}
				}

			// Si el nodo es candidato;
			} else if nr.Rol == "Candidate" {
				// Iniciar el proceso de elección como candidato
				nr.Mux.Lock()
				nr.VotedFor = nr.Yo
				nr.NumVotes = 1
				nr.CurrentTerm++
				nr.Mux.Unlock()
				solicitarVotos(nr)

				// Manejar eventos según el resultado de la elección
				select {
				case <-nr.FollowerChan:
					nr.Rol = "Follower"

				case <-nr.Heartbeat:
					nr.Logger.Printf("Soy candidato, pero recibo heartbeat, así que vuelvo a follower\n")
					nr.Rol = "Follower"

				// Si sale elegido lider;
				case <-nr.LeaderChan:
					nr.Rol = "Leader"
					nr.Logger.Printf("Era candidato, pero me acaban de elegir líder\n")

					// Configurar NextIndex y MatchIndex para replicación de logs
					for i := 0; i < len(nr.Nodos); i++ {
						if i != nr.Yo {
							nr.Mux.Lock()
							nr.NextIndex[i] = len(nr.Log) + 1
							nr.MatchIndex[i] = 0
							nr.Mux.Unlock()
						}
					}

				// Si expira el timeout, el nodo se vuelve a presentar como candidato
				case <-time.After(generarTimeout()):
					nr.Rol = "Candidate"
					nr.Logger.Printf("Timeout expirado, me presento nuevamente como candidato\n")
				}

			// Si el nodo es líder;
			} else if nr.Rol == "Leader" {
				// Configurar el nodo como líder y enviar heartbeats
				nr.Mux.Lock()
				nr.IdLider = nr.Yo
				nr.Mux.Unlock()
				enviaHeartbeats(nr)

				// Manejar eventos según el rol actual
				select {
				case <-nr.FollowerChan:
					nr.Rol = "Follower"
				case <-nr.Heartbeat:
					nr.Rol = "Follower"
					nr.Logger.Printf("Soy líder, pero recibo heartbeat, así que vuelvo a follower\n")
				case <-time.After(50 * time.Millisecond):
					nr.Rol = "Leader"
				}
			}
		}
	}
}



// Funcion para enviar AppendEntries a todos los nodos
// enviaHeartbeats envía heartbeats a otros nodos en el clúster Raft para mantener la comunicación y confirmar liderazgo.
// enviaHeartbeats envía heartbeats a todos los nodos en el clúster Raft.
// Si el nodo no está activo, se espera durante 400 milisegundos antes de enviar heartbeats.
// Para cada nodo en el clúster, se inicia una goroutine para enviar heartbeats.
// Si hay entradas nuevas en el log para enviar, se envían mediante AppendEntries.
// Si no hay entradas nuevas, se envía un heartbeat.
// El parámetro nr contiene la información del nodo Raft.
func enviaHeartbeats(nr *NodoRaft) {
	// Verificar si el nodo está activo antes de enviar heartbeats
	if !nr.alive {
		time.Sleep(400 * time.Millisecond)
		return
	}

	// Iterar sobre todos los nodos en el clúster Raft
	for i := 0; i < len(nr.Nodos); i++ {
		// Verificar si el índice actual en la iteración no es igual al índice del propio nodo
		if i != nr.Yo {
			// Iniciar una goroutine para enviar heartbeats al nodo actual
			go func(node int) {
				var result ResultsAE
				var entradas []Entry

				// Si hay nuevas entradas que aun no se han enviado al seguidor node:
				if len(nr.Log) >= nr.NextIndex[node] {
					for i := nr.NextIndex[node] - 1; i < len(nr.Log); i++ {
						entrada := nr.Log[i]
						nuevaEntrada := Entry{Term: entrada.Term, Operacion: entrada.Operacion, Index: entrada.Index,
						}
						entradas = append(entradas, nuevaEntrada)
					}

					if nr.NextIndex[node] > 1 {
						entrada := ArgAppendEntries{nr.CurrentTerm, nr.Yo, nr.NextIndex[node] - 1, nr.Log[nr.NextIndex[node]-1].Term, entradas, nr.CommitIndex}
						sendEntries(nr, node, &result, &entrada)
					} else {
						entrada := ArgAppendEntries{nr.CurrentTerm, nr.Yo, -1,  0, entradas, nr.CommitIndex}
						sendEntries(nr, node, &result, &entrada)
					}
				// Sino, enviar un heartbeat
				} else {
					nr.Logger.Printf("Envio heartbeat a %d\n", node)
					if nr.NextIndex[node] > 2 {
						entrada := ArgAppendEntries{nr.CurrentTerm, nr.Yo, nr.NextIndex[node] - 1, nr.Log[nr.NextIndex[node]-2].Term, entradas, nr.CommitIndex}
						sendHeartbeat(nr, node, &result, &entrada)
					} else {
						entrada := ArgAppendEntries{nr.CurrentTerm, nr.Yo, -1,  0, entradas, nr.CommitIndex}
						sendHeartbeat(nr, node, &result, &entrada)
					}
				}
			}(i)
		}
	}
}


// sendEntries envía las entradas de log al nodo identificado por 'node' utilizando una llamada remota.
// Devuelve true si el envío fue exitoso, de lo contrario devuelve false.
func sendEntries(nr *NodoRaft, node int, result *ResultsAE, entrada *ArgAppendEntries) bool {
	// Verificar si el nodo está vivo antes de intentar enviar entradas de log
	if !nr.alive {
		time.Sleep(400 * time.Millisecond)
		nr.Logger.Printf("No se ha podido enviar entries a %d porque está muerto\n", node)
		return false
	}

	// Realizar una llamada remota para enviar entradas de log al nodo identificado por 'node'
	err := nr.Nodos[node].CallTimeout("NodoRaft.AppendEntries", entrada, result, 700*time.Millisecond)

	// Si la llamada remota es exitosa, se actualizan los valores de NextIndex y MatchIndex del nodo destino.
	if result.Success {
		nr.Mux.Lock()

		// Actualizar el NextIndex y el MatchIndex del nodo destino
		nr.NextIndex[node] += len(entrada.Entries)
		nr.MatchIndex[node] = result.MatchIndex

		if nr.CommitIndex < nr.MatchIndex[node] {
			nr.NumSucess++

			// Si la mayoría de los nodos han replicado la entrada, se realiza un commit y se actualiza el CommitIndex.
			if nr.NumSucess >= len(nr.Nodos)/2 {
				// Actualizar el CommitIndex y reiniciar el contador de nodos con éxito
				nr.CommitIndex += len(entrada.Entries)
				nr.NumSucess = 0
				nr.Logger.Printf("CommitIndex actualizado, ahora es: %d.\n", nr.CommitIndex)
			}
		}

		nr.Mux.Unlock()

	// Si la llamada remota no es exitosa y el resultado tiene un término mayor a cero, se decrementa el NextIndex.
	} else if result.Term > 0 {
		nr.NextIndex[node]--
		nr.Logger.Printf("No ha habido éxito, se decrementa el NextIndex. Nuevo valor: %d\n", nr.NextIndex[node])
	}

	return err == nil
}
	



// sendHeartbeat envía un "heartbeat" al nodo especificado como parametro
// Devuelve true si el envío fue exitoso, de lo contrario devuelve false.
func sendHeartbeat(nr *NodoRaft, node int, result *ResultsAE, entrada *ArgAppendEntries) bool {
	// Verificar si el nodo está vivo antes de enviar el "heartbeat"
	if !nr.alive {
		time.Sleep(400 * time.Millisecond)
		nr.Logger.Printf("No se ha podido enviar heartbeat a %d porque está muerto\n", node)
		return false
	}

	// Realizar una llamada remota para enviar el "heartbeat"
	err := nr.Nodos[node].CallTimeout("NodoRaft.AppendEntries", entrada, result, 700*time.Millisecond)

	// Volver a ser seguidor si se recibe un término mayor en el "heartbeat"
	if nr.CurrentTerm < result.Term {
		nr.CurrentTerm = result.Term
		nr.IdLider = -1
		nr.FollowerChan <- true
		nr.Logger.Printf("Término mayor: Mi término es: %d, el suyo es: %d (vuelvo a ser seguidor)\n", nr.CurrentTerm, result.Term)
	}

	return err == nil
}



	type ArgAppendEntries struct {
		LeaderTerm     int 		// Termino del lider
		LeaderId int 			// Id del lider
		PrevLogIndex	int		// Indice del registro previo al nuevo
		PrevLogTerm		int		// Termino del registro previo al nuevo
		Entries      []Entry 	// Entradas de registro a replicar
		LeaderCommit int   		// Indice del registro mas alto conocido que se ha comprometido
	}
	
	type ResultsAE struct {
		Term    int  			// Termino actual del nodo para que el candidato se actualice
		Success bool 			// True si el voto se ha concedido
		MatchIndex int 			// Indice del registro mas alto conocido que se ha replicado en el cluster
	}
	
	// Metodo de tratamiento de llamadas RPC AppendEntries
	func (nr *NodoRaft) AppendEntries(args *ArgAppendEntries, results *ResultsAE) error {
		if !nr.alive {
			time.Sleep(400 * time.Millisecond)
			return nil
		}

		nr.Mux.Lock()
		nr.Logger.Printf("Recibo AppendEntries, args: %v\n", args)

		// 1:  Reply false if term < currentTerm
		if nr.CurrentTerm > args.LeaderTerm {
			nr.Logger.Printf("Su mandato es: %d, el mío es: %d. Por tanto le devuelvo dalso", args.LeaderTerm, nr.CurrentTerm)
			results.Success = false
			results.Term = nr.CurrentTerm
			results.MatchIndex = 0
			nr.Mux.Unlock()
			return nil
		}

		// 2. Reply false if log doesnt contain an entry at prevLogIndex whose term matches prevLogTerm
		if len(nr.Log) > 0 && args.PrevLogIndex > -1 && len(nr.Log) < args.PrevLogIndex {
			nr.Logger.Printf("Su LastApplied es: %d, el mío es: %d", args.PrevLogIndex, nr.LastApplied)
			results.Success = false
			results.Term = nr.CurrentTerm
			results.MatchIndex = 0
			nr.Mux.Unlock()
			return nil
		}

		// 3. If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it
		if len(nr.Log) > 0 && args.PrevLogIndex > -1 && nr.Log[args.PrevLogIndex-1].Term != args.PrevLogTerm {
			results.Success = false
			results.Term = nr.CurrentTerm
			results.MatchIndex = 0
			nr.Log = nr.Log[:args.PrevLogIndex]
			nr.LastApplied = max(0, len(nr.Log)-1)
			nr.Logger.Printf("Entrada bajo conflicto. Entrada: %v\n", args)
			nr.Mux.Unlock()
			return nil
		}

		results.Success = true
		results.Term = args.LeaderTerm
		nr.IdLider = args.LeaderId
		nr.CurrentTerm = args.LeaderTerm

		// 4. Append any new entries not already in the log
		if len(args.Entries) > 0 {
			nr.Log = append(nr.Log, args.Entries...)
			nr.Logger.Printf("Se han añadido entradas al log: %v\n", args.Entries)
		}
		results.MatchIndex = len(nr.Log)

		// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry
		if nr.CommitIndex < args.LeaderCommit {
			nr.CommitIndex = min(args.LeaderCommit,len(nr.Log))
			nr.Logger.Printf("Se ha actualizado el commitIndex a: %d\n", nr.CommitIndex)
		}

		nr.Heartbeat <- true
		nr.Mux.Unlock()

		return nil
	}

	
	// max devuelve el máximo entre dos números enteros.
	func max(a, b int) int {
		if a > b {
			return a
		}
		return b
	}

	// min devuelve el mínimo entre dos números enteros.
	func min(a, b int) int {
		if a < b {
			return a
		}
		return b
	}
	

	// generarTimeout genera un valor de tiempo aleatorio entre 200 y 500 milisegundos.
	// Este rango de tiempo se recomienda en el paper.
	func generarTimeout() time.Duration {
		return time.Duration(rand.Intn(200)+300) * time.Millisecond
	}
	
	
	
	// KillNode detiene el nodo actual y marca su estado como no activo.
	func (nr *NodoRaft) KillNode(args Vacio, reply *Vacio) error {
		nr.alive = false
		nr.Logger.Printf("Nodo %d muerto\n", nr.Yo)
		return nil
	}
	
	// ReviveNode revive un nodo Raft que estaba previamente marcado como no vivo.
	// Este método establece el atributo "alive" en true y registra un mensaje de registro indicando que el nodo ha sido revivido.
	func (nr *NodoRaft) ReviveNode(args Vacio, reply *Vacio) error {
		nr.alive = true
		nr.Logger.Printf("Nodo %d revivido\n", nr.Yo)
		return nil
	}
