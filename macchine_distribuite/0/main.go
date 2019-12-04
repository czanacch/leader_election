package main

/*
DA FARE:
- PROBLEMA: c'è qualcosa che fa scattare il recovery anche quando tutti i processi concordano per lo stesso leader.
Capire che cosa fa scattare il recovery e correggere. L'effetto è il non sintonizzarsi su un unico leader
[si potrebbe eliminare completamente e il problema sembrerebbe risolto]
- Sistemare la faccenda della "chiamata al Merge()" in base all'importanza del (numero del) processo associata al timer
*/

import (
	"math"
	"sync"
	"encoding/json"
	"net/http"
	"bytes"
	"fmt"
	"time"
	//"math/rand"
	"hash/fnv"
	_ "github.com/mattn/go-sqlite3"
)

var T int = 5
var stateMutex = sync.Mutex{} // Sezione critica per i cambi di stato
var mapMutex = sync.Mutex{}

var timer int = 3 // Timer per l'invocazione periodica della funzione Check()
var timer_leader int = len(machines)*T + len(Up)*T // Timer dopo il quale un processo si rende conto che il proprio leader non c'è più
var T_readyMessage int = T // Timer per i secondi di attesa di risposte dei messaggi
var T_areYouThereMessage int = T // Timer per i secondi di attesa di risposte dei messaggi
var T_areYouCoordinatorMessage int = T // Timer per i secondi di attesa di risposte dei messaggi
var T_acceptMessage int = T // Timer per i secondi di attesa di risposte dei messaggi

var my_address string = "127.0.0.1:8000" // Numero identificativo del processo
var machines []string  // Indirizzi di tutti i processi

// Variabili locali del processo P_i: 
var s string // Indica lo stato del processo: "Down", "Election", "Reorganization" o "Normal"
var c string  // Numero identificativo (indirizzo IP) del processo coordinatore del gruppo NOTA: per noi è una stringa e non un int
var counter int // Contatore che si riferisce ai gruppi generati dal processo p_i
var g int // Identificativo univoco (numerico) del gruppo del processo p_i
var Up map[string]bool // Insieme di nodi che partecipano al gruppo: un nodo è nell'insieme se compare il suo indirizzo associato al valore true

func initialization() {
	machines = []string{"127.0.0.1:8000","127.0.0.1:8001","127.0.0.1:8002"}
	s = "Normal"
	c = my_address
	counter = 0
	g = hash(my_address)
	Up = make(map[string]bool)
	//Up[my_address] = true
}

// Funzione di utilità per convertire stringhe a interi univoci
func hash(s string) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	return int(h.Sum32())
}

// Funzione ausiliaria che dato un "insieme" (mappa in cui le chiavi sono stringhe) restituisce la chiave maggiore
/*
func maxStringInMap(set map[string]bool) string {
	strings := []string{} // array di stringhe
	for key,_ := range set {
		strings = append(strings, key)
	}
	// Da adesso l'array di stringhe
	max := strings[0]
	for _, value := range strings {
		if value > max {
			max = value
		}
	}
	return max
}*/

func maxAddressNum(set map[string]bool) int {
	strings := []string{} // array di stringhe
	for key,_ := range set {
		strings = append(strings, key)
	}
	// Da adesso l'array di stringhe

	numbers := []int{} // array di stringhe
	for _, value := range strings {
		numbers = append(numbers, hash(value))
	}

	max := numbers[0]
	for _, value := range numbers {
		if value > max {
			max = value
		}
	}

	return max

}

// Funzione ausiliaria che invoca la procedura Check() ogni 5 secondi
func invocatorCheck() {
	for {
		Check()
		/*
		for timer > 0 {
			timer = timer - 1
			time.Sleep(1 * time.Second) // Aspetta un secondo
			fmt.Println("Il mio leader è ", c)
		}
		if timer == 0 {
			//s = "Normal" // TODO: ultima invenzione
			timer = 3
			Check()
		}
		*/
	}
}

// Un non-leader deve ricevere periodicamente dei messaggi del leader del proprio gruppo vivo,
// altrimenti invoca TimeOut
func invocatorTimeout() {
	for {
		if my_address != c { // se non sono un leader
			for timer_leader > 0 {
				fmt.Println("Quando arriva a 0 invocherò Timeout: ", timer_leader)
				timer_leader = timer_leader - 1
				time.Sleep(1 * time.Second) // Aspetta un secondo
			}
			if timer_leader == 0 {
				timer_leader = len(machines)*T + len(Up)*T
				Timeout()
			}
		}
	}
}

func leaderToGroup() {
	for j,_ := range Up {
		jsonData := ImYourLeader{
			Sender: my_address}
		jsonValue, _ := json.Marshal(jsonData)
		go http.Post("http://"+j+"/ImYourLeaderMessage", "application/json", bytes.NewBuffer(jsonValue)) // Dì esplicitamente a P_j di unirsi a g
	}	
}



func main() {
	initialization()
		
	fmt.Println("Starting machine", my_address)

	timer_leader = len(machines)*T + len(Up)*T

	go invocatorCheck() // Goroutine che periodicamente invoca la funzione principale Check()

	go invocatorTimeout() // Goroutine che controlla il timer_leader e invoca Timeout() se questo è uguale a 0

	http.HandleFunc("/AcknowledgmentMessage", handleAcknowledgmentMessage)

	http.HandleFunc("/ImYourLeaderMessage", handleImYourLeaderMessage)

	http.HandleFunc("/AcceptMessage", handleAcceptMessage)

	http.HandleFunc("/InvitationMessage", handleInvitationMessage)

	http.HandleFunc("/AreYouThereMessage", handleAreYouThereMessage)

	http.HandleFunc("/AreYouCoordinatorMessage", handleAreYouCoordinatorMessage)

	http.HandleFunc("/ReadyMessage", handleReadyMessage)

	http.ListenAndServe(":8000", nil)

}

/*MESSAGGI*/
type Acknowledgment struct { // Messaggio usato per confermare la ricezione
	Typemessage string `json:"typemessage"`
	Sender string `json:"sender"`
	Ans string `json:"ans"`
}

type ImYourLeader struct { // Messaggio mandato periodicamente dal leader per dire che c'è
	Sender string `json:"sender"`
}





type AcceptMessage struct {
	Sender string `json:"sender"`
	Gn int `json:"gn"`
}
var mapAcceptMessage map[string]bool = make(map[string]bool)

type InvitationMessage struct {
	Sender string `json:"sender"`
	Gn int `json:"gn"`
}
var mapInvitationMessage map[string]bool = make(map[string]bool)

type AreYouThereMessage struct {
	Sender string `json:"sender"`
	Gn int `json:"gn"`
}
var mapAreYouThereMessage map[string]bool = make(map[string]bool) // Nota: in questo caso viene portata anche una risposta
var mapAreYouThereMessage2 map[string]string = make(map[string]string)

type AreYouCoordinatorMessage struct {
	Sender string `json:"sender"`
}
var mapAreYouCoordinatorMessage map[string]bool = make(map[string]bool) // Nota: in questo caso viene portata anche una risposta
var mapAreYouCoordinatorMessage2 map[string]string = make(map[string]string)

type ReadyMessage struct {
	Sender string `json:"sender"`
	Gn int `json:"gn"`
}
var mapReadyMessage map[string]bool = make(map[string]bool)










/* FUNZIONI */

// Il processo P_i si riprende dopo un fallimento, o dopo che altre cose sono andate storte!
// TODO: invocarla dopo un ripristino del nodo
func Recovery() {
	stateMutex.Lock()
	fmt.Println("Io,", my_address, "faccio recovery")
	s = "Election"
	// TODO: capire cosa significa "Call stop"
	counter = counter + 1 // sto per creare un nuovo gruppo
	g = hash(my_address) + counter // Nuovo identificativo del gruppo. TODO: capire se è la cosa giusta da fare per creare gruppi univoci
	c = my_address
	mapMutex.Lock()
	Up = make(map[string]bool)
	mapMutex.Unlock()
	s = "Normal"
	stateMutex.Unlock()
}

// Funzione che mira a formare un nuovo gruppo in cui P_i è il coordinatore, avente dentro tutti i membri del gruppo
// CoordinatorSet (originariamente in questo gruppo sono tutti dei coordinatori!)
func Merge(CoordinatorSet map[string]bool) {
	
	fmt.Println("Io, processo ", my_address, "chiamo merge")
	
	stateMutex.Lock()
	s = "Election"
	counter = counter + 1 // visto che sto formando un altro gruppo, aumento il counter
	g = hash(my_address) + counter // Nuovo identificativo del gruppo. TODO: capire se è la cosa giusta da fare per creare gruppi univoci
	c = my_address // mi eleggo leader
	TempSet := Up
	mapMutex.Lock()
	Up = make(map[string]bool) // Setto il mio gruppo attuale a "gruppo vuoto"
	//Up[my_address] = true // mi metto io dentro Up
	mapMutex.Unlock()
	stateMutex.Unlock()
	
	for j,_ := range CoordinatorSet { // Per ogni coordinatore dell'insieme CoordinatorSet
		jsonData := InvitationMessage{
			Sender: my_address,
			Gn: g}
		jsonValue, _ := json.Marshal(jsonData)
		go http.Post("http://"+j+"/InvitationMessage", "application/json", bytes.NewBuffer(jsonValue)) // Dì esplicitamente a P_j di unirsi a g
		mapMutex.Lock()
		Up[j] = true
		mapMutex.Unlock()
	}
	for j,_ := range TempSet { // Per ogni membro che avevo già nel mio gruppo
		jsonData := InvitationMessage{
			Sender: my_address,
			Gn: g}
		jsonValue, _ := json.Marshal(jsonData)
		go http.Post("http://"+j+"/InvitationMessage", "application/json", bytes.NewBuffer(jsonValue))
		mapMutex.Lock()
		Up[j] = true
		mapMutex.Unlock()
	}
	stateMutex.Lock()
	s = "Reorganization"
	stateMutex.Unlock()
	for j,_ := range Up {
		jsonData := ReadyMessage{
			Sender: my_address,
			Gn: g}
		jsonValue, _ := json.Marshal(jsonData)
		go http.Post("http://"+j+"/ReadyMessage", "application/json", bytes.NewBuffer(jsonValue))

		for T_readyMessage > 0 && mapReadyMessage[j] == false {
			T_readyMessage = T_readyMessage - 1
			time.Sleep(1 * time.Second) // Aspetta un secondo
		}
		if T_readyMessage == 0 && mapReadyMessage[j] == false { // Caso "sfortunato" del timer scaduto
			T_readyMessage = 5
			fmt.Println("Faccio recovery perchè sono Merge!")
			Recovery()
		} else {
			T_readyMessage = 5
			mapMutex.Lock()
			mapReadyMessage[j] = false
			mapMutex.Unlock()
		}
	}
	stateMutex.Lock()
	s = "Normal"
	stateMutex.Unlock()
}

// Funzione chiamata quando non sento il mio leader per un po' di tempo...
func Timeout() {
	fmt.Println("Faccio TIMEOUT!!!")
	stateMutex.Lock()
	MyCoord := c
	MyGroup := g
	stateMutex.Unlock()
	if MyCoord != my_address {
		jsonData := AreYouThereMessage{
			Sender: my_address,
			Gn: MyGroup}
		jsonValue, _ := json.Marshal(jsonData)
		go http.Post("http://"+MyCoord+"/AreYouThereMessage", "application/json", bytes.NewBuffer(jsonValue))

		for T_areYouThereMessage > 0 && mapAreYouThereMessage[MyCoord] == false {
			T_areYouThereMessage = T_areYouThereMessage - 1
			time.Sleep(1 * time.Second) // Aspetta un secondo
		}
		if T_areYouThereMessage == 0 && mapAreYouThereMessage[MyCoord] == false { // Caso "sfortunato" del timer scaduto
			T_areYouThereMessage = 5
			fmt.Println("Invoco il recovery perchè sono Timeout!")
			Recovery()
		} else { // Caso risposta arrivata
			T_areYouThereMessage = 5
			mapMutex.Lock()
			mapAreYouThereMessage[MyCoord] = false
			mapMutex.Unlock()
			if mapAreYouThereMessage2[MyCoord] == "No" {
				mapMutex.Lock()
				mapAreYouThereMessage2[MyCoord] = ""
				mapMutex.Unlock()
				fmt.Println("Invoco il recovery perchè sono Timeout 2!")
				Recovery()
			}
		}
	}
}

// FUNZIONE PRINCIPALE: la procedura è chiamata periodicamente con un "qualche timer"
func Check() {
	if s == "Normal" && c == my_address {
		fmt.Println("Il mio leader è ", c)
		fmt.Println("Invoco la check!")
		leaderToGroup() // Invio di notifica del lavoro del leader
		TempSet := make(map[string]bool) // TempSet è l'insieme dei leader attuali
		for _,j := range machines { // Per ogni processo, tranne P_i
			if j != my_address {
				jsonData := AreYouCoordinatorMessage{
					Sender: my_address}
				jsonValue, _ := json.Marshal(jsonData)
				go http.Post("http://"+j+"/AreYouCoordinatorMessage", "application/json", bytes.NewBuffer(jsonValue))
			
				for T_areYouCoordinatorMessage > 0 && mapAreYouCoordinatorMessage[j] == false {
					T_areYouCoordinatorMessage = T_areYouCoordinatorMessage - 1
					time.Sleep(1 * time.Second) // Aspetta un secondo
				}
				if T_areYouCoordinatorMessage == 0 && mapAreYouCoordinatorMessage[j] == false { // Caso "sfortunato" del timer scaduto
					T_areYouCoordinatorMessage = 5
					continue // Salta alla successiva iterazione del ciclo
				} else { // Caso risposta arrivata
					T_areYouCoordinatorMessage = 5
					mapMutex.Lock()
					mapAreYouCoordinatorMessage[j] = false
					mapMutex.Unlock()
					if mapAreYouCoordinatorMessage2[j] == "Yes" {
						mapMutex.Lock()
						TempSet[j] = true // Aggiungi P_j all'insieme
						mapAreYouCoordinatorMessage2[j] = ""
						mapMutex.Unlock()
					}
				}
			}
		}
		if len(TempSet) != 0 { // Se l'insieme TempSet è diverso dall'insieme vuoto
			p := maxAddressNum(TempSet)
			if hash(my_address) < p {
				fmt.Println(hash(my_address))
				seconds := int(math.Log10(float64(p - hash(my_address))))
				invocaMerge(TempSet, seconds)
			}
		}
	} else {
		time.Sleep(1 * time.Second)
		fmt.Println("Il mio leader è ", c)
	}
}


func invocaMerge(TempSet map[string]bool, seconds int) {
	x := seconds
	for x > 0 {
		fmt.Println(x)
		x = x - 1
		time.Sleep(1 * time.Second)
	}
	if c == my_address { // Se sono ancora leader
		Merge(TempSet)
	}
}




/****************************** EVENTI *****************************/
// Evento principale che gestisce le risposte alle chiamate che devono arrivare in tempo:
// ogni volta che un processo risponde ad un qualche messaggio, questo evento fa in modo
// da settare la giusta casella di una mappa in modo tale che il mittente si accorge della risposta arrivata in tempo
func handleAcknowledgmentMessage(w http.ResponseWriter, r *http.Request) {
	
	w.Header().Set("Content-Type", "application/json")
	var data Acknowledgment
	_ = json.NewDecoder(r.Body).Decode(&data)

	switch data.Typemessage {
		case "AcceptMessage": {
			mapMutex.Lock()
			mapAcceptMessage[data.Sender] = true
			mapMutex.Unlock()
		}
		case "InvitationMessage": {
			mapMutex.Lock()
			mapInvitationMessage[data.Sender] = true
			mapMutex.Unlock()
		}
		case "AreYouThereMessage": {
			mapMutex.Lock()
			mapAreYouThereMessage[data.Sender] = true
			mapAreYouThereMessage2[data.Sender] = data.Ans
			mapMutex.Unlock()
		}
		case "AreYouCoordinatorMessage": {
			mapMutex.Lock()
			mapAreYouCoordinatorMessage[data.Sender] = true
			mapAreYouCoordinatorMessage2[data.Sender] = data.Ans
			mapMutex.Unlock()
		}
		case "ReadyMessage": {
			mapMutex.Lock()
			mapReadyMessage[data.Sender] = true
			mapMutex.Unlock()
		}
	}
}

// Evento che si manifesta ogni volta che arriva un avviso del fatto che il leader è vivo
func handleImYourLeaderMessage(w http.ResponseWriter, r *http.Request) {

	w.Header().Set("Content-Type", "application/json")
	var data Acknowledgment
	_ = json.NewDecoder(r.Body).Decode(&data)

	if data.Sender == c {
		timer_leader = len(machines)*T + len(Up)*T
	}
}



// Evento ACCEPT: il nodo P_j (che fa scattare questo evento) ha accettato l'invito di P_i di entrare
// nel gruppo Gn
func handleAcceptMessage(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	var data AcceptMessage
	_ = json.NewDecoder(r.Body).Decode(&data)

	// TODO: prima cosa da fare è rispondere al mittente con un Acknowledgment
	jsonData := Acknowledgment{
		Typemessage: "AcceptMessage",
		Sender: my_address }
	jsonValue, _ := json.Marshal(jsonData)
	go http.Post("http://"+data.Sender+"/AcknowledgmentMessage", "application/json", bytes.NewBuffer(jsonValue))

	stateMutex.Lock()
	if s == "Election" && g == data.Gn && c == my_address {
		j := data.Sender
		mapMutex.Lock()
		Up[j] = true
		mapMutex.Unlock()
	}
	stateMutex.Unlock()
}

// Evento INVITATION: vengo invitato da un processo P_j a entrare nel gruppo Gn
func handleInvitationMessage(w http.ResponseWriter, r *http.Request) {
	fmt.Println("A me ", my_address, " è arrivato un InvitationMessage")
	var Temp string
	var TempSet map[string]bool
	
	w.Header().Set("Content-Type", "application/json")
	var data InvitationMessage
	_ = json.NewDecoder(r.Body).Decode(&data)
	
	// TODO: prima cosa da fare è rispondere al mittente con un Acknowledgment
	jsonData := Acknowledgment{
		Typemessage: "InvitationMessage",
		Sender: my_address }
	jsonValue, _ := json.Marshal(jsonData)
	go http.Post("http://"+data.Sender+"/AcknowledgmentMessage", "application/json", bytes.NewBuffer(jsonValue))

	stateMutex.Lock()
	if s == "Normal" { // Posso accettare l'invito solo se sono nello stato "Normal"
		// TODO: capire cosa significa "Call stop"
		Temp = c // Temp è il leader attuale di P_i
		TempSet = Up // TempSet è il gruppo di P_i
		s = "Election" // Imposta lo stato di P_i su "Election"
		c = data.Sender // Il nuovo coordinatore di P_i è il nodo P_j
		g = data.Gn // Il nuovo gruppo di P_i è il nodo P_j
	}
	stateMutex.Unlock()
	
	if Temp == my_address { // Se io (P_i) ero il leader, invito in Gn i membri del mio gruppo
		for address,_ := range TempSet { // Per ogni processo dentro Up
			
			jsonData := InvitationMessage{
				Sender: data.Sender,
				Gn: data.Gn }
			jsonValue, _ := json.Marshal(jsonData)
			go http.Post("http://"+address+"/InvitationMessage", "application/json", bytes.NewBuffer(jsonValue))
			// Nota: qui non è necessario il timer
		}
	}

	jsonData2 := AcceptMessage{
		Sender: my_address,
		Gn: data.Gn }
	jsonValue, _ = json.Marshal(jsonData2)
	go http.Post("http://"+data.Sender+"/AcceptMessage", "application/json", bytes.NewBuffer(jsonValue))

	/*
	for T_acceptMessage > 0 && mapAcceptMessage[data.Sender] == false {
		T_acceptMessage = T_acceptMessage - 1
		time.Sleep(1 * time.Second) // Aspetta un secondo
	}
	if T_acceptMessage == 0 && mapAcceptMessage[data.Sender] == false { // Caso "sfortunato" del timer scaduto
		T_acceptMessage = 5
		fmt.Println("Faccio recovery perchè sono handleInvitationMessage")
		Recovery()
	} else {
		T_acceptMessage = 5
		mapMutex.Lock()
		mapAcceptMessage[data.Sender] = false
		mapMutex.Unlock()
	}
	*/
	mapMutex.Lock()
	mapAcceptMessage = make(map[string]bool) // Resettato per il futuro
	mapMutex.Unlock()

	stateMutex.Lock()
	s = "Reorganization" // TODO: questo dovrebbe essere s = "Reorganization"
	stateMutex.Unlock()
}

// Evento AREYOUTHERE: chi spedisce questo messaggio a P_i desidera sapere se P_i è coordinatore
// del gruppo Gn e se considera P_j (mittente del messaggio) come membro di quel gruppo
func handleAreYouThereMessage(w http.ResponseWriter, r *http.Request) {

	w.Header().Set("Content-Type", "application/json")
	var data AreYouThereMessage
	_ = json.NewDecoder(r.Body).Decode(&data)
	
	// In questo caso l'Acknowledgment porta con sé qualcosa di aggiuntivo (Ans)

	stateMutex.Lock()
	if g == data.Gn && c == my_address && Up[data.Sender] == true {
		jsonData := Acknowledgment{
			Typemessage: "AreYouThereMessage",
			Sender: my_address,
			Ans: "Yes" }
		jsonValue, _ := json.Marshal(jsonData)
		go http.Post("http://"+data.Sender+"/AcknowledgmentMessage", "application/json", bytes.NewBuffer(jsonValue))
	} else {
		jsonData := Acknowledgment{
			Typemessage: "AreYouThereMessage",
			Sender: my_address,
			Ans: "No" }
		jsonValue, _ := json.Marshal(jsonData)
		go http.Post("http://"+data.Sender+"/AcknowledgmentMessage", "application/json", bytes.NewBuffer(jsonValue))
	}
	stateMutex.Unlock()
}

// Evento AREYOUCOORDINATOR: Il nodo mittente vuole sapere se P_i è un coordinatore nello stato "Normal". P_i Può quindi
// rispondere SI oppure NO
func handleAreYouCoordinatorMessage(w http.ResponseWriter, r *http.Request) {

	w.Header().Set("Content-Type", "application/json")
	var data AreYouCoordinatorMessage
	_ = json.NewDecoder(r.Body).Decode(&data)

	// In questo caso l'Acknowledgment porta con sé qualcosa di aggiuntivo (Ans)
	stateMutex.Lock()
	if s == "Normal" && c == my_address {
		jsonData := Acknowledgment{
			Typemessage: "AreYouCoordinatorMessage",
			Sender: my_address,
			Ans: "Yes" }
		jsonValue, _ := json.Marshal(jsonData)
		go http.Post("http://"+data.Sender+"/AcknowledgmentMessage", "application/json", bytes.NewBuffer(jsonValue))
	} else {
		jsonData := Acknowledgment{
			Typemessage: "AreYouCoordinatorMessage",
			Sender: my_address,
			Ans: "No" }
		jsonValue, _ := json.Marshal(jsonData)
		go http.Post("http://"+data.Sender+"/AcknowledgmentMessage", "application/json", bytes.NewBuffer(jsonValue))
	}
	stateMutex.Unlock()

}

// Evento READY: Il nodo mittente di questo messaggio è un qualche coordinatore che richiede a P_i di settarsi allo stato
// Normale per decretare la fine della "leader election"
func handleReadyMessage(w http.ResponseWriter, r *http.Request) {

	w.Header().Set("Content-Type", "application/json")
	var data ReadyMessage
	_ = json.NewDecoder(r.Body).Decode(&data)

	// TODO: prima cosa da fare è rispondere al mittente con un Acknowledgment
	jsonData := Acknowledgment{
		Typemessage: "ReadyMessage",
		Sender: my_address }
	jsonValue, _ := json.Marshal(jsonData)
	go http.Post("http://"+data.Sender+"/AcknowledgmentMessage", "application/json", bytes.NewBuffer(jsonValue))

	stateMutex.Lock()
	if s == "Reorganization"  && g == data.Gn {
		s = "Normal"
		fmt.Println("Finalmente eseguo PAXOS")
		// TODO: nello peudocodice questa funzione prende dei "dati": significa che da questo momento in poi
		// finisce la leader election? Importante per i lock quando si dovrà fondere con Paxos
	}
	stateMutex.Unlock()
}


