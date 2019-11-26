package main

/*
DA FARE:
- PROBLEMA: c'è qualcosa che fa scattare il recovery anche quando tutti i processi concordano per lo stesso leader.
Capire che cosa fa scattare il recovery e correggere. L'effetto è il non sintonizzarsi su un unico leader
[si potrebbe eliminare completamente e il problema sembrerebbe risolto]
- Sistemare la faccenda della "chiamata al Merge()" in base all'importanza del (numero del) processo associata al timer
*/

import (
	"sync"
	"encoding/json"
	"net/http"
	"bytes"
	"fmt"
	"time"
	"math/rand"
	"hash/fnv"
	_ "github.com/mattn/go-sqlite3"
)

var mapMutexUp = sync.Mutex{}
var mapMutexMapReadyMessage = sync.Mutex{}
var mapMutexMapAreYouThereRequestMessage = sync.Mutex{}
var mapMutexAreYouCoordinatorRequestMessage = sync.Mutex{}
var mapMutexAcceptMessage = sync.Mutex{}
var mapMutexInvitationMessage = sync.Mutex{}

var timer int = 5 // Timer per l'invocazione periodica della funzione Check()
var timer_leader int = 4 // Timer dopo il quale un processo si rende conto che il proprio leader non c'è più
var T_readyMessage int = 3 // Timer per i secondi di attesa di risposte dei messaggi
var T_areYouThereRequestMessage int = 3 // Timer per i secondi di attesa di risposte dei messaggi
var T_areYouCoordinatorRequestMessage int = 3 // Timer per i secondi di attesa di risposte dei messaggi
var T_acceptMessage int = 3 // Timer per i secondi di attesa di risposte dei messaggi


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
	Up[my_address] = true
}

// Funzione di utilità per convertire stringhe a interi univoci
func hash(s string) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	return int(h.Sum32())
}

// Funzione ausiliaria che dato un "insieme" (mappa in cui le chiavi sono stringhe) restituisce la chiave maggiore
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
}

// Funzione ausiliaria che invoca la procedura Check() ogni 5 secondi
func invocatorCheck() {
	for {
		for timer > 0 {
			timer = timer - 1
			time.Sleep(1 * time.Second) // Aspetta un secondo
			fmt.Println("Il mio leader è ", c)
		}
		if timer == 0 {
			timer = 5
			go Check()
		}
	}
}

// Un non-leader deve ricevere periodicamente dei messaggi del leader del proprio gruppo vivo,
// altrimenti invoca TimeOut
func invocatorTimeout() {
	for {
		if my_address != c { // se non sono un leader
			for timer_leader > 0 {
				timer_leader = timer_leader - 1
				time.Sleep(1 * time.Second) // Aspetta un secondo
			}
			if timer_leader == 0 {
				timer_leader = 4
				go Timeout()
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

	go invocatorCheck() // Goroutine che periodicamente invoca la funzione principale Check()

	go invocatorTimeout() // Goroutine che controlla il timer_leader e invoca Timeout() se questo è uguale a 0

	http.HandleFunc("/AcknowledgmentMessage", handleAcknowledgmentMessage)

	http.HandleFunc("/ImYourLeaderMessage", handleImYourLeaderMessage)

	http.HandleFunc("/AcceptMessage", handleAcceptMessage)

	http.HandleFunc("/InvitationMessage", handleInvitationMessage)

	http.HandleFunc("/AreYouThereRequestMessage", handleAreYouThereRequestMessage)

	http.HandleFunc("/AreYouCoordinatorRequestMessage", handleAreYouCoordinatorRequestMessage)

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

type AreYouThereRequestMessage struct {
	Sender string `json:"sender"`
	Gn int `json:"gn"`
}
var mapAreYouThereRequestMessage map[string]bool = make(map[string]bool) // Nota: in questo caso viene portata anche una risposta
var mapAreYouThereRequestMessage2 map[string]string = make(map[string]string)

type AreYouCoordinatorRequestMessage struct {
	Sender string `json:"sender"`
}
var mapAreYouCoordinatorRequestMessage map[string]bool = make(map[string]bool) // Nota: in questo caso viene portata anche una risposta
var mapAreYouCoordinatorRequestMessage2 map[string]string = make(map[string]string)

type ReadyMessage struct {
	Sender string `json:"sender"`
	Gn int `json:"gn"`
}
var mapReadyMessage map[string]bool = make(map[string]bool)










/* FUNZIONI */

// Il processo P_i si riprende dopo un fallimento, o dopo che altre cose sono andate storte!
// TODO: invocarla dopo un ripristino del nodo
func Recovery() {
	fmt.Println("Io,", my_address, "faccio recovery")
	s = "Election"
	// TODO: capire cosa significa "Call stop"
	counter = counter + 1 // sto per creare un nuovo gruppo
	g = hash(my_address) + counter // Nuovo identificativo del gruppo. TODO: capire se è la cosa giusta da fare per creare gruppi univoci
	c = my_address
	mapMutexUp.Lock()
	Up = make(map[string]bool)
	mapMutexUp.Unlock()
	s = "Reorganization"
	s = "Normal"
}

// Funzione che mira a formare un nuovo gruppo in cui P_i è il coordinatore, avente dentro tutti i membri del gruppo
// CoordinatorSet (originariamente in questo gruppo sono tutti dei coordinatori!)
func Merge(CoordinatorSet map[string]bool) {
	
	fmt.Println("Io, processo ", my_address, "chiamo merge")
	
	s = "Election"
	// TODO: capire cosa vuol dire Call stop
	counter = counter + 1 // visto che sto formando un altro gruppo, aumento il counter
	g = hash(my_address) + counter // Nuovo identificativo del gruppo. TODO: capire se è la cosa giusta da fare per creare gruppi univoci
	c = my_address // mi eleggo leader
	TempSet := Up
	mapMutexUp.Lock()
	Up = make(map[string]bool) // Setto il mio gruppo attuale a "gruppo vuoto"
	Up[my_address] = true // mi metto io dentro Up
	mapMutexUp.Unlock()
	
	for j,_ := range CoordinatorSet { // Per ogni coordinatore dell'insieme CoordinatorSet
		jsonData := InvitationMessage{
			Sender: my_address,
			Gn: g}
		jsonValue, _ := json.Marshal(jsonData)
		go http.Post("http://"+j+"/InvitationMessage", "application/json", bytes.NewBuffer(jsonValue)) // Dì esplicitamente a P_j di unirsi a g
		mapMutexUp.Lock()
		Up[j] = true
		mapMutexUp.Unlock()
	}
	for j,_ := range TempSet { // Per ogni membro che avevo già nel mio gruppo
		jsonData := InvitationMessage{
			Sender: my_address,
			Gn: g}
		jsonValue, _ := json.Marshal(jsonData)
		go http.Post("http://"+j+"/InvitationMessage", "application/json", bytes.NewBuffer(jsonValue))
		mapMutexUp.Lock()
		Up[j] = true
		mapMutexUp.Unlock()
	}
	s = "Reorganization"
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
			T_readyMessage = 3
			fmt.Println("Faccio recovery perchè sono Merge!")
			Recovery()
		} else {
			T_readyMessage = 3
			mapMutexMapReadyMessage.Lock()
			mapReadyMessage[j] = false
			mapMutexMapReadyMessage.Unlock()
		}
	}
}

// Funzione chiamata quando non sento il mio leader per un po' di tempo...
func Timeout() {
	MyCoord := c
	MyGroup := g
	if MyCoord != my_address {
		jsonData := AreYouThereRequestMessage{
			Sender: my_address,
			Gn: MyGroup}
		jsonValue, _ := json.Marshal(jsonData)
		go http.Post("http://"+MyCoord+"/AreYouThereRequestMessage", "application/json", bytes.NewBuffer(jsonValue))

		for T_areYouThereRequestMessage > 0 && mapAreYouThereRequestMessage[MyCoord] == false {
			T_areYouThereRequestMessage = T_areYouThereRequestMessage - 1
			time.Sleep(1 * time.Second) // Aspetta un secondo
		}
		if T_areYouThereRequestMessage == 0 && mapAreYouThereRequestMessage[MyCoord] == false { // Caso "sfortunato" del timer scaduto
			T_areYouThereRequestMessage = 3
			fmt.Println("Invoco il recovery perchè sono Timeout!")
			Recovery()
		} else { // Caso risposta arrivata
			T_areYouThereRequestMessage = 3
			mapMutexMapAreYouThereRequestMessage.Lock()
			mapAreYouThereRequestMessage[MyCoord] = false
			mapMutexMapAreYouThereRequestMessage.Unlock()
			if mapAreYouThereRequestMessage2[MyCoord] == "No" {
				mapMutexMapAreYouThereRequestMessage.Lock()
				mapAreYouThereRequestMessage2[MyCoord] = ""
				mapMutexMapAreYouThereRequestMessage.Unlock()
				fmt.Println("Invoco il recovery perchè sono Timeout 2!")
				Recovery()
			}
		}
	}
}

// FUNZIONE PRINCIPALE: la procedura è chiamata periodicamente con un "qualche timer"
func Check() {
	if s == "Normal" && c == my_address {
		fmt.Println("Invoco sta check!")
		leaderToGroup()
		TempSet := make(map[string]bool) // TempSet è l'insieme dei leader attuali
		for _,j := range machines { // Per ogni processo, tranne P_i
			if j != my_address {
				jsonData := AreYouCoordinatorRequestMessage{
					Sender: my_address}
				jsonValue, _ := json.Marshal(jsonData)
				go http.Post("http://"+j+"/AreYouCoordinatorRequestMessage", "application/json", bytes.NewBuffer(jsonValue))
			
				for T_areYouCoordinatorRequestMessage > 0 && mapAreYouCoordinatorRequestMessage[j] == false {
					T_areYouCoordinatorRequestMessage = T_areYouCoordinatorRequestMessage - 1
					time.Sleep(1 * time.Second) // Aspetta un secondo
				}
				if T_areYouCoordinatorRequestMessage == 0 && mapAreYouCoordinatorRequestMessage[j] == false { // Caso "sfortunato" del timer scaduto
					T_areYouCoordinatorRequestMessage = 3
					continue // Salta alla successiva iterazione del ciclo
				} else { // Caso risposta arrivata
					T_areYouCoordinatorRequestMessage = 3
					mapMutexAreYouCoordinatorRequestMessage.Lock()
					mapAreYouCoordinatorRequestMessage[j] = false
					mapMutexAreYouCoordinatorRequestMessage.Unlock()
					if mapAreYouCoordinatorRequestMessage2[j] == "Yes" {
						mapMutexAreYouCoordinatorRequestMessage.Lock()
						TempSet[j] = true // Aggiungi P_j all'insieme
						mapAreYouCoordinatorRequestMessage2[j] = ""
						mapMutexAreYouCoordinatorRequestMessage.Unlock()
					}
				}
			}
		}
		if len(TempSet) != 0 { // Se l'insieme TempSet è diverso dall'insieme vuoto
			p := maxStringInMap(TempSet)
			if my_address < p {
				invocaMerge(TempSet)
			}
			
		}
	}
}


func invocaMerge(TempSet map[string]bool) {
	x := rand.Intn(len(machines))
	for x > 0 { // TODO: il numero deve essere proporzionale alla differenza
		x = x - 1
		time.Sleep(1)
	}
	Merge(TempSet)
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
			mapMutexAcceptMessage.Lock()
			mapAcceptMessage[data.Sender] = true
			mapMutexAcceptMessage.Unlock()
		}
		case "InvitationMessage": {
			mapMutexInvitationMessage.Lock()
			mapInvitationMessage[data.Sender] = true
			mapMutexInvitationMessage.Unlock()
		}
		case "AreYouThereRequestMessage": {
			mapMutexMapAreYouThereRequestMessage.Lock()
			mapAreYouThereRequestMessage[data.Sender] = true
			mapAreYouThereRequestMessage2[data.Sender] = data.Ans
			mapMutexMapAreYouThereRequestMessage.Unlock()
		}
		case "AreYouCoordinatorRequestMessage": {
			mapMutexAreYouCoordinatorRequestMessage.Lock()
			mapAreYouCoordinatorRequestMessage[data.Sender] = true
			mapAreYouCoordinatorRequestMessage2[data.Sender] = data.Ans
			mapMutexAreYouCoordinatorRequestMessage.Unlock()
		}
		case "ReadyMessage": {
			mapMutexMapReadyMessage.Lock()
			mapReadyMessage[data.Sender] = true
			mapMutexMapReadyMessage.Unlock()
		}
	}
}

// Evento che si manifesta ogni volta che arriva un avviso del fatto che il leader è vivo
func handleImYourLeaderMessage(w http.ResponseWriter, r *http.Request) {

	w.Header().Set("Content-Type", "application/json")
	var data Acknowledgment
	_ = json.NewDecoder(r.Body).Decode(&data)

	if data.Sender == c {
		timer_leader = 4
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

	if s == "Election" && g == data.Gn && c == my_address {
		j := data.Sender
		mapMutexUp.Lock()
		Up[j] = true
		mapMutexUp.Unlock()
	}
}

// Evento INVITATION: vengo invitato da un processo P_j a entrare nel gruppo Gn
func handleInvitationMessage(w http.ResponseWriter, r *http.Request) {
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

	if s == "Normal" { // Posso accettare l'invito solo se sono nello stato "Normal"
		// TODO: capire cosa significa "Call stop"
		Temp = c // Temp è il leader attuale di P_i
		TempSet = Up // TempSet è il gruppo di P_i
		s = "Election" // Imposta lo stato di P_i su "Election"
		c = data.Sender // Il nuovo coordinatore di P_i è il nodo P_j
		g = data.Gn // Il nuovo gruppo di P_i è il nodo P_j
	}
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

	for T_acceptMessage > 0 && mapAcceptMessage[data.Sender] == false {
		T_acceptMessage = T_acceptMessage - 1
		time.Sleep(1 * time.Second) // Aspetta un secondo
	}
	if T_acceptMessage == 0 && mapAcceptMessage[data.Sender] == false { // Caso "sfortunato" del timer scaduto
		T_acceptMessage = 3
		//fmt.Println("Faccio recovery perchè sono handleInvitationMessage")
		//Recovery()
	} else {
		T_acceptMessage = 3
		mapMutexAcceptMessage.Lock()
		mapAcceptMessage[data.Sender] = false
		mapMutexAcceptMessage.Unlock()
	}
	mapMutexAcceptMessage.Lock()
	mapAcceptMessage = make(map[string]bool) // Resettato per il futuro
	mapMutexAcceptMessage.Unlock()

	s = "Normal"
}

// Evento AREYOUTHERE: chi spedisce questo messaggio a P_i desidera sapere se P_i è coordinatore
// del gruppo Gn e se considera P_j (mittente del messaggio) come membro di quel gruppo
func handleAreYouThereRequestMessage(w http.ResponseWriter, r *http.Request) {

	w.Header().Set("Content-Type", "application/json")
	var data AreYouThereRequestMessage
	_ = json.NewDecoder(r.Body).Decode(&data)
	
	// In questo caso l'Acknowledgment porta con sé qualcosa di aggiuntivo (Ans)

	if g == data.Gn && c == my_address && Up[data.Sender] == true {
		jsonData := Acknowledgment{
			Typemessage: "AreYouThereRequestMessage",
			Sender: my_address,
			Ans: "Yes" }
		jsonValue, _ := json.Marshal(jsonData)
		go http.Post("http://"+data.Sender+"/AcknowledgmentMessage", "application/json", bytes.NewBuffer(jsonValue))
	} else {
		jsonData := Acknowledgment{
			Typemessage: "AreYouThereRequestMessage",
			Sender: my_address,
			Ans: "No" }
		jsonValue, _ := json.Marshal(jsonData)
		go http.Post("http://"+data.Sender+"/AcknowledgmentMessage", "application/json", bytes.NewBuffer(jsonValue))
	}
}

// Evento AREYOUCOORDINATOR: Il nodo mittente vuole sapere se P_i è un coordinatore nello stato "Normal". P_i Può quindi
// rispondere SI oppure NO
func handleAreYouCoordinatorRequestMessage(w http.ResponseWriter, r *http.Request) {

	w.Header().Set("Content-Type", "application/json")
	var data AreYouCoordinatorRequestMessage
	_ = json.NewDecoder(r.Body).Decode(&data)

	// In questo caso l'Acknowledgment porta con sé qualcosa di aggiuntivo (Ans)

	if s == "Normal" && c == my_address {
		jsonData := Acknowledgment{
			Typemessage: "AreYouCoordinatorRequestMessage",
			Sender: my_address,
			Ans: "Yes" }
		jsonValue, _ := json.Marshal(jsonData)
		go http.Post("http://"+data.Sender+"/AcknowledgmentMessage", "application/json", bytes.NewBuffer(jsonValue))
	} else {
		jsonData := Acknowledgment{
			Typemessage: "AreYouCoordinatorRequestMessage",
			Sender: my_address,
			Ans: "No" }
		jsonValue, _ := json.Marshal(jsonData)
		go http.Post("http://"+data.Sender+"/AcknowledgmentMessage", "application/json", bytes.NewBuffer(jsonValue))
	}

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

	if s == "Reorganization" && g == data.Gn {
		s = "Normal"
		// TODO: nello peudocodice questa funzione prende dei "dati": significa che da questo momento in poi
		// finisce la leader election? Importante per i lock quando si dovrà fondere con Paxos
	}
}


