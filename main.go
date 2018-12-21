package main

import (
	"fmt"
	"sync"
	"net"
	sc "strconv"
	"time"
	"strings"
	"crypto/sha256"
)
//TODO: remove this
//test config
var r = 5;  //node count
var f = 1;  //byzantine count
var byzantine = -1 //byzintine id
//var byzantine = r-2 //client id
var test_client_id = r-1 //client id


//messages enum
const (
	REQUEST int = iota
	PREPREPARE
	PREPARE
	COMMIT
	REPLY
)

//state
const (
	NORMAL int = iota
	REQUESTED
	PREPARED
	COMMITTED
	COMMITTED_LOCAL
)

//managing neighbor
type neighbor struct {
	node_id int
	conns net.Conn
}

//node state
type node struct {
	id int
	state int
	state_count int

	view int
	seq_num int
	timestamp int

	neighbors []net.Conn
	client_id int
	logs []string
}

func main() {
	var wg sync.WaitGroup
	for i := 0; i < r; i++ {
		wg.Add(1)
		go runReplicas(i, 0)
	}
	wg.Wait()
}

func runReplicas(node_id int, view_number int) {
	primary_id :=  view_number % r
	neighbors := make([]net.Conn, r )
	state_node := node {
		id: node_id,
		state: NORMAL,
		state_count: 0,
		view: view_number,
		seq_num: 0,
		timestamp: 0,
		neighbors: neighbors,
		client_id: -1,
	}

	go Server(&state_node)
	time.Sleep(1000 * time.Millisecond)

	//TODO: need check neighbor count
	Discovery(&state_node)
	time.Sleep(2000 * time.Millisecond)

	if node_id == test_client_id {
		msg := sc.Itoa(REQUEST)
		msg += ":" + "save_sigmoid"
		msg += ":" + sc.Itoa(0)
		msg += ":" + sc.Itoa(node_id)
		fmt.Println("[", node_id, "] -> [", primary_id , "] = msg", string(msg[0]) )
		SendP2p(primary_id, neighbors, msg)
		state_node.state = REQUESTED
	}
}

// find nodes
func Discovery(n *node) {
	for i := 0; i < r ; i++ {
		if i != n.id && n.neighbors[i] == nil {
			conn, err := net.Dial("udp", genAddrString(i))
			if err != nil {
				fmt.Println("dial error")
				return
			}
			n.neighbors[i] = conn
		}
	}
}

func ProcessMsg(n *node, message string) {
	if n.id == byzantine {
		return
	}

	primary_id := n.view % r
	msg := strings.Split(message,":")
	msg_type, _ := sc.Atoi(msg[0])
	switch msg_type {
	case REQUEST:
		if n.id == primary_id && n.state == NORMAL {
			//n.state = REQUESTED
			n.client_id, _ = sc.Atoi(msg[3])

			//send msg to backups
			newmsg := sc.Itoa(PREPREPARE)
			newmsg += ":" + sc.Itoa(n.view)
			newmsg += ":" + sc.Itoa(n.seq_num)
			digest := sha256.New()
			digest.Write([]byte(message))
			newmsg += ":" + fmt.Sprintf("%x",digest.Sum(nil))
			newmsg += ":" + message
			SendMulticast(n.id, primary_id, false, n.client_id, false, n.neighbors, newmsg)
			n.seq_num += 1
		} else {
			//relay msg to primary
			//SendP2p(primary_id, n.neighbors, message)
		}

	case PREPREPARE:
		if n.state != NORMAL{
			return
		}
		//send msg to backups
		n.client_id, _ = sc.Atoi(msg[7])

		newmsg := sc.Itoa(PREPARE)
		newmsg += ":" + msg[1]
		newmsg += ":" + msg[2]
		newmsg += ":" + msg[3]
		newmsg += ":" + sc.Itoa(n.id)
		SendMulticast(n.id, primary_id, true, n.client_id, false, n.neighbors, newmsg)

	case PREPARE:
		if n.state == NORMAL {
			n.state = PREPARED;
			newmsg := sc.Itoa(COMMIT)
			newmsg += ":" + msg[1]
			newmsg += ":" + msg[2]
			newmsg += ":" + msg[3]
			newmsg += ":" + sc.Itoa(n.id)
			SendMulticast(n.id, primary_id, true, n.client_id, false, n.neighbors, newmsg)
		}
		if n.state == PREPARED {
			fmt.Println("already produced - do nothing")
			return
		}

	case COMMIT:
		if n.state == PREPARED {
			n.state = COMMITTED
			n.state_count = 1;
			return
		}

		if n.state == COMMITTED {
			n.state_count += 1
			if n.state_count < 2*f {
				return
			}
		}
		// do action
		if(n.state_count > 2*f) {
			fmt.Println("already produced - do nothing")
			return
		}
		fmt.Println("do action")
		newmsg := sc.Itoa(REPLY)
		newmsg += ":" + msg[1]
		newmsg += ":" + msg[2]
		newmsg += ":" + msg[3]
		newmsg += ":" + sc.Itoa(n.id)
		time.Sleep(5000 * time.Millisecond)
		fmt.Println("[", n.id, "] -> [", n.client_id , "] = msg", string(newmsg[0]) )
		SendP2p(n.client_id, n.neighbors, newmsg)

	case REPLY:
		fmt.Println(n.state)
		if n.state != REQUESTED {
			return
		}
		n.state_count += 1
		if n.state_count > 2*f+1 {
			fmt.Println("already produced - do nothing")
			return
		}
		if n.state_count == 2*f+1 {
			fmt.Println("commited")
		}

		//n.state = NORMAL
	}
}


func Server(n *node) {
	//Listen
	conn, err := net.ListenPacket("udp", genAddrString(n.id))
	if err != nil {
		fmt.Println("listen error", err)
		return
	}
	//fmt.Println("listen", conn.LocalAddr())
	defer conn.Close()

	//start state machine
	buf := make([]byte, 1024)
	for {
		length, _, err := conn.ReadFrom(buf)
		if err != nil {
			fmt.Println("Error: ", err)
		}
		//fmt.Println("[", node_id, "] recieved ", string(buf[0:n]), "from" , addr)
		ProcessMsg(n, string(buf[0:length]))
	}

}

// send multicast 
func SendMulticast(node_id int,
			primary_id int, include_primary bool,
			client_id int, include_client bool,
			neighbors []net.Conn, msg string) {
	time.Sleep(10000 * time.Millisecond)
	for i := 0; i < r ; i++ {
		if i == node_id {
			continue
		}
		if i == primary_id && !include_primary {
			continue
		}
		if i == client_id && !include_client {
			continue
		}
		fmt.Println("[", node_id, "] -> [", i , "] = msg", string(msg[0]) )
		SendP2p(i, neighbors, msg)
	}
}

// send peer to peer msg
func SendP2p(node_id int, neighbors []net.Conn, msg string) {
	buf := []byte(msg)
	_, err := neighbors[node_id].Write(buf)
	if err != nil {
		fmt.Println("Error: ", err)
	}
}


// helper
func genAddrString(node_id int) string {
	return "127.0.0.1:" + sc.Itoa(2620 + node_id)
}
