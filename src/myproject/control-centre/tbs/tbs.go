package tbs

import (
	"fmt"
	"net"
	"os"
	// "time"
)

type Dispatcher struct {
	listeners map[string]*EventChain
}

type EventChain struct {
	chs       []chan *Event
	callbacks []*EventCallback
}

func CreateEventChain() *EventChain {
	return &EventChain{chs: []chan *Event{}, callbacks: []*EventCallback{}}
}

type Event struct {
	eventName string
	Params    map[string]interface{}
}

func ServerStarted(port string) {
	fmt.Println("server started.")
	tcpAddr, err := net.ResolveTCPAddr("tcp4", port)
	checkError(err)
	listener, err := net.ListenTCP("tcp", tcpAddr)
	for {
		conn, err := listener.Accept()
		if err != nil {
			continue
		}
		go handleClient(conn)
	}
}

func handleClient(conn net.Conn) {
	// conn.SetReadDeadline(time.Now().Add(2 * time.Minute)) // set 2 minutes timeout
	request := make([]byte, 128) // set maxium request length to 128B to prevent flood attack
	defer conn.Close()           // close connection before exit
	for {
		read_len, err := conn.Read(request)

		if err != nil {
			// fmt.Println(err)
			if read_len == 0 {
				//移除监听
				dispatcher := SharedDispatcher()
				dispatcher.RemoveEventListener("test", _cb)
			}
			break
		}
		fmt.Println("message: ")
		fmt.Println(string(request))
		if read_len == 0 {
			//移除监听
			dispatcher := SharedDispatcher()
			dispatcher.RemoveEventListener("test", _cb)
			break // connection already closed by client
		} else {
			onData(request)
		}

		request = make([]byte, 128) // clear last read content
	}
}

func onData(request []byte) {
	//随便弄个事件携带的参数，我把参数定义为一个map
	params := make(map[string]interface{})
	params["id"] = 1000

	//创建一个事件对象
	event := CreateEvent("test", params)
	event.Params["bytes"] = request

	//获取分派器单例
	dispatcher := SharedDispatcher()

	//添加监听
	dispatcher.AddEventListener("test", _cb)

	//把事件分派出去
	dispatcher.DispatchEvent(event)
}

func checkError(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Fatal error: %s", err.Error())
		// os.Exit(1)
	}
}

func CreateEvent(eventName string, params map[string]interface{}) *Event {
	return &Event{eventName: eventName, Params: params}
}

type EventCallback func(*Event)
var _instance *Dispatcher
var _cb *EventCallback

func SetCallBack(callback *EventCallback) {
	_cb = callback
}

func SharedDispatcher() *Dispatcher {
	if _instance == nil {
		_instance = &Dispatcher{}
		_instance.Init()
	}

	return _instance
}

func (this *Dispatcher) Init() {
	this.listeners = make(map[string]*EventChain)
}

func (this *Dispatcher) AddEventListener(eventName string, callback *EventCallback) {
	eventChain, ok := this.listeners[eventName]
	if !ok {
		eventChain = CreateEventChain()
		this.listeners[eventName] = eventChain
	}

	exist := false
	for _, item := range eventChain.callbacks {
		if item == callback {
			exist = true
			break
		}
	}

	if exist {
		return
	}

	ch := make(chan *Event)

	fmt.Printf("add listener: %s\n", eventName)
	eventChain.chs = append(eventChain.chs[:], ch)
	eventChain.callbacks = append(eventChain.callbacks[:], callback)

	go func() {
		for {
			event := <-ch
			if event == nil {
				break
			}
			(*callback)(event)
		}
	}()
}

func (this *Dispatcher) RemoveEventListener(eventName string, callback *EventCallback) {
	eventChain, ok := this.listeners[eventName]
	if !ok {
		return
	}

	var ch chan *Event
	exist := false
	key := 0
	for k, item := range eventChain.callbacks {
		if item == callback {
			exist = true
			ch = eventChain.chs[k]
			key = k
			break
		}
	}

	if exist {
		fmt.Printf("remove listener: %s\n", eventName)
		ch <- nil

		eventChain.chs = append(eventChain.chs[:key], eventChain.chs[key+1:]...)
		eventChain.callbacks = append(eventChain.callbacks[:key], eventChain.callbacks[key+1:]...)
	}
}

func (this *Dispatcher) DispatchEvent(event *Event) {
	eventChain, ok := this.listeners[event.eventName]
	if ok {
		// fmt.Printf("dispatch event: %s\n", event.eventName)
		for _, chEvent := range eventChain.chs {
			chEvent <- event
		}
	}
}
