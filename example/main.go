package main

import (
	"RMQBus"
	"fmt"
	"log"
)

var p = RMQBus.DefaultOptions{"test", "amqp://localhost:5672", "tests", 60000, true, false, false, false, true, true, true, false, false, false, true, false}

func main() {
	forever := make(chan bool)
	done := make(chan bool)
	go Init(done)
	<-done
	log.Printf("Inquiry Service Started.")
	<-forever
}

func Printme() string {
	//time.Sleep(40000 * time.Millisecond)
	return "suman"
}

func PerformInquiry(msg interface{}, cb chan interface{}) {

	result := Printme()
	fmt.Println(result)
	cb <- result
}

func Ping(msg interface{}, cb chan interface{}) {

	cb <- "result"
}
func Init(done chan bool) {

	rmq := RMQBus.GetConnection(p)

	responderRegistry := RegisterResponders()
	consumerRegistry := RegisterConsumers()
	globalConsumerRegistry := RegisterGlobalConsumers()
	rmq.InitFunctions(responderRegistry, consumerRegistry, globalConsumerRegistry)
	//	rmq.InitFunctions(responderRegistry, consumerRegistry, globalConsumerRegistry)
	pc := make(chan string)
	go rmq.HandleQClose(pc)
	go func() {
		for { //receive loop
			ok := <-pc
			fmt.Println(ok)
			if ok != "" {
				fmt.Println("connection queue close main ")
				rmq.InitFunctions(responderRegistry, consumerRegistry, globalConsumerRegistry)
			}

		}

	}()
	done <- true

}

func RegisterResponders() map[string]RMQBus.EventHandler {
	m := map[string]RMQBus.EventHandler{
		"inquiry": PerformInquiry,
	}
	return m
}

func RegisterConsumers() map[string]RMQBus.EventHandler {
	m := map[string]RMQBus.EventHandler{
		"pingconsumer": Ping,
	}
	return m
}

func RegisterGlobalConsumers() map[string]RMQBus.EventHandler {
	m := map[string]RMQBus.EventHandler{}
	return m
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}
