package RMQBus

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"strings"
	"sync"

	"github.com/joho/godotenv"
	"github.com/streadway/amqp"
)

type RMQ struct {
	Ch      *amqp.Channel
	Conn    *amqp.Connection
	Options DefaultOptions
}
type EventHandler func(interface{}, chan interface{})

var singleton *RMQ
var once sync.Once
var Qclose = make(chan bool)

func GetConnection(rmqConfig DefaultOptions) *RMQ {
	once.Do(func() {
		conn, err := amqp.Dial(rmqConfig.URL)
		failOnError(err, "Failed to connect to RabbitMQ")
		notify := conn.NotifyClose(make(chan *amqp.Error)) //error channels
		go func() {
			for { //receive loop
				ok := <-notify
				if ok != nil {
					Qclose <- true
					fmt.Println("connection close asdasd")
				}
			}
		}()
		ch, err := conn.Channel()
		/*cancels := ch.NotifyCancel(make(chan string, 1))
		go func() {
			for { //receive loop
				ok := <-cancels
				fmt.Println("asd", ok)
				if ok != "" {
					Qclose <- true
					fmt.Println("connection close notify cancle")
				}
			}
		}()*/
		failOnError(err, "Failed to open a channel")
		singleton = &RMQ{Ch: ch, Conn: conn, Options: rmqConfig}
	})
	return singleton
}
func init() {
	value := "test"
	if len(value) == 0 {
		err1 := godotenv.Load()
		if err1 != nil {
			log.Panic(err1)
		}
	}
}
func (RMQ *RMQ) Rpc(topic string, msg string) interface{} {
	options := RMQ.Options
	ch, err := RMQ.Conn.Channel()
	//defer ch.Close()
	failOnError(err, "Failed to open a channel")
	q, err := ch.QueueDeclare(
		"",    // name
		false, // durable
		false, // delete when unused
		true,  // exclusive
		false, // noWait
		nil,   // arguments
	)
	failOnError(err, "Failed to declare a queue")
	options.CONSUME_CALL_NOACK = true
	fmt.Println("autoack - ", options.CONSUME_CALL_NOACK)
	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		options.CONSUME_CALL_NOACK, // auto-ack
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil,   // args
	)
	failOnError(err, "Failed to register a consumer")
	corrId := randomString(32)
	err = ch.Publish(
		"",    // exchange
		topic, // routing key
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			ContentType:   "text/plain",
			CorrelationId: corrId,
			ReplyTo:       q.Name,
			Body:          []byte(msg),
		})
	failOnError(err, "Failed to publish a message")
	var res interface{}
	for d := range msgs {
		if corrId == d.CorrelationId {
			var req interface{}
			res = json.Unmarshal([]byte(d.Body), &req)
			failOnError(err, "Failed to convert body to json")
			fmt.Println("reading done")
			ch.Cancel(d.ConsumerTag, true)
			break
		}
	}
	return res
}
func (RMQ *RMQ) Publish(topic string, msg string) {
	temp := strings.Split(topic, ".")
	exchange, rKey := temp[0], temp[1]
	ch, err := RMQ.Conn.Channel()
	defer ch.Close()
	failOnError(err, "Failed to open a channel")
	err = ch.Publish(
		exchange, // exchange
		rKey,     // routing key
		false,    // mandatory
		false,    // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(msg),
		})
	failOnError(err, "Failed to publish a message")
	log.Printf(" [x] Sent To %s", topic)
}
func (RMQ *RMQ) InitFunctions(responderRegistry map[string]EventHandler, consumerRegistry map[string]EventHandler, globalConsumerRegsitry map[string]EventHandler) {
	initCh, err := RMQ.Conn.Channel()
	failOnError(err, "Failed to open a channel")
	options := RMQ.Options
	err = initCh.Qos(
		1,     // prefetch count
		0,     // prefetch size
		false, // global
	)
	failOnError(err, "Failed to set QoS")
	for k, responderFunction := range responderRegistry {
		key, resp := k, responderFunction
		temp := []string{options.APP_NAME, ".", key}
		topicName := strings.Join(temp, "")
		q, err := initCh.QueueDeclare(
			topicName,                         // name
			options.RESPONDER_QUEUE_DURABLE,   // durable
			options.RESPONDER_QUEUE_AUTODEL,   // delete when usused
			options.RESPONDER_QUEUE_EXCLUSIVE, // exclusive
			false, // no-wait
			nil,   // arguments
		)
		failOnError(err, "Failed to declare a queue")
		msgs, err := initCh.Consume(
			q.Name, // queue
			"",     // consumer
			false,  // auto-ack
			false,  // exclusive
			false,  // no-local
			false,  // no-wait
			nil,    // args
		)
		notifych := initCh.NotifyClose(make(chan *amqp.Error))
		go func() {
			for { //receive loop
				ok := <-notifych
				fmt.Println("printing ok", ok)
				if ok != nil {
					//Qclose <- true
					fmt.Println("connection queue close ", ok)
				}
			}
		}()

		failOnError(err, "Failed to register a consumer")
		go func() {
			for msgItem := range msgs {
				var req interface{}
				json.Unmarshal([]byte(msgItem.Body), &req)
				failOnError(err, "Failed to convert body to integer")
				//Call responder function
				cbFunc := make(chan interface{})
				go resp(req, cbFunc)
				response := <-cbFunc
				strResponse, err := json.Marshal(response)
				msgToSend := amqp.Publishing{
					DeliveryMode:  amqp.Persistent,
					ContentType:   "text/plain",
					CorrelationId: msgItem.CorrelationId,
					Body:          []byte(strResponse),
				}
				//returns := initCh.NotifyReturn(make(chan amqp.Return, 1))
				//fmt.Println(msgItem.ReplyTo)
				err = initCh.Publish("", msgItem.ReplyTo, true, false, msgToSend)
				/*go func() {
					for { //receive loop
						ok := <-returns
						//fmt.Println(ok)
						if ok.ReplyCode >= 0 {
							fmt.Println(msgItem)
							msgItem.Ack(true)
							inntt, errr := initCh.QueueDelete(msgItem.ReplyTo, false, false, true)
							fmt.Println("delete", inntt)
							fmt.Println("queue", errr)
							//panic("queue closed")
							//fmt.Println("connection queue close :  due to queue close")
							//Qclose <- true
						}
					}
				}()*/

				if err != nil {
					//log.Fatalf("%s: %s", msg, err)
					fmt.Println("got error in line number 212", err)
					//Qclose <- true
					//panic(fmt.Sprintf("%s: %s", msg, err))
				}
				errrr := msgItem.Ack(false)
				fmt.Println(errrr)

				//failOnError(err, "Failed to publish a message")

			}
		}()
		fmt.Println(" [x] Responder Registerd for event :", topicName)
	}
	registerConumerFunctions(options, RMQ.Conn, consumerRegistry, false)
	registerConumerFunctions(options, RMQ.Conn, globalConsumerRegsitry, true)
}
func registerConumerFunctions(options DefaultOptions, rmqcon *amqp.Connection, funcRegistry map[string]EventHandler, isGlobal bool) {
	channelReg, err := rmqcon.Channel()
	failOnError(err, "Failed to open a channel")
	for e, consumerinstance := range funcRegistry {
		routingKey := e
		consumerFunction := consumerinstance
		temp := []string{options.APP_NAME, ".", routingKey}
		QueueName := strings.Join(temp, "")
		exchangeName := options.APP_NAME
		if isGlobal == true {
			exchangeName = options.GLOBAL_EXCHANGE_NAME
		}
		err = channelReg.ExchangeDeclare(
			exchangeName, // name
			"direct",     // type
			options.CONSUMER_EXCHANGE_DURABLE, // durable
			options.CONSUMER_EXCHANGE_AUTODEL, // auto-deleted
			false, // internal
			false, // no-wait
			nil,   // arguments
		)
		failOnError(err, "Failed to declare an exchange")
		q, err := channelReg.QueueDeclare(
			QueueName,                        // name
			options.CONSUMER_QUEUE_DURABLE,   // durable
			options.CONSUMER_QUEUE_AUTODEL,   // delete when usused
			options.CONSUMER_QUEUE_EXCLUSIVE, // exclusive
			false, // no-wait
			nil,   // arguments
		)
		failOnError(err, "Failed to declare a queue")
		err = channelReg.QueueBind(
			q.Name,       // queue name
			routingKey,   // routing key
			exchangeName, // exchange
			false,
			nil)
		failOnError(err, "Failed to bind a queue")
		msgs, err := channelReg.Consume(
			q.Name, // queue
			"",     // consumer
			true,   // auto-ack
			false,  // exclusive
			false,  // no-local
			false,  // no-wait
			nil,    // args
		)
		failOnError(err, "Failed to register a consumer")
		/*notifych := channelReg.NotifyCancel(make(chan string)) //error channels
		go func() {
			for { //receive loop
				ok := <-notifych
				fmt.Println(ok)
				if ok != "" {
					Qclose <- true
					fmt.Println("connection queue close ")
				}
			}
		}()*/
		go func() {
			for msgItem := range msgs {
				var req interface{}
				json.Unmarshal([]byte(msgItem.Body), &req)
				failOnError(err, "Failed to convert body")
				cbFunc := make(chan interface{})
				go consumerFunction(req, cbFunc)
				<-cbFunc
			}
		}()
		fmt.Println(" [x] Consumer registered for event :", QueueName)
	}
}
func (RMQ *RMQ) HandleQClose(done chan string) {
	go func() {
		for {
			if true == <-Qclose {
				done <- "true"
			}
		}
	}()
}
func failOnError(err error, msg string) {
	if err != nil {
		//log.Fatalf("%s: %s", msg, err)
		fmt.Println(msg, err)
		Qclose <- true
		//panic(fmt.Sprintf("%s: %s", msg, err))
	}
}
func randomString(l int) string {
	bytes := make([]byte, l)
	for i := 0; i < l; i++ {
		bytes[i] = byte(randInt(65, 90))
	}
	return string(bytes)
}
func randInt(min int, max int) int {
	return min + rand.Intn(max-min)
}
