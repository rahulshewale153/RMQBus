package RMQBus

type DefaultOptions struct {
	APP_NAME                     string
	URL                          string
	GLOBAL_EXCHANGE_NAME         string
	RPC_TIMEOUT                  int
	RESPONDER_QUEUE_DURABLE      bool
	RESPONDER_QUEUE_EXCLUSIVE    bool
	RESPONDER_QUEUE_AUTODEL      bool
	RESPONDER_CB_QUEUE_DURABLE   bool
	RESPONDER_CB_QUEUE_EXCLUSIVE bool
	RESPONDER_CB_QUEUE_AUTODEL   bool
	CONSUMER_QUEUE_DURABLE       bool
	CONSUMER_QUEUE_EXCLUSIVE     bool
	CONSUMER_QUEUE_AUTODEL       bool
	CONSUMER_EXCHANGE_AUTODEL    bool
	CONSUMER_EXCHANGE_DURABLE    bool
	CONSUME_CALL_NOACK           bool
}

func New(appName string, url string) DefaultOptions {
	e := DefaultOptions{appName, url, "ayopop", 60000, true, false, false, false, true, true, true, false, false, false, true, false}
	return e
}
