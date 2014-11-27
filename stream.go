package eventsource

import (
	"errors"
	"log"
	"net/http"
	"time"
)

func clientRequestHeaders(request *http.Request, lastEventId string) {
	request.Header.Set("Cache-Control", "no-cache")
	request.Header.Set("Accept", "text/event-stream")
	if len(lastEventId) > 0 {
		request.Header.Set("Last-Event-ID", lastEventId)
	}
}

func clientCheckRedirect(request *http.Request, via []*http.Request) error {
	if len(via) >= 10 {
		return errors.New("stopped after 10 redirects")
	}
	clientRequestHeaders(request, via[len(via)-1].Header.Get("Last-Event-Id")) // Go normalized key.
	return nil
}

func clientCheckReconnect(stream *Stream, err error) error {
	// Default emit error but reconnect.
	if err != nil {
		stream.Errors <- err
	}
	return nil
}

type Client struct {
	*http.Client
	// Check if the stream should reconnect or not. You may want to handle stream.Response.StatusCode == 401.
	CheckReconnect func(*Stream, error) error
	// Default user defined reconnect timeout. Streams may also issue a retry.
	Retry time.Duration
}

func NewClient() *Client {
	return &Client{
		Client:         &http.Client{CheckRedirect: clientCheckRedirect},
		CheckReconnect: clientCheckReconnect,
		Retry:          (time.Second * 3),
	}
}

// DefaultClient is the default Client used by Subscribe.
var DefaultClient = NewClient()

type StreamState uint8

const (
	_ StreamState = iota
	StreamConnecting
	StreamOpen
	StreamClosed
)

type Stream struct {
	Client      *Client
	Request     *http.Request
	Response    *http.Response
	lastEventId string
	Events      chan Event
	Errors      chan error
	state       StreamState
	retry       time.Duration
}

func (client Client) Subscribe(url, lastEventId string) (*Stream, error) {
	request, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	clientRequestHeaders(request, lastEventId)

	stream := &Stream{
		Client:  &client,
		Request: request,
		retry:   client.Retry,
		Events:  make(chan Event),
		Errors:  make(chan error),
	}
	go stream.stream()
	return stream, nil
}

func Subscribe(url, lastEventId string) (*Stream, error) {
	return DefaultClient.Subscribe(url, lastEventId)
}

func (stream *Stream) Close() {
	stream.state = StreamClosed
	if stream.Response != nil {
		stream.Response.Body.Close()
	}
}

// TODO: Can be golfed quite a bit still.
func (stream *Stream) stream() {
	defer stream.Close()
	var err error

connect:
	if stream.state == StreamClosed {
		return
	}
	stream.state = StreamConnecting
	for backoff := stream.retry; ; backoff *= 2 {
		stream.Response, err = stream.Client.Do(stream.Request)
		if err != nil || stream.Response.StatusCode != 200 {
			if err = stream.Client.CheckReconnect(stream, err); err != nil {
				return
			}
			// Log backoff.
			log.Printf("Reconnecting to %s in %s", stream.Request.URL, backoff)
			time.Sleep(backoff)
		} else {
			goto open
		}
	}
open:
	stream.state = StreamOpen
	dec := newDecoder(stream.Response.Body)
	for {
		ev, err := dec.Decode()
		if stream.state == StreamClosed {
			return
		}
		if err != nil {
			if err = stream.Client.CheckReconnect(stream, err); err != nil {
				return
			}
			// Log backoff.
			log.Printf("Reconnecting to %s in %s", stream.Request.URL, stream.retry)
			time.Sleep(stream.retry)
			goto connect
		}
		pub := ev.(*publication)
		if pub.Retry() > 0 {
			stream.retry = time.Duration(pub.Retry()) * time.Millisecond
		}
		if len(pub.Id()) > 0 {
			stream.lastEventId = pub.Id()
		}
		stream.Events <- ev
	}
}
