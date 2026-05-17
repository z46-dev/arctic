package arctic

import (
	"encoding/gob"
	"errors"
	"net"
	"net/netip"
	"reflect"
	"sync"
	"time"
)

var (
	ErrProtocolUnsupported       error           = errors.New("arctic: protocol is not supported")
	ErrClientNotConnected        error           = errors.New("arctic: client is not connected")
	ErrServerAlreadyListening    error           = errors.New("arctic: server is already listening")
	ErrMessageTooLarge           error           = errors.New("arctic: message exceeds configured buffer size")
	ErrGobTypeInvalid            error           = errors.New("arctic: gob message type must be a struct")
	ErrGobTypeNotRegistered      error           = errors.New("arctic: gob message type is not registered")
	ErrGobTypeRegistrationFailed error           = errors.New("arctic: gob message type registration failed")
	gobTypes                     gobTypeRegistry = gobTypeRegistry{
		registered: map[reflect.Type]struct{}{},
	}
)

const (
	ProtocolTCP Protocol = "tcp"
	ProtocolUDP Protocol = "udp"
)

type (
	Protocol string

	ClientConfig struct {
		ServerAddress  string
		Protocol       Protocol
		BufferSize     int
		Timeout        time.Duration
		UnsafeZeroCopy bool
	}

	ServerConfig struct {
		BindAddress    string
		Protocol       Protocol
		BufferSize     int
		Timeout        time.Duration
		UnsafeZeroCopy bool
	}

	MessageHandler func([]byte)
	ClientHandler  func(*ServerClient)
	CloseHandler   func()
	ErrorHandler   func(error)
	Middleware     func(*MessageContext, Next) error
	Next           func(*MessageContext) error

	MessageContext struct {
		Client  *Client
		Message []byte
	}

	Client struct {
		conn        net.Conn
		udpConn     *net.UDPConn
		udpAddr     *net.UDPAddr
		udpAddrPort udpClientKey
		config      ClientConfig
		onMessage   MessageHandler
		onClose     CloseHandler
		onError     ErrorHandler
		middleware  []Middleware
		mutex       sync.RWMutex
		writeMutex  sync.Mutex
		writeBuffer []byte
		udpWriter   *sync.Mutex
		closeOnce   sync.Once
		closeHook   func()
		ownsConn    bool
		done        chan struct{}
	}

	ServerClient struct {
		id int
		*Client
	}

	Server struct {
		listener            net.Listener
		udpConn             *net.UDPConn
		udpConns            []*net.UDPConn
		config              ServerConfig
		clientIDAccumulator int
		clients             map[int]*ServerClient
		udpClients          map[udpClientKey]*ServerClient
		onClient            ClientHandler
		onClose             CloseHandler
		onError             ErrorHandler
		mutex               sync.RWMutex
		udpWriter           sync.Mutex
		closeOnce           sync.Once
		done                chan struct{}
	}

	GobMessageHandler[MessageType any] func(MessageType)
	GobClientHandler[MessageType any]  func(*GobServerClient[MessageType])
	GobMiddleware[MessageType any]     func(*GobMessageContext[MessageType], GobNext[MessageType]) error
	GobNext[MessageType any]           func(*GobMessageContext[MessageType]) error

	GobMessageContext[MessageType any] struct {
		Client  *GobClient[MessageType]
		Message MessageType
	}

	GobClient[MessageType any] struct {
		*Client
		encoder    *gob.Encoder
		decoder    *gob.Decoder
		onMessage  GobMessageHandler[MessageType]
		middleware []GobMiddleware[MessageType]
		mutex      sync.RWMutex
	}

	GobServerClient[MessageType any] struct {
		id int
		*GobClient[MessageType]
	}

	GobServer[MessageType any] struct {
		*Server
		gobClients map[int]*GobServerClient[MessageType]
		onClient   GobClientHandler[MessageType]
		mutex      sync.RWMutex
	}

	gobTypeRegistry struct {
		mutex      sync.RWMutex
		registered map[reflect.Type]struct{}
	}

	udpClientKey = netip.AddrPort
)
