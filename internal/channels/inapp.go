package channels

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/notification-service/naas/internal/config"
	"github.com/notification-service/naas/pkg/models"
	"github.com/gorilla/websocket"
	"go.uber.org/zap"
)

type InAppProvider struct {
	config   config.InAppConfig
	logger   *zap.Logger
	clients  map[string]*Client
	mu       sync.RWMutex
	upgrader websocket.Upgrader
}

type Client struct {
	UserID    string
	TenantID  string
	Conn      *websocket.Conn
	SendChan  chan *Message
	CloseChan chan struct{}
}

func NewInAppProvider(cfg config.InAppConfig, logger *zap.Logger) *InAppProvider {
	return &InAppProvider{
		config: cfg,
		logger: logger,
		clients: make(map[string]*Client),
		upgrader: websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool { return true },
		},
	}
}

func (p *InAppProvider) Name() string {
	return "in_app"
}

func (p *InAppProvider) Channel() models.Channel {
	return models.ChannelInApp
}

func (p *InAppProvider) Send(ctx context.Context, msg *Message) (*SendResult, error) {
	p.mu.RLock()
	client, ok := p.clients[msg.UserID]
	p.mu.RUnlock()

	if !ok {
		return &SendResult{
			ProviderID: "in_app",
			SentAt:     time.Now(),
			Metadata: map[string]interface{}{
				"provider": "in_app",
				"channel":  "in_app",
				"status":   "user_offline",
			},
		}, nil
	}

	select {
	case client.SendChan <- msg:
		return &SendResult{
			ProviderID:   "in_app",
			ProviderMsgID: fmt.Sprintf("inapp-%d", time.Now().UnixNano()),
			SentAt:       time.Now(),
			Metadata: map[string]interface{}{
				"provider": "in_app",
				"channel":  "in_app",
				"status":   "delivered",
			},
		}, nil
	case <-time.After(5 * time.Second):
		return nil, fmt.Errorf("timeout sending in-app message")
	}
}

func (p *InAppProvider) RegisterClient(userID, tenantID string, conn *websocket.Conn) {
	client := &Client{
		UserID:    userID,
		TenantID:  tenantID,
		Conn:      conn,
		SendChan:  make(chan *Message, 100),
		CloseChan: make(chan struct{}),
	}

	p.mu.Lock()
	p.clients[userID] = client
	p.mu.Unlock()

	go p.writePump(client)
	go p.readPump(client)
}

func (p *InAppProvider) writePump(client *Client) {
	for {
		select {
		case msg := <-client.SendChan:
			client.Conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := client.Conn.WriteJSON(msg); err != nil {
				p.logger.Error("Failed to write message", zap.Error(err))
				return
			}
		case <-client.CloseChan:
			return
		}
	}
}

func (p *InAppProvider) readPump(client *Client) {
	defer func() {
		p.mu.Lock()
		delete(p.clients, client.UserID)
		p.mu.Unlock()
		client.Conn.Close()
	}()

	client.Conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	client.Conn.SetPongHandler(func(string) error {
		client.Conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		return nil
	})

	for {
		_, _, err := client.Conn.ReadMessage()
		if err != nil {
			break
		}
	}
}

func (p *InAppProvider) HealthCheck(ctx context.Context) error {
	return nil
}

func (p *InAppProvider) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, client := range p.clients {
		close(client.CloseChan)
	}
	return nil
}

func (p *InAppProvider) GetConnectedUsers() []string {
	p.mu.RLock()
	defer p.mu.RUnlock()

	users := make([]string, 0, len(p.clients))
	for userID := range p.clients {
		users = append(users, userID)
	}
	return users
}