package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/notification-service/naas/internal/api"
	"github.com/notification-service/naas/internal/analytics"
	"github.com/notification-service/naas/internal/channels"
	"github.com/notification-service/naas/internal/config"
	"github.com/notification-service/naas/internal/orchestration"
	"github.com/notification-service/naas/internal/preferences"
	"github.com/notification-service/naas/internal/processing"
	"github.com/notification-service/naas/internal/reliability"
	"github.com/notification-service/naas/internal/routing"
	"github.com/notification-service/naas/internal/templates"
	"github.com/notification-service/naas/pkg/errors"
	"github.com/notification-service/naas/pkg/models"
	"go.uber.org/zap"
)

func main() {
	logger, _ := zap.NewProduction()
	defer logger.Sync()

	cfg, err := config.Load()
	if err != nil {
		logger.Fatal("Failed to load config", zap.Error(err))
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		logger.Info("Shutdown signal received")
		cancel()
	}()

	dbConfig := models.DatabaseConfig{
		Host:            cfg.Database.Host,
		Port:            cfg.Database.Port,
		User:            cfg.Database.User,
		Password:        cfg.Database.Password,
		Database:        cfg.Database.Database,
		MaxOpenConns:    cfg.Database.MaxOpenConns,
		MaxIdleConns:    cfg.Database.MaxIdleConns,
		ConnMaxLifetime: cfg.Database.ConnMaxLifetime,
	}

	store, err := models.NewStore(ctx, dbConfig)
	if err != nil {
		logger.Fatal("Failed to initialize store", zap.Error(err))
	}
	defer store.Close()

	templateStore := templates.NewPostgresStore(store.DB)

	redisConfig := models.RedisConfig{
		Addresses:     cfg.Redis.Addresses,
		Password:      cfg.Redis.Password,
		DB:            cfg.Redis.DB,
		PoolSize:      cfg.Redis.PoolSize,
		MinIdleConns:  cfg.Redis.MinIdleConns,
	}
	redisClient := models.NewRedisClient(redisConfig)
	defer redisClient.Close()

	templateEngine := templates.NewEngine(templateStore, redisClient, logger)
	channelRegistry := channels.NewRegistry(cfg, logger)
	preferenceManager := preferences.NewManager(store.DB, redisClient, logger)
	router := routing.NewRouter(channelRegistry, preferenceManager, logger)
	orchestrator := orchestration.NewOrchestrator(router, templateEngine, preferenceManager, logger)
	processor := processing.NewProcessor(store.DB, redisClient, logger)
	reliabilityManager := reliability.NewManager(store.DB, redisClient, cfg, logger)
	analyticsEngine := analytics.NewAnalytics(redisClient, &analytics.Config{
		Enabled:       cfg.Analytics.Enabled,
		EventTopic:    cfg.Analytics.EventTopic,
		RetentionDays: cfg.Analytics.RetentionDays,
		AuditEnabled:  cfg.Analytics.AuditEnabled,
		KafkaBrokers:  cfg.Analytics.KafkaBrokers,
	}, logger)

	orchestrator.SetChannelRegistry(channelRegistry)
	orchestrator.SetReliabilityManager(reliabilityManager)

	server := api.NewServer(&api.Config{
		Port:         cfg.Server.Port,
		Host:         cfg.Server.Host,
		ReadTimeout:  cfg.Server.ReadTimeout,
		WriteTimeout: cfg.Server.WriteTimeout,
		IdleTimeout:  cfg.Server.IdleTimeout,
	}, orchestrator, processor, reliabilityManager, preferenceManager, templateEngine, logger)

	if err := reliabilityManager.Start(ctx, orchestrator); err != nil {
		logger.Fatal("Failed to start reliability manager", zap.Error(err))
	}

	analyticsEngine.StartFlushWorker(ctx)

	go func() {
		if err := server.Start(); err != nil {
			logger.Error("Server error", zap.Error(err))
			cancel()
		}
	}()

	logger.Info("Notification Service started", zap.String("port", cfg.Server.Port))

	<-ctx.Done()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	if err := server.Stop(shutdownCtx); err != nil {
		logger.Error("Server shutdown error", zap.Error(err))
	}

	reliabilityManager.Shutdown()
	processor.Shutdown()
	analyticsEngine.Close()

	logger.Info("Notification Service stopped gracefully")
}

func init() {
	if err := errors.Register(); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to register errors: %v\n", err)
		os.Exit(1)
	}
}