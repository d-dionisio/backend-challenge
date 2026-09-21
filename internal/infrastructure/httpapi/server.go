package httpapi

import (
	"context"
	"errors"
	"net"
	"net/http"
	"sync"
	"time"

	"go.uber.org/fx"
)

type Server struct {
	address string
}

func (s *Server) Address() string { return s.address }

func NewServer(lifecycle fx.Lifecycle, config Config, api *API, shutdown fx.Shutdowner) *Server {
	s := &Server{}

	var requests sync.WaitGroup

	var mu sync.Mutex

	stopping := false

	handler := api.Handler()

	server := &http.Server{
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			if stopping {
				mu.Unlock()
				writeError(w, http.StatusServiceUnavailable, "SHUTTING_DOWN")
				return
			}
			requests.Add(1)
			mu.Unlock()
			defer requests.Done()
			handler.ServeHTTP(w, r)
		}),
	}

	done := make(chan struct{})

	lifecycle.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {

			listener, err := (&net.ListenConfig{}).Listen(ctx, "tcp", config.Address)
			if err != nil {
				return err
			}

			s.address = listener.Addr().String()

			go func() {
				defer close(done)

				if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
					api.logger.Error("HTTP server stopped unexpectedly")
					_ = shutdown.Shutdown(fx.ExitCode(1))
				}
			}()

			api.logger.Info("HTTP server started", "address", s.address)

			return nil
		},

		OnStop: func(ctx context.Context) error {
			// Impede novas operações antes de esperar pelas que já começaram.
			mu.Lock()
			stopping = true
			mu.Unlock()

			err := server.Shutdown(ctx)
			if err != nil {
				_ = server.Close()
			} // Cancela o contexto das requisições restantes.

			<-done
			// Os handlers respeitam seus prazos; as dependências só fecham depois deles.
			requests.Wait()

			api.logger.Info("HTTP server stopped")

			return err
		},
	})

	return s
}

var Module = fx.Module("http-api", fx.Provide(NewConfig, NewReadiness, NewAPI, NewServer), fx.Invoke(func(*Server) {}))
