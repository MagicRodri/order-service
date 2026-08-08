package api

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/MagicRodri/order-service/internal/app"
	"github.com/MagicRodri/order-service/internal/domain"
)

type API struct {
	app *app.App
	log *slog.Logger
}

func New(a *app.App, log *slog.Logger) *API { return &API{app: a, log: log} }

func (a *API) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", a.health)
	mux.HandleFunc("POST /orders", a.createOrder)
	mux.HandleFunc("GET /orders", a.listOrders)
	mux.HandleFunc("GET /orders/{id}", a.getOrder)
	mux.HandleFunc("POST /orders/{id}/cancel", a.cancelOrder)
	// Exposes the replicated view so the effect of customer events is visible
	// without opening a psql session.
	mux.HandleFunc("GET /customer-view/{id}", a.getCustomerView)
	return a.logRequests(mux)
}

func (a *API) health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (a *API) createOrder(w http.ResponseWriter, r *http.Request) {
	var req struct {
		CustomerID string        `json:"customer_id"`
		Items      []domain.Item `json:"items"`
		Currency   string        `json:"currency"`
	}
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	order, err := a.app.CreateOrder(r.Context(), req.CustomerID, req.Items, req.Currency)
	if err != nil {
		a.fail(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, order)
}

func (a *API) listOrders(w http.ResponseWriter, r *http.Request) {
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	orders, err := a.app.ListOrders(r.Context(), r.URL.Query().Get("customer_id"), limit)
	if err != nil {
		a.fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"orders": orders})
}

func (a *API) getOrder(w http.ResponseWriter, r *http.Request) {
	order, err := a.app.GetOrder(r.Context(), r.PathValue("id"))
	if err != nil {
		a.fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, order)
}

func (a *API) cancelOrder(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Reason string `json:"reason"`
	}
	// A body is optional here, so a decode failure is not fatal.
	_ = decodeJSON(r, &req)

	order, err := a.app.CancelOrder(r.Context(), r.PathValue("id"), req.Reason)
	if err != nil {
		a.fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, order)
}

func (a *API) getCustomerView(w http.ResponseWriter, r *http.Request) {
	view, err := a.app.GetCustomerView(r.Context(), r.PathValue("id"))
	if err != nil {
		a.fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, view)
}

func (a *API) fail(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, domain.ErrNotFound):
		writeError(w, http.StatusNotFound, err)
	case errors.Is(err, domain.ErrUnknownCustomer):
		// Not 404: the customer may exist but its CustomerCreated event has not
		// arrived yet, so the caller should retry rather than treat it as gone.
		writeError(w, http.StatusConflict, err)
	case errors.Is(err, domain.ErrInvalidInput):
		writeError(w, http.StatusBadRequest, err)
	case errors.Is(err, domain.ErrCustomerBlocked):
		writeError(w, http.StatusForbidden, err)
	case errors.Is(err, domain.ErrAlreadyCancelled):
		writeError(w, http.StatusConflict, err)
	default:
		a.log.Error("request failed", "error", err)
		writeError(w, http.StatusInternalServerError, errors.New("internal error"))
	}
}

func (a *API) logRequests(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		a.log.Debug("request", "method", r.Method, "path", r.URL.Path)
		next.ServeHTTP(w, r)
	})
}

func decodeJSON(r *http.Request, dst any) error {
	dec := json.NewDecoder(http.MaxBytesReader(nil, r.Body, 1<<20))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return errors.New("invalid JSON body")
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func writeError(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, map[string]string{"error": err.Error()})
}
