package web

import (
	"context"
	"net/http"

	"jinx/internal/relay/model"
)

type GroupProvider interface {
	Groups(context.Context) []model.GroupInfo
}

type PageProvider interface {
	PageHandler() http.Handler
}

type RouteRegistrar func(string, http.HandlerFunc) error

type Extension interface {
	GroupProvider
	RegisterRoutes(RouteRegistrar) error
	Start(func(*model.Message)) error
	Stop()
}

type LegacyConfigHandler interface {
	ConfigHandler() http.HandlerFunc
}
