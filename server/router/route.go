package router

import "net/http"

type HandlerFunc func(w http.ResponseWriter, r *http.Request) error

type Route struct {
	Method  string
	Path    string
	Handler HandlerFunc
}

func NewRoute(method string, path string, handler HandlerFunc) Route {
	return Route{method, path, handler}
}

func NewGetRoute(path string, handler HandlerFunc) Route {
	return NewRoute("GET", path, handler)
}

func NewPostRoute(path string, handler HandlerFunc) Route {
	return NewRoute("POST", path, handler)
}

func NewPutRoute(path string, handler HandlerFunc) Route {
	return NewRoute("PUT", path, handler)
}
