package main

import (
	"fmt"
	"io"
	"net/http"
	"strconv"
)

// Define the port to run the service on
const SERVER_PORT = 8080

// Entry point for the exchange service
func main() {
	fmt.Println("Launching exchange")

	// use the standard library (net/http) to set up HTTP endpoints for the exchange
	mux := http.NewServeMux()
	mux.Handle("/", &HomeHandler{})
	mux.Handle("/auction/", &AuctionHandler{})
	mux.Handle("/bid/", &BidHandler{})
	mux.Handle("/nurl/", &WinHandler{})

	// start the HTTP service
	fmt.Printf("Starting exchange on port: %d\n", SERVER_PORT)
	http.ListenAndServe(":"+strconv.Itoa(SERVER_PORT), mux)
}

// Define structs for our different endpoint handlers.
// These implement the ServeHTTP function below
type HomeHandler struct{}
type AuctionHandler struct{}
type BidHandler struct{}
type WinHandler struct{}

// Handler for the default endpoint, which provides details on how to use the exchange
func (h *HomeHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("<h1>RTB Exchange</h1>Send auctions requests to /auction and bids to /bid"))
}

// Handler for auction requests. This function checks for a valid request body, and set
// sets the status codes for bad requests
func (h *AuctionHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// check for the auction json data, and set status codes if needed
	requestBody, err := io.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Invalid Request Body"))
		return
	} else if len(requestBody) == 0 {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Missing Request Body"))
		return
	}

	// run the auction
	HandleAuction(requestBody, w)
}

// Handler for bid requests. This function checks for a valid request body, and set
// sets the status codes for bad requests
func (h *BidHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// check for the auction json data, and set status codes if needed
	requestBody, err := io.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Invalid Request Body"))
		return
	} else if len(requestBody) == 0 {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Missing Request Body"))
		return
	}

	// place the bid
	HandleBid(requestBody, w)
}

// Handler for win notifications. This should not be part of the exchange service, but I included
// it here to simply the implementation. This just writes the request body to standard out.
func (h *WinHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	requestBody, err := io.ReadAll(r.Body)
	if err == nil {
		fmt.Println(string(requestBody))
	}
}
