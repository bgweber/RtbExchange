package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"
)

// Use the sync.Map object for safe concurrent access to the auction data
// The key is the auction ID and value is a pointer to the auction (map[string]*Auction)
var liveAuctions sync.Map

// Object for storing auction details. We parse a subset of the fields from the JSON and
// update the other fields based on bids received
type Auction struct {
	AuctionId   string  `json:"auction_id"`
	AskPriceCPM float64 `json:"ask_price_cpm"`
	Tmax        int     `json:"tmax"`
	Offers      []float64
	WinPrice    float64
	WinnerNurl  string
	Lock        sync.Mutex
}

// Details about the auction. This is a different object than the Auction struct,
// because we use it to for marshalling to JSON for the auction results.
type AuctionResult struct {
	AuctionId   string    `json:"auction_id"`
	AskPriceCPM float64   `json:"ask_price_cpm"`
	Offers      []float64 `json:"offers"`
	WinPriceCpm float64   `json:"win_price_cpm"`
}

// Object for a bid request. I made this a nested structure to make it similar to the OpenRTB
// specification. But here we are not supporting an array of bid objects, and just using 1.
type Bid struct {
	Seat    string      `json:"seat"`
	Details *BidDetails `json:"bid"`
}

// Nested bid details
type BidDetails struct {
	AuctionId   string  `json:"auction_id"`
	BidPriceCPM float64 `json:"bid_price_cpm"`
	Nurl        string  `json:"nurl"`
}

// Parse the Auction JSON and run an auction if valid
func HandleAuction(jsonRequest []byte, w http.ResponseWriter) {
	// parse the auction json
	auction := &Auction{}
	err := json.Unmarshal(jsonRequest, &auction)
	auction.Offers = make([]float64, 0) // initialize this list, to avoid null results in JSON generation

	// check for a JSON parsing error
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Error parsing request: " + err.Error()))
		return
	}

	// log the auction details to standard out
	fmt.Printf("Auction Received - ID:%s AskPrice:%f Tmax:%d\n",
		auction.AuctionId, auction.AskPriceCPM, auction.Tmax)

	// check for valid auction parameters
	if len(auction.AuctionId) == 0 {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Missing auction ID"))
		return
	} else if auction.AskPriceCPM <= 0.0 {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Invalid Ask Price"))
		return
	} else if auction.Tmax <= 0 {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Invalid timeout value"))
		return
	}

	// request is valid, log that we are going to run an auction
	fmt.Printf("Starting Auction ID:%s\n", auction.AuctionId)
	RunAuction(auction)
	fmt.Printf("Ending Auction ID:%s\n", auction.AuctionId)

	// create the auction results object
	results := &AuctionResult{}
	results.AuctionId = auction.AuctionId
	results.AskPriceCPM = auction.AskPriceCPM
	results.Offers = auction.Offers
	results.WinPriceCpm = auction.WinPrice

	// generate the response JSON
	resultsJson, err := json.Marshal(results)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Error generating auction results"))
		return
	}

	// send the auction results to the winner, if there is a winner
	// do this in a go routine in order to avoid adding latency to the auction
	if len(auction.WinnerNurl) > 0 {
		go func() {
			req, err := http.NewRequest(http.MethodPut, auction.WinnerNurl, bytes.NewBuffer(resultsJson))
			if err == nil {
				resp, err := http.DefaultClient.Do(req)
				if err == nil {
					defer resp.Body.Close()
				}
			}
		}()
	}

	// output the auction results to the auction initiator
	w.Write(resultsJson)
}

// Add the auction to the list of live auctions for the tmax value
func RunAuction(auction *Auction) {
	// add the auction to the list of live auctions
	liveAuctions.Store(auction.AuctionId, auction)

	// wait for the auction to timeout
	time.Sleep(time.Duration(auction.Tmax) * time.Millisecond)

	// remove the auction from the list of live auctions
	liveAuctions.Delete(auction.AuctionId)
}

// Parse the Bid JSON and run an auction if valid
func HandleBid(jsonRequest []byte, w http.ResponseWriter) {
	// parse the bid json
	bid := &Bid{}
	err := json.Unmarshal(jsonRequest, &bid)

	// check for a JSON parsing error
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Error parsing request: " + err.Error()))
		return
	}

	// check for valid bid params
	if bid.Details.BidPriceCPM <= 0.0 {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Invalid bid price"))
		return
	} else if len(bid.Details.AuctionId) == 0 {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Invalid bid price"))
		return
	}

	// log the bid details to standard out
	fmt.Printf("Bid Received - Seat: %s ID:%s Price:%f\n",
		bid.Seat, bid.Details.AuctionId, bid.Details.BidPriceCPM)

	// Apply the bid to the auction
	PlaceBid(bid.Details, w)
}

// Checks for a live auction for the bid and adds the bid to the auction if it is above the ask price
func PlaceBid(details *BidDetails, w http.ResponseWriter) {
	// check if the auction is active
	auctionRef, ok := liveAuctions.Load(details.AuctionId)
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte("Auction is not active"))
		return
	}

	// Cast to an auction event and lock the object while we update pricing details
	auction := auctionRef.(*Auction)
	auction.Lock.Lock()
	defer auction.Lock.Unlock()

	// check if bid is above the floor
	if details.BidPriceCPM < auction.AskPriceCPM {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Bid price is below the ask price"))
		return
	}

	// add this to the list of bids on the auction
	auction.Offers = append(auction.Offers, details.BidPriceCPM)

	// is this the highest bid so far? if so, update the winner details
	if details.BidPriceCPM > auction.WinPrice {
		auction.WinPrice = details.BidPriceCPM
		auction.WinnerNurl = details.Nurl
	}

	// let the DSP know that the bid was included in the auction
	w.Write([]byte("Bid placed"))
}
