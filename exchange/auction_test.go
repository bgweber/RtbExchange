package main

import (
	"github.com/stretchr/testify/assert"
	"net/http"
	"os"
	"testing"
	"time"
)

// A mock implementation of http.ResponseWriter that just records the response as a string
type MockResponseWriter struct {
	response string
}

func (m *MockResponseWriter) Header() http.Header {
	return http.Header{}
}

func (m *MockResponseWriter) Write(data []byte) (int, error) {
	m.response = string(data)
	return 0, nil
}

func (m *MockResponseWriter) WriteHeader(statusCode int) {}

// Test and auction with no bids
func TestAuctionNoBids(t *testing.T) {
	request, err := os.ReadFile("../requests/Auction1.json")
	assert.Equal(t, err, nil)

	writer := &MockResponseWriter{}
	HandleAuction(request, writer)
	assert.Equal(t, writer.response, "{\"auction_id\":\"abcdef\",\"ask_price_cpm\":4.95,\"offers\":[],\"win_price_cpm\":0}")
}

// Test bidding for an auction that is not active
func TestBidNoAuction(t *testing.T) {
	request, err := os.ReadFile("../requests/Bid3a.json")
	assert.Equal(t, err, nil)

	writer := &MockResponseWriter{}
	HandleBid(request, writer)
	assert.Equal(t, writer.response, "Auction is not active")
}

// Test an auction with 2 bids placed
func TestAuctionAndBids(t *testing.T) {
	request, err := os.ReadFile("../requests/Auction2.json")
	assert.Equal(t, err, nil)

	// bid after 2 and 3 seconds
	go func() {
		time.Sleep(time.Duration(2) * time.Second)
		request, err := os.ReadFile("../requests/Bid2a.json")
		assert.Equal(t, err, nil)

		writer := &MockResponseWriter{}
		HandleBid(request, writer)
		assert.Equal(t, writer.response, "Bid placed")

		time.Sleep(time.Duration(1) * time.Second)
		request, err = os.ReadFile("../requests/Bid2b.json")
		assert.Equal(t, err, nil)

		writer = &MockResponseWriter{}
		HandleBid(request, writer)
		assert.Equal(t, writer.response, "Bid placed")
	}()

	writer := &MockResponseWriter{}
	HandleAuction(request, writer)
	assert.Equal(t, writer.response, "{\"auction_id\":\"123456\",\"ask_price_cpm\":3.95,\"offers\":[10,15],\"win_price_cpm\":15}")
}
