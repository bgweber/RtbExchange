# RtbExchange

Simulates an RTB exchange. Provides the following endpoints.

- /
  - Default endpoint
- /auction/
    - Endpoint for starting an auction
- /bid/
    - Endpoint for making a bid
- /nurl/
    - Endpoint for testing the win URL (nurl) callbacks
    
Auctions can be started using CURL:
```
curl -H 'Content-Type: application/json' http://localhost:8080/auction/ --data-binary @Auction1.json
```

Bids can be made using CURL:
```
curl -H 'Content-Type: application/json' http://localhost:8080/bid/ --data-binary @Bid1b.json
```




