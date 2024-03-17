// An Auction has a list of supported auction types
type Auction = {
  type: string[];
};

// Creates an auction from the detals and checks if the allowed auction type is in the list of types in the auction
const simulateBid = (auctionDetails: string, allowedType: string) => {
    const auction = convertToObject(auctionDetails)

    if (auction.type.includes(allowedType)) {
        console.log("Auction allowed");
    } else {
        console.log("Auction not allowed");
    }
};  

// Parses the passed in string into an Auction type.
// Only parses out the type attribute for Auction definitions.
const convertToObject = (type: string) => {

    // attributes to parse out of the typescript type
    var auctionTypes: string[];
    auctionTypes = [];

    // iterate over the attributes in the type
    var attributes = type.split("{")[1].split("}")[0].split(";")
    for (let i=0; i<attributes.length; i++) {
        const attribute = attributes[i].trim();

        // get each attribute name and value
        if (attribute.length > 0) {
            const name = attribute.split(" ")[0].split(":")[0];
            const value = attribute.substring(name.length + 1)

            // for the type attribute, parse the array of auction types
            if (name == 'type') {
                const types = value.replace("|", "").split("\"")    

                for (let j=0; j<types.length; j++) {
                    const auctionType = types[j].trim();
                    
                    if (auctionType.length > 0) {
                        auctionTypes.push(auctionType)
                    }
                }
            }
        }
    }

    // create and return the auction object
    const auction: Auction  = {
        type: auctionTypes
    }

    return auction;
};

// call simulate bid for the example string
var auctionDetails = `type Auction = { type: "open" | "pmp";};`
simulateBid(auctionDetails, "pmp")

// call with additional (ignored) auction attributes
auctionDetails = `type Auction = { bcat: "IAB1" | "IAB2" ; ask_price: 0.0; type: "open" | "pmp";};`
simulateBid(auctionDetails, "pmp")

// test auction not allowed
auctionDetails = `type Auction = { type: "open" | "pmp";};`
simulateBid(auctionDetails, "private")
