import {fetchAirlines} from './apiCalls';

//works
const distinct = (value, index, self) => {
    return self.indexOf(value) === index
}

//works
const findAllAirlines = (flights) => {
    const allAirlines = flights.map(flight => flight.airline);
    const uniqueAirlines = allAirlines.filter(distinct);
    return uniqueAirlines;
}

const groupByAirline = (flights) => {
    const uniqueAirlines = findAllAirlines(flights);
    let output = [];
    for (let i=0; i < uniqueAirlines.length; i++){
        const airline = uniqueAirlines[i]
        const airline_flights = flights.filter((flight) => flight.airline === airline);
        const count = airline_flights.length;
        let initialValD = 0;
        const depart_delay_avg = (airline_flights.reduce((accumulator, flight) => {return (accumulator + flight.departure_delay)}, initialValD) / count);
        let initialValA = 0;
        const arrival_delay_avg = (airline_flights.reduce((accumulator, flight) => {return (accumulator=accumulator + flight.arrival_delay)}, initialValA) / count);
        let dict = {};
        dict["airline"] = airline;
        dict["count"] = count;
        dict["flights"] = airline_flights;
        dict["depart_delay_avg"] = depart_delay_avg;
        dict["arrival_delay_avg"] = arrival_delay_avg;
        output.push(dict);
    }
    return output;
}

const rankCount = (flights) => {
    const groupedAirlines = groupByAirline(flights);
    const output = groupedAirlines.sort((a,b) =>{
        if(a.count > b.count){return -1;}
        if(a.count < b.count){return 1;}
        return 0;
    });
    return output;
}

const filterFlights = (flights, filter) => {
    const ranked = rankCount(flights);
    let output = [];
    if(filter === "depart_delay_avg"){
        return ranked.sort((a,b) => {
            if(a.depart_delay_avg > b.depart_delay_avg) { return 1;}
            if(a.depart_delay_avg < b.depart_delay_avg) {return -1;}
            return 0;
        });
    } else if (filter === "arrival_delay_avg"){
        return ranked.sort((a, b) => {
            if (a.arrival_delay_avg > b.arrival_delay_avg) {
                return 1;
            }
            if (a.arrival_delay_avg < b.arrival_delay_avg) {
                return -1;
            }
            return 0;
        });
    } else if (filter === "count"){
        output = ranked;
    } else{
        console.log("error city");
    }

    return output;
}

export {findAllAirlines,groupByAirline, rankCount, filterFlights};

