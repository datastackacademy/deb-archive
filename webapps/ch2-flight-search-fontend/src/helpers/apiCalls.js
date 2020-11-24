export const fetchPreload = async (baseURL) => {
    let path = `${baseURL}query/preload`
    let result = await fetch(path, {method: 'GET',headers:{"Accept":"application/json", "Connection":"keep-alive"}})
    return result
}

export const formatAirportOptions = (airport) => {
    return {value: airport.iata, label:`${airport.iata} (${airport.city},${airport.state})`}
}

export const fetchAirportInfo = async (baseURL, src, dest) => {
    let path = `${baseURL}query/airports?src=${src}&dest=${dest}`
    let result = await fetch(path)
    return result
}

export const fetchAirlines = async(baseURL, airlineArray) => {
    let airlineString = "";
    let result = "";
    if(airlineArray.length >0 ){
        const airlineString = airlineArray.reduce((accumulator, currentValue) => {return accumulator+","+currentValue})
    let path = `${baseURL}query/airlines?iata=${airlineString}`
    result = await fetch(path);
    } else{
        result = "empty airline array"
    }
    
    return result;
}

export const airlineDictionary = (airlineArray) => {
    let output = {};
    airlineArray.forEach(airline => output[airline.iata] = airline.name);
    return output;
}

export const fetchFlights = async (baseURL, src, dest, start, end) => {
    let [sy,sm,sd] = start.split("-")
    let [ey,em,ed] = end.split("-")
    let path = `${baseURL}query/flights?src=${src}&dest=${dest}&start=${sy}-${sm}-${sd}&end=${ey}-${em}-${ed}`
    console.log(path)
    let result = await fetch(path)
    return result
}

export const filterQueryDate = (date) => {
    date = date.substring(5,15).split(",");
    return `${date[0]}-${date[1]}-${date[2]}`;
}

export const fetchAirplaneInfo = async(baseURL, tailnum) => {
    let path = `${baseURL}query/aircraft?tailnum=${tailnum}`
    let result = await fetch(path);
    return result;
}

//need to add functions for calculating airline rankings, calculating airline stats, and displaying all flights by airline(filter)