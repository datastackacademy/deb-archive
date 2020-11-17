import moment from 'moment';
import { colorArray } from './checker'

// Create an array of all dates from the start date to the end date inclusive
const datesArray = (start, end) => {
  let dateArr = [];
  for(let currentDate = moment(start); currentDate.format('YYYY-MM-DD') <= end;) {
    dateArr.push(currentDate.format('MM/DD/YYYY'));
    currentDate = currentDate.add(1, 'd');
  }
  return dateArr;
}

// Check if an airline doesn't have a count for a date in the date array and if not, add that date and set it to zero
const setZeroIfNoFlight = (counts, dates) => {
  dates.forEach(e => {
    if(!counts[e]) {
      counts[e] = 0;
    }
  })
  return Object.values(counts);
}



const graphData = (groupedFlights, airlines, graphDates) => {
  const colors = colorArray(groupedFlights, "b");
  return groupedFlights.map((airlineFlights, i) => {
    let returnObj = {};
    returnObj.label = airlines[airlineFlights.airline];
    returnObj.data = setZeroIfNoFlight(airlineFlights.flights_per_day_count, graphDates);
    returnObj.fill = false;
    returnObj.borderColor = [colors[i]];
    returnObj.pointBackgroundColor = colors[i];
    return returnObj;
  });
}

export {datesArray, graphData};