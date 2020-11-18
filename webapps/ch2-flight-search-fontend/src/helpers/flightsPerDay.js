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

// Check if an airline doesn't have a count for a date in the date array and if not add a zero for that day
const addZeroIfNoFlight = (counts, dates) => {
  let returnCounts = [];
  dates.forEach(date => {
    let formattedDate = moment(date).format("YYYY-MM-DD")
    !counts[formattedDate]? returnCounts.push(0): returnCounts.push(counts[formattedDate]);
  })
  console.log(counts);
  return returnCounts;
}



const graphData = (groupedFlights, airlines, graphDates) => {
  const colors = colorArray(groupedFlights, "b");
  return groupedFlights.map((airlineFlights, i) => {
    let returnObj = {};
    returnObj.label = airlines[airlineFlights.airline];
    returnObj.data = addZeroIfNoFlight(airlineFlights.flights_per_day_count, graphDates);
    returnObj.fill = false;
    returnObj.borderColor = [colors[i]];
    returnObj.pointBackgroundColor = colors[i];
    return returnObj;
  });
}

export {datesArray, graphData};